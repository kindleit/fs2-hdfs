package kindleit.fs2

import cats.effect.{Effect, Sync}
import cats.syntax.apply._
import cats.syntax.functor._
import cats.syntax.flatMap._

import fs2.{Chunk, Sink, Stream, async, io}
import fs2.async.Ref

import scala.concurrent.ExecutionContext

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec

import java.net.URI
import java.io.{InputStream, OutputStream}

/** Provides various ways to work with streams that perform HDFS operations.
  * These combinators can be re-used with fs2.io methods for nice and clean HDFS reads and writes. */
package object hdfs {

  /** Safe construction of an HDFS FileSystem from a Configuration. */
  def fromConfig[F[_]](conf: Configuration)(implicit F: Sync[F]): F[FileSystem] =
    F.delay(FileSystem.get(conf))

  /** Safe construction of an HDFS FileSystem from a URI and a Configuration. */
  def get[F[_]](uri: URI, conf: Configuration)(implicit F: Sync[F]): F[FileSystem] =
    F.delay(FileSystem.get(uri, conf))

  /** Open an OutputStream to a created File at the given path. Optionally compress the output stream. */
  def create[F[_]](path: Path, gzip: Boolean = true)(fs: FileSystem)(implicit F: Sync[F]): F[OutputStream] = {
    val os = F.delay(fs.create(path):OutputStream)
    if (gzip)
      (F.delay(new GzipCodec()), os).mapN(_.createOutputStream(_):OutputStream)
    else os
  }

  /** Open an InputStream ot a given Path. Optionally decompress the input stream. */
  def open[F[_]](path: Path, gzip: Boolean = true)(fs: FileSystem)(implicit F: Sync[F]): F[InputStream] = {
    val is = F.delay(fs.open(path):InputStream)
    if (gzip)
      (F.delay(new GzipCodec()), is).mapN(_.createInputStream(_):InputStream)
    else is
  }

  /** Open an Outputstream to an exisiting file for appending at the given path. Optionally compress the output stream. */
  def append[F[_]](path: Path, gzip: Boolean = true)(fs: FileSystem)(implicit F: Sync[F]): F[OutputStream] = {
    val os = F.delay(fs.append(path):OutputStream)
    if (gzip)
      (F.delay(new GzipCodec()), os).mapN(_.createOutputStream(_):OutputStream)
    else os
  }

  /** Attempts to delete a path in a give Filesystem. */
  def delete[F[_]](path: Path)(fs: FileSystem)(implicit F: Sync[F]): F[Boolean] = {
    F.delay(fs.delete(path, true))
  }

  /** Return a Stream of Sinks thats will write to the supplied HDFS Path(s) and a bindable action to close them. */
  def writePaths[F[_]](writer: Int => F[OutputStream], maxFiles: Int)
                (implicit F: Sync[F]): F[(Stream[F, Sink[F, Byte]], F[Unit])] =
    async.refOf[F, F[Unit]](F.pure(())) map { closeAll =>

      def create(i: Int): F[Sink[F, Byte]] = for {
        os <- writer(i)
        sink  = io.writeOutputStream(F.pure(os), false)
        _  <- closeAll.modify(_ *> F.delay(os.close))
      } yield sink

      val dests: Stream[F, Sink[F, Byte]] = Stream.range(0, maxFiles).evalMap(create)

      (dests, closeAll.get.flatten)
    }

  /** Return a single Sink thats will distribute writes via an indexed stream
    * of byte arrays. The generated sink holds per Outputstream queues to
    * garuantee ordered writes per index. The individual Outputstreams are held
    * open for the duration the sink is open and they will be closed together
    * once all data has been written out.
    * You are guaranteed:
    * - writes to each Outputstream are sequential
    * - the chunkiness is preserved; that is to say the incomming  byte arrays are sent verbatim.
    */
  def writePathsAsync[F[_], IDX](writer: IDX => F[OutputStream], concurrentWrites: Int = 8)
                     (implicit F: Effect[F], ec: ExecutionContext): Sink[F, (IDX, Array[Byte])] = {
    type Snk = Sink[F, Byte]
    type Memory = (Map[IDX, Snk], F[Unit])

    def memStep(i: IDX, os: OutputStream, m: Ref[F, Memory]): F[Snk] = {
      val data = io.writeOutputStreamAsync(F.pure(os), false)
      m.modify(mem => (mem._1 + (i -> data), mem._2 *> F.delay(os.close))).map(_ => data)
    }

    def writeAsync(sink: Sink[F, Byte], buf: Array[Byte]): Stream[F, Unit] =
      Stream.chunk(Chunk.array(buf)).covary[F].to(sink)

    def closeAll(m: Ref[F, Memory]): F[Unit] =
      m.get.flatMap(_._2)

    source => {
      def writes(m: Ref[F, Memory]): Stream[F, Stream[F, Unit]] = source.map {
        case (i, bytes) =>
          val create: F[Snk] =
            writer(i).flatMap(memStep(i, _, m))

          val getOrCreate: F[Snk] =
            m.get.flatMap(_._1.get(i).map(F.pure).getOrElse(create))

          Stream.eval(getOrCreate).flatMap(writeAsync(_, bytes))
      }

      val empty: Memory = (Map.empty, F.pure(()))
      Stream.bracket(async.refOf(empty))(writes(_).join(concurrentWrites), closeAll)
    }
  }

}
