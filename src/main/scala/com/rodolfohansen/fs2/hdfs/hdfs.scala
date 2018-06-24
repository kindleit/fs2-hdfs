package com.rodolfohansen.fs2

import cats.effect.{Effect, Sync}
import cats.instances.unit._
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.flatMap._

import fs2.{Chunk, Sink, Stream, async, io}

import scala.concurrent.ExecutionContext

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec

import java.net.URI
import java.io.{InputStream, OutputStream}

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
  def writePaths[F[_]](writer: Int => F[OutputStream], maxFiles: Int)(implicit F: Sync[F]): F[(Stream[F, Sink[F, Byte]], F[Unit])] =
    async.refOf[F, Map[Int, (F[Sink[F, Byte]], F[Unit])]](Map.empty) map { opened =>

      def create(i: Int): F[Sink[F, Byte]] = opened.modify2 { m =>
        val os    = writer(i)
        val sink  = io.writeOutputStream(os, false)
        val close = os.map(_.close)
        (m + (i -> ((F.delay(sink), close))), sink)
      }.map(_._2)

      val closeAll = opened.get.flatMap(_.values.toList.foldMapM(_._2))

      val dests: Stream[F, Sink[F, Byte]] = Stream.range(0, maxFiles).evalMap { i =>
        opened.get.flatMap(_.get(i).fold(create(i))(_._1))
      }

      (dests, closeAll)
    }

  /** Return a Stream of Sinks thats will write to the supplied HDFS Path(s) and a bindable action to close them. */
  def writePathsAsync[F[_], IDX](writer: IDX => F[OutputStream], concurrentWrites: Int = 8, fileQueue: Int = 1)(implicit F: Effect[F], ec: ExecutionContext): Sink[F, (IDX, Array[Byte])] = {
    type SinkMap = Map[IDX, (F[Sink[F, Array[Byte]]], F[Unit], F[async.mutable.Queue[F, Array[Byte]]])]

    def viaChunks(s: Stream[F, Array[Byte]]): Stream[F, Byte] =
      s.flatMap(bs => Stream.chunk(Chunk.bytes(bs)))

    def getOrCreate(sinks: async.Ref[F, SinkMap], i: IDX): F[(Sink[F, Array[Byte]], async.mutable.Queue[F, Array[Byte]])] =
      sinks.get.flatMap(_.get(i).fold(create(sinks, i))(m => (m._1, m._3).tupled))

    def create(sinks: async.Ref[F, SinkMap], i: IDX): F[(Sink[F, Array[Byte]], async.mutable.Queue[F, Array[Byte]])] =
      sinks.modify2 { m =>
        val os    = writer(i)
        val close = os.map(_.close)
        val sink  = F.delay(io.writeOutputStreamAsync(os, false).compose(viaChunks))
        val queue = async.boundedQueue[F, Array[Byte]](fileQueue)
        (m + (i -> ((sink, close, queue))), (sink, queue).tupled)
      }.flatMap(_._2)

    def closeAll(sinks: async.Ref[F, SinkMap]): F[Unit] =
      sinks.get.flatMap(_.values.toList.foldMapM(_._2))

    source => {
      def useSinks(sinks: async.Ref[F, SinkMap]): Stream[F, Stream[F, Unit]] = {
        source.flatMap { case (i, bytes) =>
          Stream.eval(getOrCreate(sinks, i)).map { case (sink, queue) =>
              Stream.eval(queue.enqueue1(bytes)) ++ queue.dequeue.to(sink)
          }
        }
      }
      Stream.bracket(async.refOf[F, SinkMap](Map.empty))(useSinks(_).join(concurrentWrites), closeAll)
    }
  }

}
