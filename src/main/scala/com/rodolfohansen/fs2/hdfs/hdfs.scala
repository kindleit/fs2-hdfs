package com.rodolfohansen.fs2

import cats.effect.Sync
import cats.instances.unit._
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.flatMap._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec

import fs2.{Sink, Stream, async, io}

import java.net.URI
import java.io.OutputStream

package object hdfs {

  /** Safe construction of an HDFS FileSystem from a Configuration. */
  def fromConfig[F[_]](conf: Configuration)(implicit F: Sync[F]): F[FileSystem] =
    F.delay(FileSystem.get(conf))

  /** Safe construction of an HDFS FileSystem from a URI and a Configuration. */
  def get[F[_]](uri: URI, conf: Configuration)(implicit F: Sync[F]): F[FileSystem] =
    F.delay(FileSystem.get(uri, conf))

  /** Return a Sink that writes to the supplied HDFS Path auto-closing it with optional gzipping. */
  def writePath[F[_]](path: Path, gzip: Boolean = true)(fs: FileSystem)(implicit F: Sync[F]): Sink[F, Byte] = {
    val os = F.delay(fs.create(path):OutputStream)
    io.writeOutputStream(if (gzip)
                            (F.delay(new GzipCodec()), os).mapN(_.createOutputStream(_))
                         else os)
  }

  /** Return a Sink and close pair. The sink writes to the supplied HDFS Path with optional gzipping. */
  def writePath2[F[_]](path: Path, gzip: Boolean = true)(fs: FileSystem)(implicit F: Sync[F]): (Sink[F, Byte], F[Unit]) = {
    val pos = F.delay(fs.create(path):OutputStream)
    val os = if (gzip)
               (F.delay(new GzipCodec()), pos).mapN(_.createOutputStream(_):OutputStream)
             else pos
    (io.writeOutputStream[F](os, false), os.map(_.close))
  }

  /** Return a Stream of Sinks thats will write to the supplied HDFS Path(s) and a bindable action to close them. */
  def writePaths[F[_]](path: Int => Path, maxFiles: Int)(fs: FileSystem)(implicit F: Sync[F]): F[(Stream[F, Sink[F, Byte]], F[Unit])] =
    async.refOf[F, Map[Int, (F[Sink[F, Byte]], F[Unit])]](Map.empty) map { opened =>

      def create(i: Int): F[Sink[F, Byte]] = opened.modify2 { m =>
        val (sink, close) = writePath2(path(i), true)(fs)
        (m + (i -> ((F.delay(sink), close))), sink)
      }.map(_._2)

      val closeAll = opened.get.flatMap(_.values.toList.foldMapM(_._2))

      val dests: Stream[F, Sink[F, Byte]] = Stream.range(0, maxFiles).evalMap { i =>
        opened.get.flatMap(_.get(i).fold(create(i))(_._1))
      }

      (dests, closeAll)
    }

}
