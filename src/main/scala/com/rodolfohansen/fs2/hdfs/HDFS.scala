package com.rodolfohansen.fs2.hdfs

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

class HDFSService[F[+_]: Sync] {

  val sync = implicitly[Sync[F]]

  /** Safe construction of an HDFS FileSystem from a Configuration. */
  def fromConfig(conf: Configuration): F[FileSystem] =
    sync.delay(FileSystem.get(conf))

  /** Safe construction of an HDFS FileSystem from a URI and a Configuration. */
  def get(uri: URI, conf: Configuration): F[FileSystem] =
    sync.delay(FileSystem.get(uri, conf))

  /** Return a Sink and close pair. The sink writes to the supplied HDFS Path with optional gzipping and auto-closing. */
  def writePath2(path: Path, gzip: Boolean = true)(fs: FileSystem): (Sink[F, Byte], F[Unit]) = {
    val fo = sync.delay(fs.create(path))
    val gz = sync.delay(new GzipCodec())
    val go = if (gzip)
               (gz, fo).mapN(_.createOutputStream(_))
             else fo
    (io.writeOutputStream[F](go, false), go.map(_.close))
  }

  /** Return a Sink thats writes to the supplied HDFS Path with optional gzipping and auto-closing. */
  def writePath(path: Path, gzip: Boolean = true)(fs: FileSystem): Sink[F, Byte] = writePath2(path, gzip)(fs)._1

  /** Return a Stream of Sinks thats will write to the supplied HDFS Path(s) and a bindable action to close them. */
  def writePaths(path: Int => Path, maxFiles: Int)(fs: FileSystem): F[(Stream[F, Sink[F, Byte]], F[Unit])] =
    async.refOf[F, Map[Int, (F[Sink[F, Byte]], F[Unit])]](Map.empty) map { opened =>

      def create(i: Int): F[Sink[F, Byte]] = opened.modify2 { m =>
        val (os, go) = writePath2(path(i), true)(fs)
        (m + (i -> ((sync.delay(os), go))), os)
      }.map(_._2)

      val close = opened.get.flatMap(_.values.toList.foldMapM(_._2))

      val dests: Stream[F, Sink[F, Byte]] = Stream.range(0, maxFiles).evalMap { i =>
        opened.get.flatMap(_.get(i).fold(create(i))(_._1))
      }

      (dests, close)
    }

}
