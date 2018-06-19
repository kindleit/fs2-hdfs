package com.rodolfohansen.fs2.hdfs

import cats.effect.IO
import cats.instances.unit._
import cats.instances.list._
import cats.syntax.foldable._

import fs2.{Stream, StreamApp, Segment, async, text, io}

import org.slf4j.{Logger, LoggerFactory}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec

import java.net.URI

import java.io.OutputStream

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends StreamApp[IO] {

  import Stream._
  import StreamApp.{ExitCode => Exit}

  val utf8Charset = java.nio.charset.Charset.forName("UTF8")

  val start = System.currentTimeMillis

  def logger(key: String): Logger = LoggerFactory.getLogger(key)

  val service = new HDFSService[IO]
  val hdfsCluster = service.setupCluster()

  def open = {
    val uri = URI.create("/tmp/rhansen")
    val config = new Configuration()
    config.set("fs.defaultFS", "hdfs://localhost:54310")
    service.get(uri, config)
  }

  def close(fs: FileSystem) = IO{ fs.close(); hdfsCluster.shutdown(true)}

  def write(fs: FileSystem): Stream[IO, Unit] = {
    val linesPerWrite = 1000
    val files = 100
    val threads = 8

    val source: Stream[IO, String] = {
      val in = IO(new java.util.zip.GZIPInputStream(
                    new java.io.FileInputStream(
                      new java.io.File("/home/rhansen/file.gz"))))

      io.readInputStream[IO](in, 10000, true)
        .through(text.utf8Decode)
        .through(text.lines)
    }

    val destinations = for {
      gzip <- IO(new GzipCodec())
      opened <- async.refOf[IO, Map[Int, IO[OutputStream]]](Map.empty)
      oss = {
        def create(i: Int): IO[OutputStream] = opened.modify2 { m =>
          val fo = fs.create(new Path(s"test/output.$i.gz"))
          val os = gzip.createOutputStream(fo)
          (m + (i -> IO(os)), os)
        }.map(_._2)
        Stream.range(0, files).evalMap { i =>
          opened.get.flatMap(_.getOrElse(i, create(i)))
        }
      }
      close = opened.get.flatMap(_.values.toList.foldMapM(_.map(_.close())))
    } yield (oss, close)

    def write(s: Segment[String, Unit], os: OutputStream): Stream[IO, Unit] = {
      val lines = s.force.toList.mkString("", "\n", "\n")
      Stream(os.write(lines.getBytes(utf8Charset)))
    }

    Stream.eval(destinations).flatMap {
      case (ds, close)  =>
        source.segmentN(linesPerWrite, true)
          .prefetch.zipWith(ds.repeat)(write).join(threads)
          .onFinalize(close)
    }
  }

  def stream(args: List[String], kill: IO[Unit]): Stream[IO, Exit] =
    Stream.bracket(open)(write, close).attempt.map(exit)

  def exit: Either[Throwable, _] => Exit = {
    case Left(e) => logger("main").error("Unexpected Failure", e); Exit(99)
    case Right(_) => Exit(0)
  }


}
