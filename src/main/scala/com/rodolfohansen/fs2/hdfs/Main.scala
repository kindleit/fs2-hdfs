package com.rodolfohansen.fs2.hdfs

import cats.effect.IO
import cats.syntax.apply._

import fs2.{Stream, StreamApp, Segment, async, text, io}

import org.slf4j.{Logger, LoggerFactory}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec

import java.net.URI

import java.io.OutputStream

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.{Stream => ScalaStream}

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
    val files = 100l
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
      opened <- async.refOf[IO, Long](0l)
      oss = {
        def dest(i: Long) = async.once(
          IO {
            val fo = fs.create(new Path(s"test/output.$i.gz"))
            gzip.createOutputStream(fo)
          } <* opened.setSync(i + 1)
        ).unsafeRunSync()
        val ds = ScalaStream.range(0, files).map(dest)
        Stream.emits(ds).evalMap(identity)
      }
      close = opened.get.flatMap(oss.take(_).observe1(os => IO(os.close)).compile.drain)
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
