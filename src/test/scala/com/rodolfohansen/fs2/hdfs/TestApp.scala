package com.rodolfohansen.fs2

import cats.effect.IO

import fs2.{Chunk, Stream, StreamApp, Segment, text, io}

import org.slf4j.{Logger, LoggerFactory}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}

import java.net.URI
import java.io.File

import scala.concurrent.ExecutionContext.Implicits.global

object TestApp extends StreamApp[IO] {

  import Stream._
  import StreamApp.{ExitCode => Exit}

  def getTestDir: File = {
    val targetDir = new File("target")
    val testWorkingDir =
      new File(targetDir, s"hdfs-${System.currentTimeMillis}")
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs
    testWorkingDir
  }

  def setupCluster(): MiniDFSCluster = {
    val baseDir = new File(getTestDir, "miniHDFS")
    val conf = new HdfsConfiguration
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    val hdfsCluster = builder.nameNodePort(54310).format(true).build()
    hdfsCluster.waitClusterUp()
    hdfsCluster
  }

  val utf8Charset = java.nio.charset.Charset.forName("UTF8")

  val start = System.currentTimeMillis

  def logger(key: String): Logger = LoggerFactory.getLogger(key)

  val hdfsCluster = setupCluster()

  def openFileSystem = {
    val uri = URI.create("/tmp/rhansen")
    val config = new Configuration()
    config.set("fs.defaultFS", "hdfs://localhost:54310")
    hdfs.get[IO](uri, config)
  }

  def shutdown(fs: FileSystem) = IO {
    fs.close()
    hdfsCluster.shutdown()
  }

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

    def toBytes(s: Segment[String, Unit]): Stream[IO, Byte] = {
      val lines = s.force.toList.mkString("", "\n", "\n")
      Stream.chunk(Chunk.array(lines.getBytes(utf8Charset)))
    }

    def path(i: Int) = hdfs.create[IO](new Path(s"output.$i"), false)(fs)


    Stream.eval(hdfs.writePaths[IO](path, files)).flatMap({
      case (ds, close)  =>
        source.segmentN(linesPerWrite, true).map(toBytes)
          .prefetch.zipWith(ds.repeat)(_ to _).join(threads)
          .onFinalize(close)
    })
  }

  def stream(args: List[String], kill: IO[Unit]): Stream[IO, Exit] =
    Stream.bracket(openFileSystem)(write, shutdown).attempt.map(exit)

  def exit: Either[Throwable, _] => Exit = {
    case Left(e) => logger("main").error("Unexpected Failure", e); Exit(99)
    case Right(_) => Exit(0)
  }


}
