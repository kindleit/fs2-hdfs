package kindleit.fs2.hdfs

import cats.effect.IO
import cats.instances.unit._

import fs2.{Chunk, Stream, StreamApp, Segment, text, io}

import org.slf4j.{Logger, LoggerFactory}

import org.specs2._
import org.specs2.specification.BeforeAfterAll
import org.specs2.concurrent.ExecutionEnv

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}

import java.net.URI
import java.io.File

import scala.concurrent.Future
import scala.concurrent.duration._

class HDFSWriterSpec(implicit ee: ExecutionEnv) extends Specification with BeforeAfterAll {

  import Stream._
  import StreamApp.{ExitCode => Exit}

  def is = s2"""
  hdfs.writePaths should
    Exited cleanly.        ${matchRunExitCode(wpRun)}
  hdfs.writePathsAsync should
    Exited cleanly.        ${matchRunExitCode(wpaRun)}
  """

  def matchRunExitCode(run: Future[List[Exit]]) =
    run.map(_.head) must be_==(Exit(0)).awaitFor(1.minute)

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

  def logger(key: String): Logger = LoggerFactory.getLogger(key)

  lazy val start = System.currentTimeMillis
  val hdfsCluster = setupCluster()

  override def beforeAll = {
    logger("main").info(s"before All $start")
    hdfsCluster.waitClusterUp()
  }

  override def afterAll = {
    logger("main").info("after All")
    hdfsCluster.shutdown()
  }


  def openFileSystem = {
    logger("main").info("Opening FS")
    val uri = URI.create("/tmp/rhansen")
    val config = new Configuration()
    config.set("fs.defaultFS", "hdfs://localhost:54310")
    get[IO](uri, config)
  }

  def closeFileSystem(fs: FileSystem) = IO {
    logger("main").info("Closing FS")
    fs.close()
  }

  val linesPerWrite = 1000
  val files = 100
  val threads = 8

  def source: Stream[IO, String] = {
    val in = IO(new java.util.zip.GZIPInputStream(
                  new java.io.FileInputStream(
                    new java.io.File("/home/rhansen/file.gz"))))

    io.readInputStream[IO](in, 10000, true)
      .through(text.utf8Decode)
      .through(text.lines)
  }

  def toBytes(s: Segment[String, _]): Array[Byte] = {
    val lines = s.force.toList.mkString("", "\n", "\n")
    lines.getBytes(utf8Charset)
  }

  def write(fs: FileSystem): Stream[IO, Unit] = {
    def path(i: Int) = create[IO](new Path(s"output.$i"), false)(fs)

    def toByteStream(s: Segment[String, Unit]): Stream[IO, Byte] =
      Stream.chunk(Chunk.array(toBytes(s)))

    //Uses writePaths which fails to complete inversion of controll, but
    //looses its restriction on requiring its input to be an indexed byte array.
    //Clients end up having to control how they operate with the returned
    //stream of sinks + close effect
    Stream.eval(writePaths[IO](path, files)).flatMap({
      case (ds, close)  =>
        source.segmentN(linesPerWrite, true).map(toByteStream)
          .prefetch.zipWith(ds.repeat)(_ to _).join(threads)
          .onFinalize(close)
    }).foldMonoid
  }

  def writeAsync(fs: FileSystem): Stream[IO, Unit] = {
    def path(i: Int) = create[IO](new Path(s"second.$i"), false)(fs)

    //Simpler, well encapsulated synk. you just need the stream value to be
    // (IDX -> Byte[Array])
    source.segmentN(linesPerWrite, true).zipWithIndex
      .map { case (s, i) => ((i % files).toInt, toBytes(s)) }
      .to (writePathsAsync(path, threads))
      .foldMonoid
  }

  lazy val wpRun = Stream.bracket(openFileSystem)(write, closeFileSystem)
    .attempt.map(exit)
    .compile.toList.unsafeToFuture()

  lazy val wpaRun = Stream.bracket(openFileSystem)(writeAsync, closeFileSystem)
    .attempt.map(exit)
    .compile.toList.unsafeToFuture()

  def exit: Either[Throwable, _] => Exit = {
    case Left(e) => logger("main").error("Unexpected Failure", e); Exit(99)
    case Right(_) => Exit(0)
  }


}
