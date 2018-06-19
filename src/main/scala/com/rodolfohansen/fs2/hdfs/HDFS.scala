package com.rodolfohansen.fs2.hdfs

import cats.effect.Sync

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}

import java.io.File
import java.net.URI

class HDFSService[F[_]: Sync] {

  val sync = implicitly[Sync[F]]

  def fromConfig(conf: Configuration): F[FileSystem] =
    sync.delay(FileSystem.get(conf))

  def get(uri: URI, conf: Configuration): F[FileSystem] =
    sync.delay(FileSystem.get(uri, conf))

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

}
