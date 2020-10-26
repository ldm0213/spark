/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * 磁盘块管理器。对磁盘上的文件及目录的读写操作进行管理。
 * 负责为逻辑的Block与数据写入磁盘的位置之间建立逻辑的映射关系。
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */

// deleteFilesOnStop: 停止DiskBlockManager的时候是否删除本地目录的布尔类型标记。
// 当不指定外部的ShuffleClient（即spark.shuffle.service.enabled属性为false）或者当前实例是Driver时，此属性为true。
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {

  // 磁盘存储DiskStore的本地子目录的数量。可以通过spark.diskStore.subDirectories属性配置，默认为64。
  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  // 本地目录的数组。默认获取spark.local.dir属性或者系统属性java.io.tmpdir指定的目录，目录可能有多个。
  // 并在每个路径下创建以blockmgr-为前缀，UUID为后缀的随机字符串的子目录，例如，blockmgr-f4cf9ae6-9213-4178-98a7-11b4a1fe12c7。
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  // 本地文件一级目录的名字Array
  private[spark] val localDirsString: Array[String] = localDirs.map(_.toString)

  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  // DiskStore的本地子目录的二维数组：
  //   - 一维大小为spark.local.dir属性或者系统属性java.io.tmpdir指定的目录的个数。
  //   - 二维大小为subDirsPerLocalDir，即spark.diskStore.subDirectories指定的大小，默认为64。
  //   - 元素为File对象。
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  // 结束回调函数
  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExecutorDiskUtils#getFile().
  // 根据指定的文件名获取文件。
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    // 获取文件名的非负哈希值
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % localDirs.length     // 按照Hash取余获取一级目录
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir     // 按照Hash取余获取二级目录

    // Create the subdirectory if it doesn't already exist
    // 尝试获取对应的二级目录
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {  // 目录不为空
        old
      } else { // 目录为空，需要创建新的目录
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists()) {
          Files.createDirectory(newDir.toPath)
        }
        // 记录到subDirs数组中
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  // 此方法根据BlockId获取文件,blockId的name是文件命名
  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block. 用于检查本地localDirs目录中是否包含BlockId对应的文件 */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. 用于获取本地localDirs目录中的所有文件 */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir => // 一层目录遍历
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone() // 加锁后克隆一份，避免线程安全问题
      }
    }.filter(_ != null).flatMap { dir => // 二层目录遍历
      val files = dir.listFiles()
      if (files != null) files.toSeq else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    // 使用getAllFiles()获取所有文件，构造为BlockId对象数组返回
    getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          // Skip files which do not correspond to blocks, for example temporary
          // files created by [[SortShuffleWriter]].
          None
      }
    }
  }

  /** Produces a unique block id and File suitable for storing local intermediate results.
   * 用于为中间结果创建唯一的BlockId和文件，此文件将用于保存本地Block的数据。
   */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results.
   * 创建唯一的BlockId和文件，用来存储Shuffle中间结果（即Map任务的输出）
   */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    // 获取一级目录的路径，并进行flatMap
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        // 在每个一级目录下都创建名为"blockmgr-UUID字符串"的子目录
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    // 虚拟机关闭钩子
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop() // 在虚拟机关闭时也关闭DiskBlockManager
    }
  }

  /** Cleanup local dirs and stop shuffle sender.正常停止DiskBlockManager */
  private[spark] def stop(): Unit = {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>  // 遍历一级目录
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir) // 递归删除一级目录及其中的内容
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
