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

package org.apache.spark.broadcast

import java.io._
import java.lang.ref.SoftReference
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.{KeyLock, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor).
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * Driver将序列化后的对象切分成许多小块，将这些小块保存在driver的BlockManager中。
 * 在每个executor上，每个executor首先尝试从自己的本地BlockManager上去获取这些小块，
 * 如果不存在，就会从driver或者其他的executor上去获取，一旦获取到了目标小块，
 * 该executor就会将小块保存在自己的BlockManager中，等待被其他的executor获取。
 *
 * 这种机制使得在driver发送多份broadcast数据时（对每一个executor而言）避免成为系统的瓶颈
 *
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager. We hold
   * a soft reference so that it can be garbage collected if required, as we can always reconstruct
   * in the future.
   * 从Executor或者Driver上读取的广播块的值。通过调用readBroadcastBlock()方法获得的广播对象。
   * 该值的类型是SoftReference软引用，表示如果内存空间足够，垃圾回收器就不会回收它；
   * 如果内存空间不足了，就会回收这些对象的内存。只要垃圾回收器没有回收它，该对象就可以被程序使用。
   * 软引用可用来实现内存敏感的高速缓存，可以这样做是因为即使GC了也可以从blockManager从新获取，免得占用内存过多
   */
  @transient private var _value: SoftReference[T] = _

  /** The compression codec to use, or None if compression is disabled
   *  用于广播对象的压缩编解码器。可以设置spark.broadcast.compress属性为true启用，默认是启用的。
   * 最终采用的压缩算法与SerializerManager中的CompressionCodec是一致的。
   */
  @transient private var compressionCodec: Option[CompressionCodec] = _

  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster.
   * 每个块的大小。只读属性，可以使用spark.broadcast.blockSize属性进行配置，默认为4MB。
   */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf): Unit = {
    compressionCodec = if (conf.get(config.BROADCAST_COMPRESS)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    // spark.broadcast.blockSize设置broadcast的每个块的大小,默认为4M
    blockSize = conf.get(config.BROADCAST_BLOCKSIZE).toInt * 1024
    // spark.broadcast.checksum控制是否checksum
    checksumEnabled = conf.get(config.BROADCAST_CHECKSUM)
  }
  // 设置压缩器和块大小
  setConf(SparkEnv.get.conf)

  // "broadcast_" + broadcastId + (if (field == "") "" else "_" + field)
  private val broadcastId = BroadcastBlockId(id)

  /** Total number of blocks this broadcast variable contains.
   * 直接写入Block<Memory+Disk>
   * */
  private val numBlocks: Int = writeBlocks(obj)

  /** Whether to generate checksum for blocks or not. */
  private var checksumEnabled: Boolean = false
  /** The checksum for all the blocks. */
  private var checksums: Array[Int] = _

  /** 获取broadcast对象的值,如果读取过了直接返回，否则从从driver或者其他executor读取 */
  override protected def getValue() = synchronized {
    val memoized: T = if (_value == null) null.asInstanceOf[T] else _value.get
    if (memoized != null) {
      memoized
    } else {
      val newlyRead = readBroadcastBlock()
      _value = new SoftReference[T](newlyRead)
      newlyRead
    }
  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position(), block.limit()
        - block.position())
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   *  1. value对象putSingle到driver本地
   *  2. 对象按block拆分成ByteBuffer数组：blockifyObject
   *  3. 计算每个block的校验和
   *  4. 将每个block进行putBytes
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    val blockManager = SparkEnv.get.blockManager
    /**
     * 使用BlockManager对象将广播对象写入本地的存储系统。
     * 当Spark以local模式运行时，则会将广播对象写入Driver本地的存储体系，以便于任务也可以在Driver上执行。
     * 由于MEMORY_AND_DISK对应的StorageLevel的_replication属性固定为1，
     * 因此此处只会将广播对象写入Driver或Executor本地的存储体系
     */
    if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
      throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }
    // 将对象经过序列化、压缩转换成一系列的字节块Array[ByteBuffer]，每个块大小: 4*1024*1024
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
    if (checksumEnabled) {
      checksums = new Array[Int](blocks.length)
    }
    // 对于每个块进行处理写入到
    blocks.zipWithIndex.foreach { case (block, i) =>
      if (checksumEnabled) {  // 为每个ByteBuffer的block计算校验和，并保存到数组
        checksums(i) = calcChecksum(block)
      }
      // broadcast_0_piece0、broadcast_0_piece1、broadcast_0_piece2...
      val pieceId = BroadcastBlockId(id, "piece" + i)
      // 根据块构建ChunkedByteBuffer缓冲区
      val bytes = new ChunkedByteBuffer(block.duplicate())
      // 将每个分片广播数据块以序列化方式写入Driver本地的存储体系，存储方式为MEMORY_AND_DISK_SER，同时tellMaster注册成为下载源
      if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
        throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
      }
    }
    // 返回块数量
    blocks.length
  }

  /** Fetch torrent blocks from the driver and/or other executors.
   * 从Driver、Executor的存储体系中获取块
   */
  private def readBlocks(): Array[BlockData] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[BlockData](numBlocks)
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      bm.getLocalBytes(pieceId) match {
        case Some(block) => // 本地能获取到
          blocks(pid) = block
          releaseBlockManagerLock(pieceId)
        case None =>
          bm.getRemoteBytes(pieceId) match {  // 尝试远程获取
            case Some(b) =>
              if (checksumEnabled) { // 拉取的数据进行校验
                val sum = calcChecksum(b.chunks(0))
                if (sum != checksums(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId of $broadcastId:" +
                    s" $sum != ${checksums(pid)}")
                }
              }
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              // 将分片广播块写入本地存储体系，以便于当前Executor的其他任务不用再次获取分片广播块
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  // 获取数据的值
  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    // 对broadcastId加锁
    TorrentBroadcast.torrentBroadcastLock.withLock(broadcastId) {
      // As we only lock based on `broadcastId`, whenever using `broadcastCache`, we should only
      // touch `broadcastId`.
      val broadcastCache = SparkEnv.get.broadcastManager.cachedValues

      Option(broadcastCache.get(broadcastId)).map(_.asInstanceOf[T]).getOrElse { // cache里面没有时候
        setConf(SparkEnv.get.conf)
        val blockManager = SparkEnv.get.blockManager
        // 使用BlockManager组件从本地的存储系统中获取广播对象
        blockManager.getLocalValues(broadcastId) match {
          case Some(blockResult) =>
            if (blockResult.data.hasNext) {
              val x = blockResult.data.next().asInstanceOf[T]
              releaseBlockManagerLock(broadcastId)

              if (x != null) { // 做一层cache操作
                broadcastCache.put(broadcastId, x)
              }

              x
            } else {
              throw new SparkException(s"Failed to get locally stored broadcast data: $broadcastId")
            }
          case None => // 获取不到广播对象，说明数据是通过BlockManager的putBytes方法以序列化方式写入存储体系的。
            val estimatedTotalSize = Utils.bytesToString(numBlocks * blockSize)
            logInfo(s"Started reading broadcast variable $id with $numBlocks pieces " +
              s"(estimated total size $estimatedTotalSize)")
            val startTimeNs = System.nanoTime()
            val blocks = readBlocks()  // 从Driver或Executor的存储体系中获取广播块
            logInfo(s"Reading broadcast variable $id took ${Utils.getUsedTimeNs(startTimeNs)}")

            try {
              // 将一系列的分片广播块转换回原来的广播对象
              val obj = TorrentBroadcast.unBlockifyObject[T](
                blocks.map(_.toInputStream()), SparkEnv.get.serializer, compressionCodec)
              // Store the merged copy in BlockManager so other tasks on this executor don't
              // need to re-fetch it.
              val storageLevel = StorageLevel.MEMORY_AND_DISK
              // 将广播对象写入本地的存储体系，便于当前Executor的其他任务不用再次获取广播对象
              if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
                throw new SparkException(s"Failed to store $broadcastId in BlockManager")
              }

              if (obj != null) { // 缓存起来
                broadcastCache.put(broadcastId, obj)
              }

              obj
            } finally {
              blocks.foreach(_.dispose())
            }
        }
      }
    }
  }

  /**
   * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseBlockManagerLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener[Unit](_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

}


private object TorrentBroadcast extends Logging {

  /**
   * A [[KeyLock]] whose key is [[BroadcastBlockId]] to ensure there is only one thread fetching
   * the same [[TorrentBroadcast]] block.
   */
  private val torrentBroadcastLock = new KeyLock[BroadcastBlockId]

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    // 基于blockSize创建ChunkedByteBufferOutputStream流
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    // 使用压缩器对ChunkedByteBufferOutputStream流包装进行包装
    val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    // 使用序列化器再次包装流
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      // 使用最终的输出流进行输出
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    // 返回块集合，Array[ByteBuffer]类型
    cbbos.toChunkedByteBuffer.getChunks()
  }

  // 将Array[InputStream]转换成对象
  // 先解压，再反序列化。InputStream是从内而外的操作数据
  def unBlockifyObject[T: ClassTag](
      blocks: Array[InputStream],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    // 顺序合并流SequenceInputStream
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    // 解压
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    // 反序列化
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]() // 读取
    } {
      serIn.close()
    }
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
