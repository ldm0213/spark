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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable.ListBuffer

import com.google.common.io.Closeables
import io.netty.channel.DefaultFileRegion
import org.apache.commons.io.FileUtils

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.{AbstractFileRegion, JavaUtils}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Stores BlockManager blocks on disk.
 *
 * 磁盘存储。依赖于DiskBlockManager，负责对Block的磁盘存储，存储数据的抽象为BlockData。
 * 使用NIO ByteBuffer，MappedByteBuffer，FileChannel等JAVA NIO特性加速文件写入读取
 */
private[spark] class DiskStore(
    conf: SparkConf,
    diskManager: DiskBlockManager,
    securityManager: SecurityManager) extends Logging { // SecurityManager用于提供对数据加密的支持

  // 读取磁盘中的Block时，是直接读取还是使用FileChannel的内存镜像映射方法读取的阈值。由spark.storage.memoryMapThreshold配置，默认为2M
  private val minMemoryMapBytes = conf.get(config.STORAGE_MEMORY_MAP_THRESHOLD)
  // 使用内存映射读取文件的最大阈值，由配置项spark.storage.memoryMapLimitForTests指定。它是个测试参数，默认值为不限制。
  private val maxMemoryMapBytes = conf.get(config.MEMORY_MAP_LIMIT_FOR_TESTS)
  // 维护块ID与其对应大小之间的映射关系的ConcurrentHashMap。
  private val blockSizes = new ConcurrentHashMap[BlockId, Long]()

  def getSize(blockId: BlockId): Long = blockSizes.get(blockId)

  /**
   * Invokes the provided callback function to write the specific block.
   * 处理memoryStore不能载入的数据或者纯写磁盘的数据
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId)(writeFunc: WritableByteChannel => Unit): Unit = {
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    val startTimeNs = System.nanoTime()
    // diskManager负责目录规划和文件创建
    val file = diskManager.getFile(blockId)
    // 利用FileChannel来写入文件，带有count的FileChannel
    val out = new CountingWritableChannel(openForWrite(file))
    var threwException: Boolean = true
    try {
      writeFunc(out) // 参数列表中定义的写入函数，写入到文件中
      blockSizes.put(blockId, out.getCount) // 记录blockId->大小
      threwException = false
    } finally {
      try {
        out.close()
      } catch {
        case ioe: IOException =>
          if (!threwException) {
            threwException = true
            throw ioe
          }
      } finally {
         if (threwException) {
          remove(blockId)
        }
      }
    }
    logDebug(s"Block ${file.getName} stored as ${Utils.bytesToString(file.length())} file" +
      s" on disk in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
  }

  // 用于将BlockId所对应的Block写入磁盘
  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { channel =>
      bytes.writeFully(channel) // 使用FileChannel写出到磁盘，channel在put()中会定义，CountingWritableChannel
    }
  }

  // 用于读取给定BlockId所对应的Block，并封装为BlockData返回
  def getBytes(blockId: BlockId): BlockData = {
    getBytes(diskManager.getFile(blockId.name), getSize(blockId))
  }

  def getBytes(f: File, blockSize: Long): BlockData = securityManager.getIOEncryptionKey() match {
    case Some(key) =>
      // Encrypted blocks cannot be memory mapped; return a special object that does decryption
      // and provides InputStream / FileRegion implementations for reading the data.
      new EncryptedBlockData(f, blockSize, conf, key) // 加密数据

    case _ =>
      new DiskBlockData(minMemoryMapBytes, maxMemoryMapBytes, f, blockSize) // 磁盘数据
  }

  def remove(blockId: BlockId): Boolean = {
    blockSizes.remove(blockId)
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  /**
   * @param blockSize if encryption is configured, the file is assumed to already be encrypted and
   *                  blockSize should be the decrypted size
   */
  def moveFileToBlock(sourceFile: File, blockSize: Long, targetBlockId: BlockId): Unit = {
    blockSizes.put(targetBlockId, blockSize)
    val targetFile = diskManager.getFile(targetBlockId.name)
    FileUtils.moveFile(sourceFile, targetFile)
  }

  def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }

  private def openForWrite(file: File): WritableByteChannel = {
    // 通过文件对象构造了文件输出流FileOutputStream，然后获取它对应的Channel对象用于写数据。
    val out = new FileOutputStream(file).getChannel()
    try {
      // 如果I/O需要加密，就需要另外调用CryptoStreamUtils.createWritableChannel()方法包装
      securityManager.getIOEncryptionKey().map { key =>
        CryptoStreamUtils.createWritableChannel(out, conf, key)
      }.getOrElse(out)
    } catch {
      case e: Exception =>
        Closeables.close(out, true)
        file.delete()
        throw e
    }
  }

}

private class DiskBlockData(
    minMemoryMapBytes: Long,
    maxMemoryMapBytes: Long,
    file: File,
    blockSize: Long) extends BlockData {

  override def toInputStream(): InputStream = new FileInputStream(file)

  /**
  * Returns a Netty-friendly wrapper for the block's data.
  *
  * Please see `ManagedBuffer.convertToNetty()` for more details.
  */
  override def toNetty(): AnyRef = new DefaultFileRegion(file, 0, size)

  /**
   * toChunkedByteBuffer()方法会将文件转化为输入流FileInputStream，并获取其ReadableFileChannel，
   * 再调用JavaUtils.readFully()方法将从Channel中取得的数据填充到ByteBuffer中。
   * 每个ByteBuffer即为一个Chunk，所有Chunk的数组形成最终的ChunkedByteBuffer。
   */
  override def toChunkedByteBuffer(allocator: (Int) => ByteBuffer): ChunkedByteBuffer = {
    Utils.tryWithResource(open()) { channel =>
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) { // 不断读取变成ChunkedByteBuffer里面的块
        val chunkSize = math.min(remaining, maxMemoryMapBytes)
        val chunk = allocator(chunkSize.toInt) // 可以从堆内或者堆外内存分配执行内存
        remaining -= chunkSize
        JavaUtils.readFully(channel, chunk)
        chunk.flip()
        chunks += chunk
      }
      new ChunkedByteBuffer(chunks.toArray)
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    require(blockSize < maxMemoryMapBytes,
      s"can't create a byte buffer of size $blockSize" +
      s" since it exceeds ${Utils.bytesToString(maxMemoryMapBytes)}.")
    Utils.tryWithResource(open()) { channel =>
      if (blockSize < minMemoryMapBytes) {
        // For small files, directly read rather than memory map.
        val buf = ByteBuffer.allocate(blockSize.toInt)
        JavaUtils.readFully(channel, buf)
        buf.flip()
        buf
      } else {
        channel.map(MapMode.READ_ONLY, 0, file.length)
      }
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = {}

  private def open() = new FileInputStream(file).getChannel
}

private[spark] class EncryptedBlockData(
    file: File,
    blockSize: Long,
    conf: SparkConf,
    key: Array[Byte]) extends BlockData {

  override def toInputStream(): InputStream = Channels.newInputStream(open())

  override def toNetty(): Object = new ReadableChannelFileRegion(open(), blockSize)

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    val source = open()
    try {
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        val chunkSize = math.min(remaining, ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(source, chunk)
        chunk.flip()
        chunks += chunk
      }

      new ChunkedByteBuffer(chunks.toArray)
    } finally {
      source.close()
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    // This is used by the block transfer service to replicate blocks. The upload code reads
    // all bytes into memory to send the block to the remote executor, so it's ok to do this
    // as long as the block fits in a Java array.
    assert(blockSize <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
      "Block is too large to be wrapped in a byte buffer.")
    val dst = ByteBuffer.allocate(blockSize.toInt)
    val in = open()
    try {
      JavaUtils.readFully(in, dst)
      dst.flip()
      dst
    } finally {
      Closeables.close(in, true)
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = { }

  private def open(): ReadableByteChannel = {
    val channel = new FileInputStream(file).getChannel()
    try {
      CryptoStreamUtils.createReadableChannel(channel, conf, key)
    } catch {
      case e: Exception =>
        Closeables.close(channel, true)
        throw e
    }
  }
}

private[spark] class EncryptedManagedBuffer(
    val blockData: EncryptedBlockData) extends ManagedBuffer {

  // This is the size of the decrypted data
  override def size(): Long = blockData.size

  override def nioByteBuffer(): ByteBuffer = blockData.toByteBuffer()

  override def convertToNetty(): AnyRef = blockData.toNetty()

  override def createInputStream(): InputStream = blockData.toInputStream()

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this
}

private class ReadableChannelFileRegion(source: ReadableByteChannel, blockSize: Long)
  extends AbstractFileRegion {

  private var _transferred = 0L

  private val buffer = ByteBuffer.allocateDirect(64 * 1024)
  buffer.flip()

  override def count(): Long = blockSize

  override def position(): Long = 0

  override def transferred(): Long = _transferred

  override def transferTo(target: WritableByteChannel, pos: Long): Long = {
    assert(pos == transferred(), "Invalid position.")

    var written = 0L
    var lastWrite = -1L
    while (lastWrite != 0) {
      if (!buffer.hasRemaining()) {
        buffer.clear()
        source.read(buffer)
        buffer.flip()
      }
      if (buffer.hasRemaining()) {
        lastWrite = target.write(buffer)
        written += lastWrite
      } else {
        lastWrite = 0
      }
    }

    _transferred += written
    written
  }

  override def deallocate(): Unit = source.close()
}

/** 带计数器的可写入的FileChannel */
private class CountingWritableChannel(sink: WritableByteChannel) extends WritableByteChannel {

  private var count = 0L

  def getCount: Long = count

  override def write(src: ByteBuffer): Int = {
    val written = sink.write(src)
    if (written > 0) {
      count += written
    }
    written
  }

  override def isOpen(): Boolean = sink.isOpen()

  override def close(): Unit = sink.close()

}
