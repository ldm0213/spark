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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.memory.MemoryMode
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * or ExternalBlockStore, whether to drop the RDD to disk if it falls out of memory or
 * ExternalBlockStore, whether to keep the data in memory in a serialized format, and whether
 * to replicate the RDD partitions on multiple nodes.
 *
 * The [[org.apache.spark.storage.StorageLevel]] singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 */
@DeveloperApi
class StorageLevel private(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useOffHeap: Boolean,
    private var _deserialized: Boolean,
    private var _replication: Int = 1)  extends Externalizable {

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  private def this(flags: Int, replication: Int) {
    this((flags & 8) != 0, (flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication)
  }

  def this() = this(false, true, false, false)

  // 能否写入磁盘。当Block的StorageLevel中的_useDisk为true时，存储体系将允许Block写入磁盘。
  def useDisk: Boolean = _useDisk
  // 能否写入堆内存。当Block的StorageLevel中的_useMemory为true时，存储体系将允许Block写入堆内存。
  def useMemory: Boolean = _useMemory
  // 能否写入堆外内存。当Block的StorageLevel中的_useOffHeap为true时，存储体系将允许Block写入堆外内存。
  def useOffHeap: Boolean = _useOffHeap
  // 表示数据是否已被反序列化。当Block本身经过了序列化后，Block的StorageLevel中的_deserialized将被设置为false，即表示在使用数据时需要对Block进行反序列化。
  def deserialized: Boolean = _deserialized

  //  Block的复制份数。Block的StorageLevel中的_replication默认等于1，可以在构造Block的StorageLevel时明确指定_replication的数量。
  //  当_replication大于1时，Block除了在本地的存储体系中写入一份，还会复制到其他不同节点的存储体系中写入，达到复制备份的目的。
  def replication: Int = _replication

  assert(replication < 40, "Replication restricted to be less than 40 for calculating hash codes")

  if (useOffHeap) { // 使用堆外内存时候必须不能支持反序列化存储数据
    require(!deserialized, "Off-heap storage level does not support deserialized storage")
  }

  private[spark] def memoryMode: MemoryMode = {
    if (useOffHeap) MemoryMode.OFF_HEAP
    else MemoryMode.ON_HEAP
  }

  override def clone(): StorageLevel = {
    new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)
  }

  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      s.useDisk == useDisk &&
      s.useMemory == useMemory &&
      s.useOffHeap == useOffHeap &&
      s.deserialized == deserialized &&
      s.replication == replication
    case _ =>
      false
  }

  def isValid: Boolean = (useMemory || useDisk) && (replication > 0)

  def toInt: Int = {
    var ret = 0
    if (_useDisk) {
      ret |= 8
    }
    if (_useMemory) {
      ret |= 4
    }
    if (_useOffHeap) {
      ret |= 2
    }
    if (_deserialized) {
      ret |= 1
    }
    ret
  }

  /**
   * 将StorageLevel首先通过toInt方法将_useDisk、_useMemory、_useOffHeap、_deserialized四个属性
   * 设置到四位数的状态位，
   * 然后与_replication一起被序列化写入外部二进制流。
   */
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeByte(toInt)
    out.writeByte(_replication)
  }

  // 从外部二进制流中读取StorageLevel的各个属性。
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val flags = in.readByte()
    _useDisk = (flags & 8) != 0
    _useMemory = (flags & 4) != 0
    _useOffHeap = (flags & 2) != 0
    _deserialized = (flags & 1) != 0
    _replication = in.readByte()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)

  override def toString: String = {
    val disk = if (useDisk) "disk" else ""
    val memory = if (useMemory) "memory" else ""
    val heap = if (useOffHeap) "offheap" else ""
    val deserialize = if (deserialized) "deserialized" else ""

    val output =
      Seq(disk, memory, heap, deserialize, s"$replication replicas").filter(_.nonEmpty)
    s"StorageLevel(${output.mkString(", ")})"
  }

  override def hashCode(): Int = toInt * 41 + replication

  def description: String = {
    var result = ""
    result += (if (useDisk) "Disk " else "")
    if (useMemory) {
      result += (if (useOffHeap) "Memory (off heap) " else "Memory ")
    }
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += s"${replication}x Replicated"
    result
  }
}


/**
 * Various [[org.apache.spark.storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  // 预定义的几种存储级别
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val DISK_ONLY_3 = new StorageLevel(true, false, false, false, 3)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(true, true, true, false, 1)

  /**
   * :: DeveloperApi ::
   * Return the StorageLevel object with the specified name.
   * 从指定的字符串得到对应的存储级别
   */
  @DeveloperApi
  def fromString(s: String): StorageLevel = s match {
    case "NONE" => NONE
    case "DISK_ONLY" => DISK_ONLY
    case "DISK_ONLY_2" => DISK_ONLY_2
    case "DISK_ONLY_3" => DISK_ONLY_3
    case "MEMORY_ONLY" => MEMORY_ONLY
    case "MEMORY_ONLY_2" => MEMORY_ONLY_2
    case "MEMORY_ONLY_SER" => MEMORY_ONLY_SER
    case "MEMORY_ONLY_SER_2" => MEMORY_ONLY_SER_2
    case "MEMORY_AND_DISK" => MEMORY_AND_DISK
    case "MEMORY_AND_DISK_2" => MEMORY_AND_DISK_2
    case "MEMORY_AND_DISK_SER" => MEMORY_AND_DISK_SER
    case "MEMORY_AND_DISK_SER_2" => MEMORY_AND_DISK_SER_2
    case "OFF_HEAP" => OFF_HEAP
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $s")
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      useOffHeap: Boolean,
      deserialized: Boolean,
      replication: Int): StorageLevel = {
    getCachedStorageLevel(
      new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object without setting useOffHeap.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      deserialized: Boolean,
      replication: Int = 1): StorageLevel = {
    getCachedStorageLevel(new StorageLevel(useDisk, useMemory, false, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object from its integer representation.
   */
  @DeveloperApi
  def apply(flags: Int, replication: Int): StorageLevel = {
    getCachedStorageLevel(new StorageLevel(flags, replication))
  }

  /**
   * :: DeveloperApi ::
   * Read StorageLevel object from ObjectInput stream.
   */
  @DeveloperApi
  def apply(in: ObjectInput): StorageLevel = {
    val obj = new StorageLevel()
    obj.readExternal(in)
    getCachedStorageLevel(obj)
  }

  private[spark] val storageLevelCache = new ConcurrentHashMap[StorageLevel, StorageLevel]()

  private[spark] def getCachedStorageLevel(level: StorageLevel): StorageLevel = {
    storageLevelCache.putIfAbsent(level, level)
    storageLevelCache.get(level)
  }
}
