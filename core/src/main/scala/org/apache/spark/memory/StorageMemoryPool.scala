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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore

/**
 * Performs bookkeeping for managing an adjustable-size pool of memory that is used for storage
 * (caching).
 *
 * StorageMemoryPool是对用于存储的物理内存的逻辑抽象，通过对存储内存的逻辑管理，提高Spark存储体系对内存的使用效率
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap storage"
    case MemoryMode.OFF_HEAP => "off-heap storage"
  }

  // 已经使用的内存大小（单位为字节）。
  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L

  // 重写内存占用值函数，返回了_memoryUsed属性的值
  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }

  // 当前StorageMemoryPool所关联的MemoryStore,存储空间的实际使用是由MemoryStore来进行控制的
  private var _memoryStore: MemoryStore = _
  // 返回了_memoryStore属性引用的MemoryStore
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   * 设置当前StorageMemoryPool所关联的MemoryStore，实际设置了_memoryStore属性。
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * 用于给BlockId对应的Block获取numBytes指定大小的内存。
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
    val numBytesToFree = math.max(0, numBytes - memoryFree)
    acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
   * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   *                申请内存的BlockId
   * @param numBytesToAcquire the size of this block
   *                申请的内存大小
   * @param numBytesToFree the amount of space to be freed through evicting blocks
   *                本次申请需要额外空出来的内存大小
   * @return whether all N bytes were successfully granted.
   *                所需要的内存是否申请成功了
   */
  def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long): Boolean = lock.synchronized {
    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    assert(memoryUsed <= poolSize)
    if (numBytesToFree > 0) { // 如果需要腾出额外的内存大小，则腾出numBytesToFree属性指定大小的空间
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
    }
    // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
    // back into this StorageMemoryPool in order to free memory. Therefore, these variables
    // should have been updated.
    // 判断可用内存是否充足
    val enoughMemory = numBytesToAcquire <= memoryFree
    if (enoughMemory) { // 可用内存充足，增加已经使用的内存大小
      _memoryUsed += numBytesToAcquire
    }
    enoughMemory
  }

  def releaseMemory(size: Long): Unit = lock.synchronized {
    if (size > _memoryUsed) { // 释放的大小大于已使用大小，则释放当前内存池的所有内存，即将_memoryUsed设置为0。
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else { // 否则从已使用内存大小中减去释放的大小
      _memoryUsed -= size
    }
  }

  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * Free space to shrink the size of this storage memory pool by `spaceToFree` bytes.
   * Note: this method doesn't actually reduce the pool size but relies on the caller to do so.
   * 用于释放指定大小的空间，缩小内存池的大小。
   * @return number of bytes to be removed from the pool's capacity.
   */
  def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    // 计算当前可释放的最大内存大小
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) { // 如果可释放的内存不满足要求释放的大小,需要尝试腾出一些内存
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      val spaceFreedByEviction =
        memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      // 返回最终释放的大小
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else { // 可释放的内存足够，直接返回释放的大小即可
      spaceFreedByReleasingUnusedMemory
    }
  }
}
