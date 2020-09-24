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

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 * 执行内存池的实现: 该类用于保证Task合理地进行内存使用，避免因为某些Task过度使用内存导致其它的Task频繁将数据溢写到磁盘。
 *   如果有N个Task，则每个Task所能分配到的内存在总内存的 1 / 2N ~ 1 / N 之间。
 *   由于Task数量是动态的，因此会跟踪所有激活的Task的数量以便重新计算 1 / 2N 和 1 / N 的值。
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 *                   内存模式。用于执行的内存池包括堆内存和堆外内存两种。
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  /**  内存池的名称 */
  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  /**
   * Map from taskAttemptId -> memory consumption in bytes
   * TaskAttempt的身份标识（taskAttemptId）与所消费内存的大小之间的映射关系。
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  /** 重写了memoryUsed方法:
   *    所有TaskAttempt所消费的内存大小之和，即memoryForTask这个Map中所有value的和。 */
  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  /**
   * Returns the memory consumption, in bytes, for the given task.
   *  获取TaskAttempt使用的内存大小，即memoryForTask中taskAttemptId对应的value值。
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * 用于给taskAttemptId对应的TaskAttempt获取指定大小（即numBytes）的内存
   *
   * @param numBytes number of bytes to acquire  分配的内存大小
   * @param taskAttemptId the task attempt acquiring memory   指定的TaskAttempt的ID
   * @param maybeGrowPool      a callback that potentially grows the size of this pool. It takes in
   *                           one parameter (Long) that represents the desired amount of memory by
   *                           which this pool should be expanded.
   *                           回调函数，用于处理潜在的内存池增长情况
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *        用于限制本次分配的最大内存的回调函数，默认传入() => poolSize，即可分配所有内存。
   *        传入回调函数的原因在于，不同的内存管理器对执行内存和存储内存的划分方式是不同的，
   *        例如UnifiedMemoryManager可以通过挤压存储内存区域以扩大执行内存区域。
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long, // 需要申请的内存大小
      taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => (),
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    if (!memoryForTask.contains(taskAttemptId)) {  // 如果memoryForTask中还没有记录taskAttemptId
      // 将taskAttemptId放入memoryForTask，初始状态taskAttemptId所消费的内存为0
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      // 唤醒其他等待获取ExecutionMemoryPool的锁的线程
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      // 获取当前激活的Task的数量
      val numActiveTasks = memoryForTask.keys.size
      // 获取当前TaskAttempt所消费的内存
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      // 执行内存增长策略，有MemoryManager进行实现
      // numBytes - memoryFree计算出不够分配的内存大小，然后尝试从其他内存池[StorageMemoryPool]回收或借用内存
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.
      // 当前能获取到的最大内存，通过增长策略或者会增加内存
      val maxPoolSize = computeMaxPoolSize()
      /** 1/2N ~ 1/N */
      // 计算每个TaskAttempt最大可以使用的内存大小，即 可用总内存大小 / 激活任务数量
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      // 计算每个TaskAttempt最小保证使用的内存大小，即 当前内存池大小 / (激活任务数量 * 2)
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      // 理论上可以分配给当前任务的最大内存（min（申请内存数，任务可获得的内存数））
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      // 实际可以分配给任务的内存（min（可分配最大内存数，当前剩余内存数））
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      // toGrant < numBytes：表示可分配大小小于本次申请需要的大小；
      // curMem + toGrant < minMemoryPerTask：表示该TaskAttempt申请的大小小于单个TaskAttempt可申请的最小大小
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()  // 没有足够多的内存分配，wait等待有Task变化时候进行notifyAll操作唤醒当前线程,唤醒重新计算while循环，还可能拿不到内存
      } else {
        memoryForTask(taskAttemptId) += toGrant // 分配到内存了
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
   * 用于给taskAttemptId对应的TaskAttempt释放指定大小（即numBytes）的内存。
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    val memoryToFree = if (curMem < numBytes) { // 释放内存大于分配给它的内存,只释放可释放的内存大小
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else { // 可释放内存大小大于指定释放的内存
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        // 如果taskAttemptId代表的TaskAttempt占用的内存大小小于等于零，
        // 还需要将taskAttemptId与所消费内存的映射关系从memoryForTask中清除。
        memoryForTask.remove(taskAttemptId)
      }
    }
    // 释放了内存，可以唤醒其他等待内存分配的线程Task进行重新计算和申请内存
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   *         用于释放taskAttemptId对应的TaskAttempt所消费的所有内存。
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId) // 进行释放
    numBytesToFree  // 返回释放的内存大小
  }

}
