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

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 * Tungsten在堆内存模式下使用的内存分配器，与onHeapExecutionMemoryPool配合使用。
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  // 关于MemoryBlock的弱引用的缓冲池，用于Page页（即MemoryBlock）的分配。
  //  当一个对象仅仅被weak reference（弱引用）指向, 而没有任何其他strong reference（强引用）指向的时候,
  //  如果这时GC运行, 那么这个对象就会被回收，不论当前的内存空间是否足够，这个对象都会被回收
  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<long[]>>> bufferPoolsBySize = new HashMap<>();

  // 池化阈值，只有在池化的MemoryBlock大于该值时，才需要被池化
  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   * 只有Size大于阈值才会被池化
   * 当要分配的内存大小大于等于1MB时，需要从bufferPoolsBySize中获取MemoryBlock。
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return size >= POOLING_THRESHOLD_BYTES;
  }

  // 用于分配指定大小（size）的MemoryBlock
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    /**
     * MemoryBlock中以Long类型数组装载数据，所以需要对申请的大小进行转换，
     *  由于申请的是字节数，因此先为其多分配7个字节，避免最终分配的字节数不够，除以8是按照Long类型由8个字节组成来计算的。
     *  例如：申请字节数为50，理想情况应该分配56字节，即7个Long型数据。
     *  如果直接除以8，会得到6，即6个Long型数据，导致只会分配48个字节，
     *  但先加7后再除以8，即 (50 + 7) / 8 = 7个Long型数据，满足分配要求。
     */
    int numWords = (int) ((size + 7) / 8);
    long alignedSize = numWords * 8L; // 补齐后的字节数
    assert (alignedSize >= size);
    if (shouldPool(alignedSize)) { // 需要从池化中拿取
      synchronized (this) {
        final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool != null) {
          while (!pool.isEmpty()) {
            // 取出池链头的MemoryBlock
            final WeakReference<long[]> arrayReference = pool.pop();
            final long[] array = arrayReference.get(); // 拿取array
            if (array != null) {
              // MemoryBlock的大小要比分配的大小大
              assert (array.length * 8L >= size);
              // 从MemoryBlock的缓存中获取指定大小的MemoryBlock并返回
              MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
              if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
                memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
              }
              return memory;
            }
          }
          bufferPoolsBySize.remove(alignedSize);
        }
      }
    }
    /**
     *  走到此处，说明满足以下任意一点：
     *   1. 指定大小的MemoryBlock不需要采用池化机制。
     *   2. bufferPoolsBySize中没有指定大小的MemoryBlock。
     *
     */
    long[] array = new long[numWords];
    // 创建MemoryBlock并返回
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }

  // 用于释放MemoryBlock
  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj != null) :
      "baseObject was null; are you trying to use the on-heap allocator to free off-heap memory?";
    assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must first be freed via TMM.freePage(), not directly in allocator " +
        "free()";

    final long size = memory.size();
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }

    // Mark the page as freed (so we can detect double-frees).
    memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;

    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to null out its reference to the long[] array.
    long[] array = (long[]) memory.obj;
    memory.setObjAndOffset(null, 0);

    long alignedSize = ((size + 7) / 8) * 8;
    if (shouldPool(alignedSize)) {
      // 将MemoryBlock的弱引用放入bufferPoolsBySize中
      synchronized (this) {
        LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(alignedSize, pool);
        }
        pool.add(new WeakReference<>(array));
      }
    } else {
      // Do nothing
    }
  }
}
