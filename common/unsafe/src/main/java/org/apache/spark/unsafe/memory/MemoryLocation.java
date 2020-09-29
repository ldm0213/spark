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

import javax.annotation.Nullable;

/**
 * A memory location. Tracked either by a memory address (with off-heap allocation),
 * or by an offset from a JVM object (on-heap allocation).
 * 为了统一on-heap<JVM管理>和off-heap<自行管理>的执行内存，抽象出来一个MemoryLocation
 */
public class MemoryLocation {

  /**
   * Tungsten处于堆内存模式时，数据作为对象存储在JVM的堆上，此时的obj不为空。
   * Tungsten处于堆外内存模式时，数据存储在JVM的堆外内存（操作系统内存）中，因而不会在JVM中存在对象。
   */
  @Nullable
  Object obj;

  /**
   * offset属性主要用来定位数据。
   * 当Tungsten处于堆内存模式时，首先从堆内找到对象，然后使用offset定位数据的具体位置。
   * 当Tungsten处于堆外内存模式时，则直接使用offset从堆外内存中定位。
   */
  long offset;

  public MemoryLocation(@Nullable Object obj, long offset) {
    this.obj = obj;
    this.offset = offset;
  }

  public MemoryLocation() {
    this(null, 0);
  }

  // 设置新的obj和offset
  public void setObjAndOffset(Object newObj, long newOffset) {
    this.obj = newObj;
    this.offset = newOffset;
  }

  public final Object getBaseObject() {
    return obj;
  }

  public final long getBaseOffset() {
    return offset;
  }
}
