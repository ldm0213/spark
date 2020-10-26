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

import java.io.IOException
import java.util.{HashMap => JHashMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.Random
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.{MapOutputTrackerMaster, SparkConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcCallContext, RpcEndpointAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedClusterMessages, CoarseGrainedSchedulerBackend}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}

/**
 * BlockManagerMasterEndpoint is an [[IsolatedRpcEndpoint]] on the master node to track statuses
 * of all the storage endpoints' block managers.
 *
 * BlockManagerMasterEndpoint由Driver上的SparkEnv负责创建和注册到Driver的RpcEnv中。
 * BlockManagerMasterEndpoint只存在于Driver的SparkEnv中，
 * Driver或Executor上的BlockManagerMaster有driverEndpoint属性，将持有BlockManagerMasterEndpoint的RpcEndpointRef。
 *
 * BlockManagerMasterEndpoint接收Driver或Executor上BlockManagerMaster发送的消息，对所有的BlockManager统一管理
 * 主要对各个节点上的BlockManager、BlockManager与Executor的映射关系，及Block位置信息（即Block所在的BlockManager）等进行管理。
 *
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    externalBlockStoreClient: Option[ExternalBlockStoreClient],
    //  BlockManagerInfo会管理对应的BlockManager的元数据信息，他们之间通过心跳连接不断地对信息进行更新，每当有对应的Block被操作的时候，此信息都会发生改变
    blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo],
    mapOutputTracker: MapOutputTrackerMaster)
  extends IsolatedRpcEndpoint with Logging {

  // Mapping from executor id to the block manager's local disk directories.
  // 存储executor标识 -> executor存储磁盘文件的一级目录缓存
  private val executorIdToLocalDirs =
    CacheBuilder
      .newBuilder()
      .maximumSize(conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE)) // 默认最大1000
      .build[String, Array[String]]()

  // Mapping from external shuffle service block manager id to the block statuses.
  // 每个BlockManager上面的Block的状态信息
  private val blockStatusByShuffleService =
    new mutable.HashMap[BlockManagerId, JHashMap[BlockId, BlockStatus]]

  // Mapping from executor ID to block manager ID.
  /** executor-id到BlockManagerId的HashMap，可以通过executor-id找到BlockManagerId的信息 */
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Set of block managers which are decommissioning
  // 下线的BlockManager
  private val decommissioningBlockManagerSet = new mutable.HashSet[BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  /** 同一个数据块可能被多个BlockManager管理，通过一个BlockId找到一个BlockManagerId集合 */
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  /** 创建一个线程池，用来处理各个slave的BlockManger发送过来的信息 */
  private val askThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool", 100)
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  /** 对集群所有节点的拓扑结构的映射 */
  private val topologyMapper = {
    val topologyMapperClassName = conf.get(
      config.STORAGE_REPLICATION_TOPOLOGY_MAPPER)
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(s"Using $topologyMapperClassName for getting topology information")
    mapper
  }

  val proactivelyReplicate = conf.get(config.STORAGE_REPLICATION_PROACTIVE)

  // rpc超时时间
  val defaultRpcTimeout = RpcUtils.askRpcTimeout(conf)

  logInfo("BlockManagerMasterEndpoint up")
  // same as `conf.get(config.SHUFFLE_SERVICE_ENABLED)
  //   && conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)`
  // 是否开启了外部shuffle服务以及外部shuffle的port
  private val externalShuffleServiceRddFetchEnabled: Boolean = externalBlockStoreClient.isDefined
  private val externalShuffleServicePort: Int = StorageUtils.externalShuffleServicePort(conf)

  // driver的RpcEnv
  private lazy val driverEndpoint =
    RpcUtils.makeDriverRef(CoarseGrainedSchedulerBackend.ENDPOINT_NAME, conf, rpcEnv)

  /** 接收和处理消息 */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    /** executor启动时候会注册BlockManager：需要executor端的id,一级目录,堆内最大存储内存,堆外最大存储内存,executor端的slaveEndpoint Ref */
    case RegisterBlockManager(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint) =>
      context.reply(register(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint))

    /** 更新BlockInfo信息，主要委托给BlockManagerInfo来对元数据的管理更新删除操作，同时更新块对应的位置信息 */
    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) => // 更新块信息
      val isSuccess = updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size)
      context.reply(isSuccess)
      // SPARK-30594: we should not post `SparkListenerBlockUpdated` when updateBlockInfo
      // returns false since the block info would be updated again later.
      if (isSuccess) {
        listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
      }

    /** 获取blockId在的位置，MasterEndpoint维护了blockId的位置信息 */
    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    /** 获取blockId在的位置和状态，如果开启了外部shuffle服务，可能还会考虑就近拿取数据即使executor挂掉了 */
    case GetLocationsAndStatus(blockId, requesterHost) =>
      context.reply(getLocationsAndStatus(blockId, requesterHost))

    /** 获取blockIds在的位置 */
    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askStorageEndpoints) =>
      context.reply(blockStatus(blockId, askStorageEndpoints))

    /** exector是否还存活 */
    case IsExecutorAlive(executorId) =>
      context.reply(blockManagerIdByExecutor.contains(executorId))

    case GetMatchingBlockIds(filter, askStorageEndpoints) =>
      context.reply(getMatchingBlockIds(filter, askStorageEndpoints))

    /** 删除rdd，shuffle， broadcast，blockID以及exector */
    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case DecommissionBlockManagers(executorIds) =>
      val bmIds = executorIds.flatMap(blockManagerIdByExecutor.get)
      decommissionBlockManagers(bmIds)
      context.reply(true)

    case GetReplicateInfoForRDDBlocks(blockManagerId) =>
      context.reply(getReplicateInfoForRDDBlocks(blockManagerId))

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()
  }

  /**
   * A function that used to handle the failures when removing blocks. In general, the failure
   * should be considered as non-fatal since it won't cause any correctness issue. Therefore,
   * this function would prefer to log the exception and return the default value. We only throw
   * the exception when there's a TimeoutException from an active executor, which implies the
   * unhealthy status of the executor while the driver still not be aware of it.
   * @param blockType should be one of "RDD", "shuffle", "broadcast", "block", used for log
   * @param blockId the string value of a certain block id, used for log
   * @param bmId the BlockManagerId of the BlockManager, where we're trying to remove the block
   * @param defaultValue the return value of a failure removal. e.g., 0 means no blocks are removed
   * @tparam T the generic type for defaultValue, Int or Boolean.
   * @return the defaultValue or throw exception if the executor is active but reply late.
   */
  private def handleBlockRemovalFailure[T](
      blockType: String,
      blockId: String,
      bmId: BlockManagerId,
      defaultValue: T): PartialFunction[Throwable, T] = {
    case e: IOException =>
      logWarning(s"Error trying to remove $blockType $blockId" +
        s" from block manager $bmId", e)
      defaultValue

    case t: TimeoutException => // 超时通过心跳连接获取到
      val executorId = bmId.executorId
      val isAlive = try { // 再次确认
        driverEndpoint.askSync[Boolean](CoarseGrainedClusterMessages.IsExecutorAlive(executorId))
      } catch {
        // ignore the non-fatal error from driverEndpoint since the caller doesn't really
        // care about the return result of removing blocks. And so we could avoid breaking
        // down the whole application.
        case NonFatal(e) =>
          logError(s"Fail to know the executor $executorId is alive or not.", e)
          false
      }
      if (!isAlive) {
        logWarning(s"Error trying to remove $blockType $blockId. " +
          s"The executor $executorId may have been lost.", t)
        defaultValue
      } else {
        throw t
      }
  }

  /** 将rddId的文件删除 */
  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the storage endpoints.

    // The message sent to the storage endpoints to remove the RDD
    val removeMsg = RemoveRdd(rddId)

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks and create the futures which asynchronously
    // remove the blocks from storage endpoints and gives back the number of removed blocks
    // 查找哪些BlockInfo持有改block,由于副本的存在可能导致一个block有多个存储位置
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    val blocksToDeleteByShuffleService =
      new mutable.HashMap[BlockManagerId, mutable.HashSet[RDDBlockId]]

    // 先删除元信息
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.remove(blockId)

      // 区分开启了外部shuffle的executor和不开启外部shuffle的executor
      val (bmIdsExtShuffle, bmIdsExecutor) = bms.partition(_.port == externalShuffleServicePort)
      val liveExecutorsForBlock = bmIdsExecutor.map(_.executorId).toSet
      // 开启外部shuffle的executor，删除blockStatusByShuffleService中的信息
      bmIdsExtShuffle.foreach { bmIdForShuffleService =>
        // if the original executor is already released then delete this disk block via
        // the external shuffle service
        if (!liveExecutorsForBlock.contains(bmIdForShuffleService.executorId)) {
          val blockIdsToDel = blocksToDeleteByShuffleService.getOrElseUpdate(bmIdForShuffleService,
            new mutable.HashSet[RDDBlockId]())
          blockIdsToDel += blockId
          blockStatusByShuffleService.get(bmIdForShuffleService).foreach { blockStatus =>
            blockStatus.remove(blockId)
          }
        }
      }

      // 没有开启外部shuffle的通过blockManagerInfo删除
      bmIdsExecutor.foreach { bmId =>
        blockManagerInfo.get(bmId).foreach { bmInfo =>
          bmInfo.removeBlock(blockId)
        }
      }
    }
    // 发送消息让executor删除
    val removeRddFromExecutorsFutures = blockManagerInfo.values.map { bmInfo =>
      bmInfo.storageEndpoint.ask[Int](removeMsg).recover {
        // use 0 as default value means no blocks were removed
        handleBlockRemovalFailure("RDD", rddId.toString, bmInfo.blockManagerId, 0)
      }
    }.toSeq

    // 通过外部shuffle服务删除
    val removeRddBlockViaExtShuffleServiceFutures = externalBlockStoreClient.map { shuffleClient =>
      blocksToDeleteByShuffleService.map { case (bmId, blockIds) =>
        Future[Int] { // 通过shuffleClient调用删除block信息
          val numRemovedBlocks = shuffleClient.removeBlocks(
            bmId.host,
            bmId.port,
            bmId.executorId,
            blockIds.map(_.toString).toArray)
          numRemovedBlocks.get(defaultRpcTimeout.duration.toSeconds, TimeUnit.SECONDS)
        }
      }
    }.getOrElse(Seq.empty)

    Future.sequence(removeRddFromExecutorsFutures ++ removeRddBlockViaExtShuffleServiceFutures)
  }

  /** 将shuffle过程中产生的文件删除 */
  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        // 给Executor发消息，让其删除块
        bm.storageEndpoint.ask[Boolean](removeMsg).recover {
          // use false as default value means no shuffle data were removed
          handleBlockRemovalFailure("shuffle", shuffleId.toString, bm.blockManagerId, false)
        }
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    val futures = requiredBlockManagers.map { bm =>
      bm.storageEndpoint.ask[Int](removeMsg).recover {
        // use 0 as default value means no blocks were removed
        handleBlockRemovalFailure("broadcast", broadcastId.toString, bm.blockManagerId, 0)
      }
    }.toSeq

    Future.sequence(futures)
  }

  private def removeBlockManager(blockManagerId: BlockManagerId): Unit = {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId
    decommissioningBlockManagerSet.remove(blockManagerId)

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)

    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) { // 删除他持有的块信息
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      // De-register the block if none of the block managers have it. Otherwise, if pro-active
      // replication is enabled, and a block is either an RDD or a test block (the latter is used
      // for unit testing), we send a message to a randomly chosen executor location to replicate
      // the given block. Note that we ignore other block types (such as broadcast/shuffle blocks
      // etc.) as replication doesn't make much sense in that context.
      if (locations.size == 0) {
        blockLocations.remove(blockId)
        logWarning(s"No more replicas available for $blockId !")
      } else if (proactivelyReplicate && (blockId.isRDD || blockId.isInstanceOf[TestBlockId])) {
        // As a heursitic, assume single executor failure to find out the number of replicas that
        // existed before failure
        val maxReplicas = locations.size + 1
        val i = (new Random(blockId.hashCode)).nextInt(locations.size)
        val blockLocations = locations.toSeq
        val candidateBMId = blockLocations(i)
        blockManagerInfo.get(candidateBMId).foreach { bm =>
          val remainingLocations = locations.toSeq.filter(bm => bm != candidateBMId)
          val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
          bm.storageEndpoint.ask[Boolean](replicateMsg)
        }
      }
    }

    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")

  }

  // 删除executor主要是删除他持有的blockManagerInfo
  private def removeExecutor(execId: String): Unit = {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Decommission the given Seq of blockmanagers
   *    - Adds these block managers to decommissioningBlockManagerSet Set
   *    - Sends the DecommissionBlockManager message to each of the [[BlockManagerReplicaEndpoint]]
   */
  def decommissionBlockManagers(blockManagerIds: Seq[BlockManagerId]): Future[Seq[Unit]] = {
    val newBlockManagersToDecommission = blockManagerIds.toSet.diff(decommissioningBlockManagerSet)
    val futures = newBlockManagersToDecommission.map { blockManagerId =>
      decommissioningBlockManagerSet.add(blockManagerId)
      val info = blockManagerInfo(blockManagerId)
      info.storageEndpoint.ask[Unit](DecommissionBlockManager)
    }
    Future.sequence{ futures.toSeq }
  }

  /**
   * Returns a Seq of ReplicateBlock for each RDD block stored by given blockManagerId
   * @param blockManagerId - block manager id for which ReplicateBlock info is needed
   * @return Seq of ReplicateBlock
   */
  private def getReplicateInfoForRDDBlocks(blockManagerId: BlockManagerId): Seq[ReplicateBlock] = {
    val info = blockManagerInfo(blockManagerId)

    val rddBlocks = info.blocks.keySet().asScala.filter(_.isRDD)
    rddBlocks.map { blockId =>
      val currentBlockLocations = blockLocations.get(blockId)
      val maxReplicas = currentBlockLocations.size + 1
      val remainingLocations = currentBlockLocations.toSeq.filter(bm => bm != blockManagerId)
      val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
      replicateMsg
    }.toSeq
  }

  // Remove a block from the workers that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId): Unit = {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        blockManager.foreach { bm =>
          // Remove the block from the BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          bm.storageEndpoint.ask[Boolean](RemoveBlock(blockId)).recover {
            // use false as default value means no blocks were removed
            handleBlockRemovalFailure("block", blockId.toString, bm.blockManagerId, false)
          }
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, Some(info.maxOnHeapMem),
        Some(info.maxOffHeapMem), info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askStorageEndpoints is true, the master queries each block manager for the most updated
   * block statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askStorageEndpoints: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askStorageEndpoints) {
          info.storageEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askStorageEndpoints is true, the master queries each block manager for the most updated
   * block statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askStorageEndpoints: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askStorageEndpoints) {
            info.storageEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  private def externalShuffleServiceIdOnHost(blockManagerId: BlockManagerId): BlockManagerId = {
    // we need to keep the executor ID of the original executor to let the shuffle service know
    // which local directories should be used to look for the file
    BlockManagerId(blockManagerId.executorId, blockManagerId.host, externalShuffleServicePort)
  }

  /**
   * Returns the BlockManagerId with topology information populated, if available.
   * 注册executor的BlockManager端的Master来统一管理
   */
  private def register(
      idWithoutTopologyInfo: BlockManagerId, // 注册时候还没有topology信息，需要Master来分配填充
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      storageEndpoint: RpcEndpointRef): BlockManagerId = {
    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    // 补充拓扑信息，并添加到blockManagerInfo中
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host)) // 主要是使用topologyMapper生产拓扑信息

    val time = System.currentTimeMillis()
    executorIdToLocalDirs.put(id.executorId, localDirs)
    if (!blockManagerInfo.contains(id)) { // 当前并未管理该BlockManagerId的BlockManagerInfo对象
      // 根据BlockManagerId中保存的Executor ID获取旧的BlockManagerId
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) => // 存在，则移除
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxOnHeapMemSize + maxOffHeapMemSize), id))

      // 更新blockManagerIdByExecutor字典
      blockManagerIdByExecutor(id.executorId) = id

      // 外部shuffle服务
      val externalShuffleServiceBlockStatus =
        if (externalShuffleServiceRddFetchEnabled) {
          val externalShuffleServiceBlocks = blockStatusByShuffleService
            .getOrElseUpdate(externalShuffleServiceIdOnHost(id), new JHashMap[BlockId, BlockStatus])
          Some(externalShuffleServiceBlocks)
        } else {
          None
        }
      // 更新blockManagerInfo字典，添加新的BlockManagerInfo对象
      blockManagerInfo(id) = new BlockManagerInfo(id, System.currentTimeMillis(),
        maxOnHeapMemSize, maxOffHeapMemSize, storageEndpoint, externalShuffleServiceBlockStatus)
    }
    // 触发对所有SparkListener的onBlockManagerAdded方法的调用，进而达到监控的目的。
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxOnHeapMemSize + maxOffHeapMemSize,
        Some(maxOnHeapMemSize), Some(maxOffHeapMemSize)))
    id
  }

  // 更新状态，同时更新location信息
  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId, // 块标识
      storageLevel: StorageLevel, // 存储级别
      memSize: Long, // 使用的内存大小
      diskSize: Long): Boolean = { // 使用的磁盘大小
    logDebug(s"Updating block info on master ${blockId} for ${blockManagerId}")

    if (blockId.isShuffle) { // shuffle中间文件是由MapOutputTracker记录的，所以不需要做什么处理
      blockId match {
        case ShuffleIndexBlockId(shuffleId, mapId, _) => // shuffle index文件
          // Don't update the map output on just the index block
          logDebug(s"Received shuffle index block update for ${shuffleId} ${mapId}, ignoring.")
          return true
        case ShuffleDataBlockId(shuffleId: Int, mapId: Long, reduceId: Int) => // Shuffle数据文件
          logDebug(s"Received shuffle data block update for ${shuffleId} ${mapId}, updating.")
          mapOutputTracker.updateMapOutput(shuffleId, mapId, blockManagerId) // 更新mapOutputTracker
          return true
        case _ =>
          logDebug(s"Unexpected shuffle block type ${blockId}" +
            s"as ${blockId.getClass().getSimpleName()}")
          return false
      }
    }

    if (!blockManagerInfo.contains(blockManagerId)) { // 判断是否存在对应的BlockManager
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {  // 如果指定的数据块的BlockId为空，仅仅更新BlockManagerInfo中记录的最后更新时间
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    // blockManagerInfo来更新Block的状态信息
    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    // 同一个数据块可能被多个BlockManager管理，通过一个BlockId找到一个BlockManagerId集合
    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    // 存储级别有效，需要将BlockManagerId添加到数据块的位置信息中
    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    if (blockId.isRDD && storageLevel.useDisk && externalShuffleServiceRddFetchEnabled) {
      val externalShuffleServiceId = externalShuffleServiceIdOnHost(blockManagerId)
      if (storageLevel.isValid) {
        locations.add(externalShuffleServiceId)
      } else {
        locations.remove(externalShuffleServiceId)
      }
    }

    // Remove the block from master tracking if it has been removed on all endpoints.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  // 获取位置信息
  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  /** 获取Block的位置信息以及状态信息 */
  private def getLocationsAndStatus(
      blockId: BlockId,
      requesterHost: String): Option[BlockLocationsAndStatus] = {
    val locations = Option(blockLocations.get(blockId)).map(_.toSeq).getOrElse(Seq.empty)
    val status = locations.headOption.flatMap { bmId =>
      if (externalShuffleServiceRddFetchEnabled && bmId.port == externalShuffleServicePort) {
        Option(blockStatusByShuffleService(bmId).get(blockId))
      } else {
        blockManagerInfo.get(bmId).flatMap(_.getStatus(blockId))
      }
    }

    if (locations.nonEmpty && status.isDefined) {
      val localDirs = locations.find { loc =>
        // When the external shuffle service running on the same host is found among the block
        // locations then the block must be persisted on the disk. In this case the executorId
        // can be used to access this block even when the original executor is already stopped.
        loc.host == requesterHost &&
          (loc.port == externalShuffleServicePort ||
            blockManagerInfo
              .get(loc)
              .flatMap(_.getStatus(blockId).map(_.storageLevel.useDisk))
              .getOrElse(false))
      }.flatMap { bmId => Option(executorIdToLocalDirs.getIfPresent(bmId.executorId)) }
      Some(BlockLocationsAndStatus(locations, status.get, localDirs))
    } else {
      None
    }
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      // 将传入的BlockManagerId过滤掉
      blockManagerIds
        .filterNot { _.isDriver }
        .filterNot { _ == blockManagerId }
        .diff(decommissioningBlockManagerSet)
        .toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerReplicaEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.storageEndpoint
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

/**  BlockManagerInfo会管理对应的BlockManager的元数据信息，块信息，堆内，堆外内存等
 *   Driver与Executor不断通信进行更新，每当有对应的Block被操作的时候，此信息都会发生改变 */
private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxOnHeapMem: Long,
    val maxOffHeapMem: Long,
    val storageEndpoint: RpcEndpointRef, // slave节点的RpcEndPoint
    val externalShuffleServiceBlockStatus: Option[JHashMap[BlockId, BlockStatus]])
  extends Logging {

  val maxMem = maxOnHeapMem + maxOffHeapMem

  val externalShuffleServiceEnabled = externalShuffleServiceBlockStatus.isDefined

  // 记录最后一次访问当前BlockManagerInfo的时间
  private var _lastSeenMs: Long = timeMs
  // 记录当前BlockManagerInfo对应的BlockManager管理的所有数据块的剩余的可用内存大小
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  // 记录当前BlockManagerInfo对应的BlockManager管理的所有数据块的BlockStatus状态对象
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // 获取指定数据块的BlockStatus
  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs(): Unit = {
    _lastSeenMs = System.currentTimeMillis()
  }

  // master接收到更新消息后进行更新
  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Unit = {

    updateLastSeenMs()

    val blockExists = _blocks.containsKey(blockId)
    var originalMemSize: Long = 0
    var originalDiskSize: Long = 0
    var originalLevel: StorageLevel = StorageLevel.NONE

    if (blockExists) {
      // The block exists on the storage endpoint already.
      // 获取数据块对应的BlockStatus
      val blockStatus: BlockStatus = _blocks.get(blockId)
      // 记录旧的值
      originalLevel = blockStatus.storageLevel
      originalMemSize = blockStatus.memSize
      originalDiskSize = blockStatus.diskSize

      if (originalLevel.useMemory) {
        /**
         * 因为原来的存储级别使用的内存，现在要对其进行修改，
         * 则先将旧的内存值累加到_remainingMem中，说明将该数据块原来使用的内存大小归还了，
         * 在后面的操作还会将新的内存值从_remainingMem中减去，说明又将特定的内存大小分配给该数据块了。
         */
        _remainingMem += originalMemSize
      }
    }

    // 检查设置的存储级别是否有效，即数据块使用了内存或磁盘存储级别，且副本数量大于1
    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      // 为什么内存和存储只会使用一个？？？
      if (storageLevel.useMemory) {  // 使用内存存储级别
        // 构建新的BlockStatus对象，磁盘存储为0
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus) // 更新字典
        _remainingMem -= memSize  // 将特定的内存大小分配给该数据块了，所以需要从总的剩余内存中减去
        if (blockExists) {
          logInfo(s"Updated $blockId in memory on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(memSize)}," +
            s" original size: ${Utils.bytesToString(originalMemSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        } else {
          logInfo(s"Added $blockId in memory on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(memSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        }
      }
      if (storageLevel.useDisk) { // 使用磁盘存储级别
        // 构建新的BlockStatus对象，内存存储为0
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)  // 更新_blocks字典
        if (blockExists) {
          logInfo(s"Updated $blockId on disk on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(diskSize)}," +
            s" original size: ${Utils.bytesToString(originalDiskSize)})")
        } else {
          logInfo(s"Added $blockId on disk on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(diskSize)})")
        }
      }

      externalShuffleServiceBlockStatus.foreach { shuffleServiceBlocks =>
        if (!blockId.isBroadcast && blockStatus.diskSize > 0) {
          shuffleServiceBlocks.put(blockId, blockStatus)
        }
      }
    } else if (blockExists) {  // 存储级别无效，检查当前BlockManager是否管理该数据块
      // If isValid is not true, drop the block.
      _blocks.remove(blockId)  // 从_blocks中移除
      externalShuffleServiceBlockStatus.foreach { blockStatus =>
        blockStatus.remove(blockId)
      }
      if (originalLevel.useMemory) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} in memory" +
          s" (size: ${Utils.bytesToString(originalMemSize)}," +
          s" free: ${Utils.bytesToString(_remainingMem)})")
      }
      if (originalLevel.useDisk) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} on disk" +
          s" (size: ${Utils.bytesToString(originalDiskSize)})")
      }
    }
  }

  // 移除指定数据块
  def removeBlock(blockId: BlockId): Unit = {
    if (_blocks.containsKey(blockId)) {
      // 归还数据块占用的内存给_remainingMem
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)  // 从_blocks中移除
      externalShuffleServiceBlockStatus.foreach { blockStatus =>
        blockStatus.remove(blockId)
      }
    }
  }

  // 获取当前BlockManagerInfo对应的BlockManager管理的内存剩余的总大小
  def remainingMem: Long = _remainingMem

  // 获取当前BlockManagerinfo最后被访问的事件
  def lastSeenMs: Long = _lastSeenMs

  // 获取_block
  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear(): Unit = {
    _blocks.clear()
  }
}
