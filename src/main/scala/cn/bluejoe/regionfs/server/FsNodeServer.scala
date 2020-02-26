package cn.bluejoe.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.util.Random

import cn.bluejoe.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import cn.bluejoe.regionfs._
import cn.bluejoe.regionfs.client._
import cn.bluejoe.regionfs.util.{ConfigurationEx, ProcessUtils, ZooKeeperUtils}
import cn.bluejoe.util.{ByteBufferInputStream, Logging}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import org.apache.commons.io.IOUtils
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/22.
  */
/**
  * FsNodeServer factory
  */
object FsNodeServer {
  /**
    * create a FsNodeServer with a configuration file, e.g. node1.conf
    */
  def create(configFile: File): FsNodeServer = {
    val conf = new ConfigurationEx(configFile)
    val storeDir = conf.get("data.storeDir").asFile(configFile.getParentFile).
      getCanonicalFile.getAbsoluteFile

    if (!storeDir.exists())
      throw new StoreDirNotExistsException(storeDir)

    val lockFile = new File(storeDir, ".lock")
    if (lockFile.exists()) {
      val fis = new FileInputStream(lockFile)
      val pid = IOUtils.toString(fis).toInt
      fis.close()

      throw new StoreLockedException(storeDir, pid)
    }

    //TODO: use a leader node: manages all regions
    new FsNodeServer(
      conf.get("zookeeper.address").asString,
      conf.get("node.id").asInt,
      storeDir,
      conf.get("server.host").withDefault("localhost").asString,
      conf.get("server.port").withDefault(1224).asInt
    )
  }
}

/**
  * a FsNodeServer responds blob save/read requests
  */
class FsNodeServer(zks: String, nodeId: Int, storeDir: File, host: String, port: Int) extends Logging {
  logger.debug(s"nodeId: ${nodeId}")
  logger.debug(s"storeDir: ${storeDir.getCanonicalFile.getAbsolutePath}")

  val env = HippoRpcEnvFactory.create(
    RpcEnvServerConfig(new RpcConf(), "regionfs-server", host, port))

  val address = env.address
  val addrString = s"${address.host}_${address.port}"

  val zookeeper = ZooKeeperUtils.createZookeeperClient(zks)

  val globalConfig = GlobalConfig.load(zookeeper)
  val localRegionManager = new RegionManager(nodeId, storeDir, globalConfig)

  //get neighbour nodes
  val neighbourNodes = new NodeWatcher(zookeeper, !address.equals(_))
  //get regions in neighbour nodes
  val neighbourRegions = new RegionWatcher(zookeeper, !address.equals(_))

  val endpoint = new FileRpcEndpoint(env)
  env.setupEndpoint("regionfs-service", endpoint)
  env.setRpcHandler(endpoint)

  def startup(): Unit = {
    println(IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8"))
    println(s"starting node server on ${address}, nodeId=${nodeId}, storeDir=${storeDir.getAbsoluteFile.getCanonicalPath}")

    writeLockFile(new File(storeDir, ".lock"))
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown();
      }
    })

    env.awaitTermination()
  }

  def shutdown(): Unit = {
    neighbourNodes.stop()
    neighbourRegions.stop()

    new File(storeDir, ".lock").delete();
    env.shutdown()
    println(s"shutdown node server on ${address}, nodeId=${nodeId}")
  }

  private def writeLockFile(lockFile: File): Unit = {
    val pid = ProcessUtils.getCurrentPid();
    val fos = new FileOutputStream(lockFile);
    fos.write(pid.toString.getBytes())
    fos.close()
  }

  private def registerLocalRegions(): Unit = {
    localRegionManager.regions.keys.foreach(regionId => {
      registerLocalRegion(regionId)
    })
  }

  private def registerLocalRegion(regionId: Long) = {
    zookeeper.create(s"/regionfs/regions/${addrString}_$regionId", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
  }

  class FileRpcEndpoint(override val rpcEnv: HippoRpcEnv)
    extends RpcEndpoint
      with HippoRpcHandler
      with Logging {

    val rand = new Random();

    //NOTE: register only on started up
    override def onStart(): Unit = {
      //register this node and regions
      zookeeper.create(s"/regionfs/nodes/$addrString", "".getBytes,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

      registerLocalRegions()
    }

    private def createNewRegion(): Region = {
      val region = localRegionManager.createNew()
      val regionId = region.regionId

      //notify neighbours
      //find thinnest neighbour which has least regions
      if (globalConfig.replicaNum > 1) {
        if (neighbourRegions.map.size < globalConfig.replicaNum - 1)
          throw new InsufficientNodeServerException(globalConfig.replicaNum);

        val thinNeighbourClient = neighbourRegions.map.
          groupBy(_._1).
          map(x => x._1 -> x._2.size).
          toArray.
          sortBy(_._2).
          map(_._1).
          headOption.
          map(neighbourNodes.clientOf(_)).
          getOrElse(
            //no node found, so throw a dice
            neighbourNodes.clients.toArray.apply(rand.nextInt(neighbourNodes.size))
          )

        //hello, pls create a new region with id=regionId
        Await.result(thinNeighbourClient.endPointRef.ask[CreateRegionResponse](
          CreateRegionRequest(regionId)), Duration.Inf)
      }

      //ok, now I register this region
      registerLocalRegion(regionId)
      region
    }

    private def chooseRegionForWrite(): Region = {
      localRegionManager.synchronized {
        //counterOffset=size of region
        //TODO: sort on idle
        localRegionManager.regions.values.toArray.sortBy(_.statTotalSize).headOption.
          getOrElse({
            //no enough regions
            createNewRegion()
          })
      }
    }

    private def getNeighboursWhoHasRegion(regionId: Long): Array[RpcAddress] = {
      //a region may be stored in multiple nodes
      neighbourRegions.map.filter(_._2 == regionId).filter(_._1 != address).map(_._1).toArray
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case GetNodeStatRequest() => {
        val nodeStat = NodeStat(nodeId, address,
          localRegionManager.regions.map { kv =>
            RegionStat(kv._1, kv._2.statFileCount(), kv._2.statTotalSize())
          }.toList)

        context.reply(GetNodeStatResponse(nodeStat))
      }

      case CreateRegionRequest(regionId: Long) => {
        localRegionManager.createNewReplica(regionId)
        registerLocalRegion(regionId)
        context.reply(CreateRegionResponse(regionId))
      }
    }

    override def onStop(): Unit = {
      logger.info("stop endpoint")
    }

    override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
      case ReadFileRequest(regionId: Long, localId: Long) => {
        val region = localRegionManager.get(regionId)
        CompleteStream.fromByteBuffer(region.read(localId))
      }
    }

    override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
      case ListFileRequest() =>
        ChunkedStream.pooled[ListFileResponseDetail](1024, (pool) => {
          localRegionManager.regions.values.foreach { x =>
            val it = x.listFiles()
            it.foreach(x => pool.push(ListFileResponseDetail(x)))
          }
        })
    }

    override def receiveWithStream(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
      case SendFileRequest(maybeRegionId: Option[Long], totalLength: Long) =>
        val (regionId, localId) = {
          //primary node
          if (!maybeRegionId.isDefined) {
            val region = chooseRegionForWrite()
            val regionId = region.regionId
            val clone = extraInput.duplicate()
            val localId = region.write(extraInput)
            val neighbours = getNeighboursWhoHasRegion(regionId)
            //ask neigbours
            val futures = neighbours.map(addr =>
              neighbourNodes.clientOf(addr).writeFileReplica(
                new ByteBufferInputStream(clone.duplicate()),
                totalLength,
                regionId))

            //wait all neigbours' replies
            futures.map(future =>
              Await.result(future, Duration.Inf))

            region.regionId -> localId
          }
          else {
            val region = localRegionManager.get(maybeRegionId.get)
            val localId = region.write(extraInput)

            region.regionId -> localId
          }
        }

        context.reply(SendFileResponse(FileId.make(regionId, localId)))
    }
  }

}

class RegionFsServerException(msg: String, cause: Throwable = null) extends
  RegionFsException(msg, cause) {
}

class InsufficientNodeServerException(num: Int) extends
  RegionFsServerException(s"insufficient node server for replica: num>=$num") {

}

class StoreLockedException(storeDir: File, pid: Int) extends
  RegionFsServerException(s"store is locked by another node server: node server pid=${pid}, storeDir=${storeDir.getPath}") {

}

class StoreDirNotExistsException(storeDir: File) extends
  RegionFsServerException(s"store dir does not exist: ${storeDir.getPath}") {

}