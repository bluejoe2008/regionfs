package net.neoremind.kraps.rpc.netty

import java.io.InputStream
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import cn.regionfs.network._
import io.netty.buffer.ByteBuf
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.serializer.{JavaSerializer, JavaSerializerInstance}
import net.neoremind.kraps.util.{ThreadUtils, Utils}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.RpcHandler
import org.slf4j.LoggerFactory
import cn.regionfs.util.ReflectUtils._
import scala.concurrent.Future
import scala.util.control.NonFatal

/**
  * Created by bluejoe on 2020/2/21.
  *
  * HippoRpcEnv enhances NettyRpcEnv with stream handling functions, besides RPC messaging
  *
  *                    ,.I ....
  *                  ... ZO.. .. M  .
  *                  ...=.       .,,.
  *                 .,D           ..?...O.
  *        ..=MD~,.. .,           .O  . O
  *     ..,            +I.        . .,N,  ,$N,,...
  *     O.                   ..    .~.+.      . N, .
  *    7.,, .                8. ..   ...         ,O.
  *    I.DMM,.                .M     .O           ,D
  *    ...MZ .                 ~.   ....          ..N..    :
  *    ?                     .I.    ,..             ..     ,
  *    +.       ,MM=       ..Z.   .,.               .MDMN~$
  *    .I.      .MMD     ..M . .. =..                :. . ..
  *    .,M      ....   .Z. .   +=. .                 ..
  *       ~M~  ... 7D...   .=~.      . .              .
  *        ..$Z... ...+MO..          .M               .
  *                     .M. ,.       .I   .?.        ..
  *                     .~ .. Z=I7.. .7.  .ZM~+N..   ..
  *                     .O   D   . , .M ...M   . .  .: .
  *                     . NNN.I....O.... .. M:. .M,=8..
  *                      ....,...,.  ..   ...   ..
  *
  *
  * usage of HippoRpcEnv is like that of NettyRpcEnv:
  *
  *   rpcEnv = HippoRpcEnvFactory.create(config)
  *   val endpoint: RpcEndpoint = new FileRpcEndpoint(rpcEnv)
  *   rpcEnv.setupEndpoint("endpoint-name", endpoint)
  *   rpcEnv.setStreamManger(new FileStreamManager())
  *   ...
  *   ...
  *   val endPointRef = rpcEnv.setupEndpointRef(RpcAddress(...), "...");
  */
object HippoRpcEnvFactory extends RpcEnvFactory {
  override def create(config: RpcEnvConfig): HippoRpcEnv = {
    val conf = config.conf

    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
      new JavaSerializer(conf).newInstance().asInstanceOf[JavaSerializerInstance]

    val nettyEnv = new HippoRpcEnv(conf, javaSerializerInstance, config.bindAddress)

    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, conf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }

    nettyEnv
  }
}

class HippoEndpointRef(ref: NettyRpcEndpointRef, rpcEnv: HippoRpcEnv, conf: RpcConf) extends RpcEndpointRef(conf) {
  override def address: RpcAddress = ref.address

  val streamingClient = new HippoClient(rpcEnv.createClient(ref.address))

  override def ask[T](message: Any, timeout: RpcTimeout)(implicit evidence$1: ClassManifest[T]): Future[T] =
    ref.ask(message, timeout)(evidence$1)

  override def name: String = ref.name

  override def send(message: Any): Unit = ref.send()

  def askWithStream[T](message: Any, extra: ((ByteBuf) => Unit)*)(implicit m: Manifest[T]): Future[T] =
    streamingClient.ask(message, extra: _*)

  def getChunkedStream[T](request: Any)(implicit m: Manifest[T]): Stream[T] = streamingClient.getChunkedStream(request)

  def getInputStream(request: Any): InputStream = streamingClient.getInputStream(request)

  def getChunkedInputStream(request: Any): InputStream = streamingClient.getChunkedInputStream(request)
}

class HippoRpcEnv(conf: RpcConf, javaSerializerInstance: JavaSerializerInstance, host: String)
  extends NettyRpcEnv(conf, javaSerializerInstance, host) {

  val hippoRpcHandler = new HippoRpcHandler(this._get("dispatcher").asInstanceOf[Dispatcher], this)
  this._set("transportContext", new TransportContext(transportConf, hippoRpcHandler))

  def setStreamManger(streamManager: HippoStreamManager): Unit = {
    hippoRpcHandler.streamManagerAdapter.streamManager = streamManager;
  }

  override def asyncSetupEndpointRefByURI(uri: String): Future[HippoEndpointRef] = {
    super.asyncSetupEndpointRefByURI(uri).map(x =>
      new HippoEndpointRef(
        x.asInstanceOf[NettyRpcEndpointRef], this, conf))(ThreadUtils.sameThread)
  }

  override def setupEndpointRefByURI(uri: String): HippoEndpointRef = {
    super.setupEndpointRefByURI(uri).asInstanceOf[HippoEndpointRef]
  }

  override def setupEndpointRef(address: RpcAddress, endpointName: String): HippoEndpointRef = {
    super.setupEndpointRef(address, endpointName).asInstanceOf[HippoEndpointRef]
  }
}

class NullHippoStreamManager extends HippoStreamManager {

}

class HippoRpcHandler(dispatcher: Dispatcher, nettyEnv: NettyRpcEnv)
  extends RpcHandler {
  private val log = LoggerFactory.getLogger(classOf[NettyRpcHandler])

  // A variable to track the remote RpcEnv addresses of all clients
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  val streamManagerAdapter = new HippoStreamManagerAdapter(new NullHippoStreamManager());

  override def getStreamManager() = streamManagerAdapter;

  override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    messageToDispatch match {
      case requestMessage: RequestMessage =>
        dispatcher.postRemoteMessage(requestMessage, callback)

      case OpenStreamRequest(streamRequest) =>
        streamManagerAdapter.handleOpenStreamRequest(streamRequest, callback)

      case CloseStreamRequest(streamId) =>
        streamManagerAdapter.handleCloseStreamRequest(streamId, callback)
    }
  }

  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    messageToDispatch match {
      case requestMessage: RequestMessage =>
        dispatcher.postOneWayMessage(requestMessage)
    }
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): Any = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val mo = nettyEnv.deserialize[Any](client, message)
    mo match {
      case requestMessage: RequestMessage =>
        if (requestMessage.senderAddress == null) {
          // Create a new message with the socket address of the client as the sender.
          RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
        } else {
          // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
          // the listening address
          val remoteEnvAddress = requestMessage.senderAddress
          if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
            dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
          }
          requestMessage
        }

      case _ => mo
    }
  }

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      log.error("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}