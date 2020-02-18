package cn.regionfs.network

import java.io.InputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CountDownLatch
import scala.concurrent.ExecutionContext.Implicits.global
import cn.regionfs.util.ByteBufferUtils._
import cn.regionfs.util.StreamUtils
import io.netty.buffer.{ByteBuf, Unpooled}
import net.neoremind.kraps.util.ByteBufferInputStream
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{ChunkReceivedCallback, RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.spark.network.server.{NoOpRpcHandler, RpcHandler, StreamManager, TransportServer}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}

import scala.collection.{JavaConversions, mutable}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by bluejoe on 2020/2/17.
  */
trait Message {

}

case class StartStreamResponse(streamId: Long, hasResults: Boolean) extends Message {

}

trait StreamingRpcHandler {
  def receive(request: Message): Message;

  def receive(message: ByteBuffer): ByteBuffer;
}

object StreamingServer {
  def create(module: String, host: String, port: Int, srh: StreamingRpcHandler): StreamingServer = {
    val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
    val conf: TransportConf = new TransportConf(module, configProvider)
    val handler: RpcHandler = new RpcHandler() {
      //mark=message(0)
      //1: raw buffer
      //2: rpc message
      //3: start stream request
      //4: close stream request
      override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback) {
        try {
          val res = message.get() match {
            case 1 => {
              srh.receive(message)
            }
            case 2 => {
              val ro = srh.receive(message.readObject().asInstanceOf[Message])
              Unpooled.buffer(1024).writeObject(ro).nioBuffer()
            }
            case 3 => {
              val ro = srh.receive(message.readObject().asInstanceOf[Message])
              Unpooled.buffer(1024).writeObject(ro).nioBuffer()
            }
          }

          callback.onSuccess(res)
        }
        catch {
          case e => callback.onFailure(e)
        }
      }

      val streamManager = new StreamManager() {
        override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
          new NioManagedBuffer(ByteBuffer.allocate(1024 * 1024))
        }

        override def openStream(streamId: String): ManagedBuffer = {
          throw new UnsupportedOperationException
        }
      }

      override def getStreamManager: StreamManager = streamManager
    }

    val context: TransportContext = new TransportContext(conf, handler)
    new StreamingServer(context.createServer(host, port, new util.ArrayList()))
  }
}

class StreamingServer(server: TransportServer) {

}

object StreamingClient {
  val clientFactoryMap = mutable.Map[String, TransportClientFactory]();

  def getClientFactory(module: String) = {
    clientFactoryMap.getOrElseUpdate(module, {
      val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
      val conf: TransportConf = new TransportConf(module, configProvider)
      val context: TransportContext = new TransportContext(conf, new NoOpRpcHandler())
      context.createClientFactory
    }
    )
  }

  def create(module: String, remoteHost: String, remotePort: Int): StreamingClient = {
    new StreamingClient(getClientFactory(module).createClient(remoteHost, remotePort))
  }
}

class StreamingClient(client: TransportClient) {
  def ask[T <: Message](request: Message): Future[(Long, T)] = {
    val buf = Unpooled.buffer(1024)
    buf.writeObject(request)
    _ask[T](2, buf, _.readObject().asInstanceOf[T])
  }

  def ask[T <: Message](request: ByteBuf): Future[(Long, T)] = {
    _ask[T](1, request, _.readObject().asInstanceOf[T])
  }

  class MyRpcResponseCallback[T](consumeResponse: (ByteBuffer) => T) extends RpcResponseCallback {
    val latch = new CountDownLatch(1);

    var res: Message = null
    var err: Throwable = null

    override def onFailure(e: Throwable): Unit = {
      err = e
      latch.countDown();
    }

    override def onSuccess(response: ByteBuffer): Unit = {
      res = response.readObject()
      latch.countDown();
    }

    def await(): T = {
      latch.await()
      if (err != null)
        throw err;

      res.asInstanceOf[T]
    }
  }

  class MyChunkReceivedCallback[T](responseConsume: (ByteBuffer) => T) extends ChunkReceivedCallback {
    val latch = new CountDownLatch(1);

    var res: Option[T] = null
    var err: Throwable = null

    override def onFailure(chunkIndex: Int, e: Throwable): Unit = {

    }

    override def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = {
      val buf = buffer.nioByteBuffer()

      val hasMore = buf.get() != 0
      if (hasMore) {
        res = None
      }
      else {
        res = Some(responseConsume(buf));
      }
    }

    def await(): Option[T] = {
      latch.await()
      if (err != null)
        throw err;

      res
    }
  }

  def _ask[T <: Message](mark: Byte, request: ByteBuf, consumeResponse: (ByteBuffer) => T): Future[(Long, T)] = {
    val callback = new MyRpcResponseCallback[T](consumeResponse);
    val id = client.sendRpc(Unpooled.buffer().writeByte(mark).writeBytes(request).nioBuffer, callback)

    Future {
      id -> callback.await()
    }
  }

  def askStream(request: Message): InputStream = {
    val iter: Iterator[InputStream] = askStream[InputStream](request, (buf: ByteBuffer) => new ByteBufferInputStream(buf)).iterator;

    StreamUtils.concatStreams {
      if (iter.hasNext) {
        Some(iter.next)
      }
      else {
        None
      }
    }
  }

  def askStream[T](request: Message, responseConsume: (ByteBuffer) => T): Stream[T] = {
    //send start stream request
    val buf = Unpooled.buffer(1024)
    buf.writeObject(request)

    val (_, response: StartStreamResponse) =
      Await.result(_ask[StartStreamResponse](3, buf, _.readObject[StartStreamResponse]()), Duration.Inf)

    val streamId:Long = response.streamId
    if (!response.hasResults) {
      Stream.empty
    }
    else {
      def buildStream(index: Int): Stream[T] = {
        val callback = new MyChunkReceivedCallback[T](responseConsume);
        client.fetchChunk(streamId, index, callback)
        val t = callback.await()
        if (t.isDefined)
          Stream.cons(t.get, {
            buildStream(index + 1)
          })
        else
          Stream.empty
      }

      buildStream(0);
    }
  }
}