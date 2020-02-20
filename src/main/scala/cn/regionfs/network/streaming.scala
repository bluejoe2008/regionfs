package cn.regionfs.network

import java.io.InputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch}

import cn.regionfs.util.ByteBufferUtils._
import cn.regionfs.util.{Logging, StreamUtils}
import io.netty.buffer.{ByteBuf, ByteBufInputStream, Unpooled}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client._
import org.apache.spark.network.server.{NoOpRpcHandler, RpcHandler, StreamManager, TransportServer}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}

import scala.collection.{JavaConversions, mutable}
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by bluejoe on 2020/2/17.
  */
trait StreamingConstants {
  val MARK_REQUEST_RAW_BUFFER: Byte = 1
  val MARK_REQUEST_MESSAGE: Byte = 2
  val MARK_REQUEST_OPEN_STREAM: Byte = 3
  val MARK_REQUEST_CLOSE_STREAM: Byte = 4

  val END_OF_STREAM = new Object()
}

trait ReceiveContext {
  def reply[T](response: T);
}

trait StreamingRpcHandler {
  def receive(request: Any, ctx: ReceiveContext): Unit;

  def receiveBuffer(request: ByteBuffer, ctx: ReceiveContext): Unit = {
    throw new UnsupportedOperationException();
  }

  def openStream(request: Any): ManagedBuffer = {
    throw new UnsupportedOperationException();
  }
}

object StreamingServer extends Logging with StreamingConstants {
  //WEIRLD: this makes next Upooled.buffer() call run fast
  Unpooled.buffer(1)

  def create(module: String, srh: StreamingRpcHandler, port: Int = -1, host: String = null): StreamingServer = {
    val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
    val conf: TransportConf = new TransportConf(module, configProvider)

    val handler: RpcHandler = new RpcHandler() {
      //mark=message(0)
      //1: raw buffer
      //2: rpc message
      //3: open stream request
      //4: close stream request
      override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback) {
        try {
          val ctx = new ReceiveContext {
            override def reply[T](response: T) = {
              replyBuffer { x: ByteBuf =>
                x.writeObject(response)
              }
            }

            def replyBuffer(response: (ByteBuf) => Unit) = {
              val buf = Unpooled.buffer(1024);
              response(buf);
              callback.onSuccess(buf.nioBuffer())
            }
          };

          message.get() match {
            case MARK_REQUEST_RAW_BUFFER => {
              srh.receiveBuffer(message, ctx)
            }
            case MARK_REQUEST_MESSAGE => {
              srh.receive(message.readObject(), ctx)
            }
          }
        }
        catch {
          case e => callback.onFailure(e)
        }
      }

      val streamManager = new StreamManager() {
        override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
          throw new UnsupportedOperationException();
        }

        override def openStream(streamId: String): ManagedBuffer = {
          val request = StreamUtils.deserializeObject(StreamUtils.base64.decode(streamId))
          srh.openStream(request);
        }
      }

      override def getStreamManager: StreamManager = streamManager
    }

    val context: TransportContext = new TransportContext(conf, handler)
    new StreamingServer(context.createServer(host, port, new util.ArrayList()))
  }
}

class StreamingServer(server: TransportServer) {
  def close() = server.close()
}

object StreamingClient extends Logging {
  //WEIRLD: this makes next Upooled.buffer() call run fast
  Unpooled.buffer(1)

  val clientFactoryMap = mutable.Map[String, TransportClientFactory]();
  val executionContext: ExecutionContext = ExecutionContext.global

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

class StreamingClient(client: TransportClient) extends Logging with StreamingConstants {
  def close() = client.close()

  class MyRpcResponseCallback[T](consumeResponse: (ByteBuffer) => T) extends RpcResponseCallback {
    val latch = new CountDownLatch(1);

    var res: Any = null
    var err: Throwable = null

    override def onFailure(e: Throwable): Unit = {
      err = e
      latch.countDown();
    }

    override def onSuccess(response: ByteBuffer): Unit = {
      try {
        res = consumeResponse(response)
      }
      catch {
        case e: Throwable => err = e
      }

      latch.countDown();
    }

    def await(): T = {
      latch.await()
      if (err != null)
        throw err;

      res.asInstanceOf[T]
    }
  }

  private def _sendAndReceive[T](produceRequest: (ByteBuf) => Unit, consumeResponse: (ByteBuffer) => T)(implicit m: Manifest[T]): Future[T] = {
    val callback = new MyRpcResponseCallback[T](consumeResponse);
    val buf = Unpooled.buffer(1024)
    produceRequest(buf)
    client.sendRpc(buf.nioBuffer, callback)
    implicit val ec: ExecutionContext = StreamingClient.executionContext
    Future {
      callback.await()
    }
  }

  def send[T](produceRequest: (ByteBuf) => Unit)(implicit m: Manifest[T]): Future[T] = {
    _sendAndReceive({ buf =>
      buf.writeByte(MARK_REQUEST_RAW_BUFFER)
      produceRequest(buf)
    }, _.readObject[T]())
  }

  def ask[T](request: Any)(implicit m: Manifest[T]): Future[T] = {
    _sendAndReceive({ buf =>
      buf.writeByte(MARK_REQUEST_MESSAGE)
      buf.writeObject(request)
    }, _.readObject[T]())
  }

  def getInputStream(request: Any): InputStream = {
    _getInputStream(StreamUtils.base64.encodeAsString(
      StreamUtils.serializeObject(request)))
  }

  private def _getInputStream(streamId: String): InputStream = {
    val queue = new ArrayBlockingQueue[AnyRef](1);

    client.stream(streamId, new StreamCallback {
      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        queue.put(Unpooled.copiedBuffer(buf));
      }

      override def onComplete(streamId: String): Unit = {
        queue.put(END_OF_STREAM)
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        throw cause;
      }
    })

    StreamUtils.concatChunks {
      val buffer = queue.take()
      if (buffer == END_OF_STREAM)
        None
      else {
        Some(new ByteBufInputStream(buffer.asInstanceOf[ByteBuf]))
      }
    }
  }
}