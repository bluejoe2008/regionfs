package cn.regionfs.network

import java.io.InputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import cn.regionfs.util.ByteBufferUtils._
import cn.regionfs.util.Profiler._
import cn.regionfs.util.{Logging, StreamUtils}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{ChunkReceivedCallback, RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.spark.network.server.{NoOpRpcHandler, RpcHandler, StreamManager, TransportServer}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}
import org.apache.zookeeper.server.ByteBufferInputStream

import scala.collection.{JavaConversions, mutable}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by bluejoe on 2020/2/17.
  */
trait RequestContext {
  def reply[T](response: T);

  def replyBuffer(response: (ByteBuf) => Unit);
}

trait CloseableStream {
  //return true if has more data
  def writeNextChunk(buf: ByteBuf): Boolean

  def close()
}

trait StreamingRpcHandler {
  def receive(request: Any, ctx: RequestContext): Unit;

  def receiveBuffer(request: ByteBuffer, ctx: RequestContext): Unit = {
    throw new UnsupportedOperationException();
  }

  def openStream(request: Any): CloseableStream = {
    throw new UnsupportedOperationException();
  }
}

object StreamingServer extends Logging {
  def create(module: String, srh: StreamingRpcHandler, port: Int = -1, host: String = null): StreamingServer = {
    val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
    val conf: TransportConf = new TransportConf(module, configProvider)
    val streamIdGen = new AtomicLong(System.currentTimeMillis());
    val streams = mutable.Map[Long, CloseableStream]();

    val handler: RpcHandler = new RpcHandler() {
      //mark=message(0)
      //1: raw buffer
      //2: rpc message
      //3: open stream request
      //4: close stream request
      override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback) {
        try {
          val ctx = new RequestContext {
            def reply[T](response: T) = {
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
            case 1 => {
              srh.receiveBuffer(message, ctx)
            }
            case 2 => {
              srh.receive(message.readObject(), ctx)
            }
            case 3 => {
              val streamId: Long = streamIdGen.getAndIncrement();
              streams(streamId) = srh.openStream(message.readObject())
              ctx.replyBuffer((buf: ByteBuf) => buf.writeLong(streamId))
            }
            case 4 => {
              val streamId = message.getLong();
              streams(streamId).close
              streams -= streamId
            }
          }
        }
        catch {
          case e => callback.onFailure(e)
        }
      }

      val streamManager = new StreamManager() {
        override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
          if (logger.isTraceEnabled)
            logger.trace(s"get chunk: streamId=$streamId, chunkIndex=$chunkIndex")

          val buf = Unpooled.buffer(1024)
          buf.writeByte(1.toByte)
          val stream = streams(streamId)
          val hasMore = stream.writeNextChunk(buf)
          if (!hasMore) {
            buf.setByte(0, 0)
            stream.close()
          }

          new NioManagedBuffer(buf.nioBuffer())
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
  def close() = server.close()
}

object StreamingClient extends Logging {
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

class StreamingClient(client: TransportClient) extends Logging {

  def close() = client.close()

  trait SendState {
    def receive[T](consumeResponse: (ByteBuffer) => T): ReceiveState[T];

    def receive[T](): ReceiveState[T];
  }

  trait ReceiveState[T] {
    def await(timeout: Duration = Duration.Inf): T;

    def submit(): Future[T];
  }

  class MySendState(val produceRequest: (ByteBuf) => Unit) extends SendState {
    def receive[T](consumeResponse: (ByteBuffer) => T): ReceiveState[T] = {
      new MyReceiveState(produceRequest, consumeResponse)
    }

    def receive[T](): ReceiveState[T] = {
      new MyReceiveState(produceRequest, _.readObject().asInstanceOf[T])
    }
  }

  val streamingClient = this;

  class MyReceiveState[T](val produceRequest: (ByteBuf) => Unit, val consumeResponse: (ByteBuffer) => T)
    extends ReceiveState[T] {
    def await(timeout: Duration): T = {
      Await.result(submit(), timeout)
    }

    def submit(): Future[T] = {
      streamingClient.ask[T](produceRequest: (ByteBuf) => Unit, consumeResponse: (ByteBuffer) => T)
    }
  }

  def sendBuffer(produceRequest: (ByteBuf) => Unit): SendState = new MySendState((buf) => {
    buf.writeByte(1)
    produceRequest(buf)
  })

  def send(request: Any): SendState = new MySendState((buf) => {
    buf.writeByte(2)
    buf.writeObject(request)
  })

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

  class MyChunkReceivedCallback[T](consumeResponse: (ByteBuffer) => T) extends ChunkReceivedCallback {
    val latch = new CountDownLatch(1);

    var res: Any = _
    var hasMoreChunks = false;
    var err: Throwable = null

    override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      err = e;
      latch.countDown();
    }

    override def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = {
      try {
        val buf = buffer.nioByteBuffer()
        hasMoreChunks = buf.get() != 0
        res = consumeResponse(buf);
      }
      catch {
        case e =>
          err = e;
      }

      latch.countDown();
    }

    def await(): (T, Boolean) = {
      latch.await()
      if (err != null)
        throw err;

      res.asInstanceOf[T] -> hasMoreChunks
    }
  }

  def ask[T](produceRequest: (ByteBuf) => Unit, consumeResponse: (ByteBuffer) => T): Future[T] = {
    val callback = new MyRpcResponseCallback[T](consumeResponse);
    val buf = Unpooled.buffer(1024)
    produceRequest(buf)
    client.sendRpc(buf.nioBuffer, callback)
    implicit val ec: ExecutionContext = StreamingClient.executionContext
    Future {
      callback.await()
    }
  }

  def askStream(request: Any): InputStream = {
    val iter: Iterator[InputStream] = timing(true) {
      askStream[InputStream](request, (buf: ByteBuffer) =>
        new ByteBufferInputStream(buf)).iterator
    }

    timing(true) {
      StreamUtils.concatStreams {
        if (iter.hasNext) {
          Some(iter.next)
        }
        else {
          None
        }
      }
    }
  }

  def askStream[T](request: Any, responseConsume: (ByteBuffer) => T): Stream[T] = {
    //send start stream request
    val streamId: Long = timing(true) {
      Await.result(ask[Long]((buf: ByteBuf) => {
        buf.writeByte(3)
        buf.writeObject(request)
      }, _.getLong()), Duration.Inf)
    }


    def buildStream(index: Int): Stream[T] = {
      if (logger.isTraceEnabled)
        logger.trace(s"build stream: streamId=$streamId, chunkIndex=$index")

      val callback = new MyChunkReceivedCallback[T](responseConsume);
      val (t, hasMoreChunks) = timing(false) {
        client.fetchChunk(streamId, index, callback)
        callback.await()
      }
      Stream.cons(t,
        if (hasMoreChunks) {
          buildStream(index + 1)
        }
        else {
          Stream.empty
        })
    }

    buildStream(0);
  }
}