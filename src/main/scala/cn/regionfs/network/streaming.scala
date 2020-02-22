package cn.regionfs.network

import java.io.InputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch}

import cn.regionfs.util.ByteBufferUtils._
import cn.regionfs.util.Profiler._
import cn.regionfs.util.{Logging, StreamUtils}
import io.netty.buffer.{ByteBuf, ByteBufInputStream, Unpooled}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.network.client._
import org.apache.spark.network.server.{NoOpRpcHandler, RpcHandler, StreamManager, TransportServer}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}
import org.apache.zookeeper.server.ByteBufferInputStream

import scala.collection.{JavaConversions, mutable}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by bluejoe on 2020/2/17.
  */
trait ReceiveContext {
  def extraInput: ByteBuf

  def reply[T](response: T, extra: ((ByteBuf) => Unit)*);
}

case class OpenStreamRequest(streamRequest: Any) {

}

case class CloseStreamRequest(streamId: Long) {

}

trait ChunkedStream {
  def hasNext(): Boolean;

  def nextChunk(buf: ByteBuf): Unit;

  def close(): Unit
}

trait ChunkedMessageStream[T] extends ChunkedStream {
  override def hasNext(): Boolean;

  def nextChunk(): Iterable[T];

  override def nextChunk(buf: ByteBuf) = {
    buf.writeObject(nextChunk())
  }

  override def close(): Unit
}

trait StreamingRpcHandler {
  def receive(ctx: ReceiveContext): PartialFunction[Any, Unit];

  def openStream(): PartialFunction[Any, ManagedBuffer] = {
    throw new UnsupportedOperationException();
  }

  def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    throw new UnsupportedOperationException();
  }
}

object HippoServer extends Logging {
  //WEIRLD: this makes next Upooled.buffer() call run fast
  Unpooled.buffer(1)

  def create(module: String, srh: StreamingRpcHandler, port: Int = -1, host: String = null): HippoServer = {
    val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
    val conf: TransportConf = new TransportConf(module, configProvider)
    val streamIdGen = new AtomicLong(System.currentTimeMillis());
    val streams = mutable.Map[Long, ChunkedStream]();

    val handler: RpcHandler = new RpcHandler() {
      //mark=message(0)
      //1: raw buffer
      //2: rpc message
      //3: open stream request
      //4: close stream request
      override def receive(client: TransportClient, input: ByteBuffer, callback: RpcResponseCallback) {
        try {
          val ctx = new ReceiveContext {
            override def reply[T](response: T, extra: ((ByteBuf) => Unit)*) = {
              replyBuffer((buf: ByteBuf) => {
                buf.writeObject(response)
                extra.foreach(_.apply(buf))
              })
            }

            def replyBuffer(writeResponse: ((ByteBuf) => Unit)) = {
              val output = Unpooled.buffer(1024);
              writeResponse.apply(output)
              callback.onSuccess(output.nioBuffer())
            }

            def extraInput: ByteBuf = Unpooled.wrappedBuffer(input)
          };

          val message = input.readObject();
          message match {
            case OpenStreamRequest(streamRequest) => {
              val streamId: Long = streamIdGen.getAndIncrement();
              val stream = srh.openChunkedStream()(streamRequest)

              ctx.replyBuffer((buf: ByteBuf) => {
                _writeNextChunk(buf, streamId, 0, stream)
              })

              if (stream.hasNext()) {
                streams(streamId) = stream
              }
            }

            case CloseStreamRequest(streamId) => {
              streams(streamId).close
              streams -= streamId
            }

            case _ => {
              srh.receive(ctx)(message)
            }
          }
        }
        catch {
          case e: Throwable => callback.onFailure(e)
        }
      }

      private def _writeNextChunk(buf: ByteBuf, streamId: Long, chunkIndex: Int, stream: ChunkedStream) {
        buf.writeLong(streamId).writeInt(0).writeByte(1)

        if (stream.hasNext()) {
          stream.nextChunk(buf)
        }

        if (!stream.hasNext()) {
          buf.setByte(8 + 4, 0)
          stream.close()
          streams.remove(streamId)
        }
      }

      val streamManager = new StreamManager() {
        override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
          if (logger.isTraceEnabled)
            logger.trace(s"get chunk: streamId=$streamId, chunkIndex=$chunkIndex")

          //1-2ms
          timing(false) {
            val buf = Unpooled.buffer(1024)
            val stream = streams(streamId)
            _writeNextChunk(buf, streamId, chunkIndex, stream)

            new NettyManagedBuffer(buf)
          }
        }

        override def openStream(streamId: String): ManagedBuffer = {
          val request = StreamUtils.deserializeObject(StreamUtils.base64.decode(streamId))
          srh.openStream()(request);
        }
      }

      override def getStreamManager: StreamManager = streamManager
    }

    val context: TransportContext = new TransportContext(conf, handler)
    new HippoServer(context.createServer(host, port, new util.ArrayList()))
  }
}

class HippoServer(server: TransportServer) {
  def close() = server.close()
}

object HippoClient extends Logging {
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

  def create(module: String, remoteHost: String, remotePort: Int): HippoClient = {
    new HippoClient(getClientFactory(module).createClient(remoteHost, remotePort))
  }
}

trait HippoStreamingClient {
  def getChunkedStream[T](request: Any)(implicit m: Manifest[T]): Stream[T]

  def getInputStream(request: Any): InputStream

  def getChunkedInputStream(request: Any): InputStream
}

trait HippoRpcClient {
  def ask[T](message: Any, extra: ((ByteBuf) => Unit)*)(implicit m: Manifest[T]): Future[T]
}

class HippoClient(client: TransportClient) extends HippoStreamingClient with HippoRpcClient with Logging {
  def close() = client.close()

  def ask[T](message: Any, extra: ((ByteBuf) => Unit)*)(implicit m: Manifest[T]): Future[T] = {
    _sendAndReceive({ buf =>
      buf.writeObject(message)
      extra.foreach(_.apply(buf))
    }, _.readObject().asInstanceOf[T])
  }

  def getInputStream(request: Any): InputStream = {
    _getInputStream(StreamUtils.base64.encodeAsString(
      StreamUtils.serializeObject(request)))
  }

  def getChunkedInputStream(request: Any): InputStream = {
    //12ms
    val iter: Iterator[InputStream] = timing(false) {
      _getChunkedStream[InputStream](request, (buf: ByteBuffer) =>
        new ByteBufferInputStream(buf)).iterator
    }

    //1ms
    timing(false) {
      StreamUtils.concatChunks {
        if (iter.hasNext) {
          Some(iter.next)
        }
        else {
          None
        }
      }
    }
  }

  def getChunkedStream[T](request: Any)(implicit m: Manifest[T]): Stream[T] = {
    val stream = _getChunkedStream(request, _.readObject().asInstanceOf[Iterable[T]])
    stream.flatMap(_.toIterable)
  }

  private case class ChunkResponse[T](streamId: Long, chunkIndex: Int, hasNext: Boolean, chunk: T) {

  }

  private class MyChunkReceivedCallback[T](consumeResponse: (ByteBuffer) => T) extends ChunkReceivedCallback {
    val latch = new CountDownLatch(1);

    var res: ChunkResponse[T] = _
    var err: Throwable = null

    override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      err = e;
      latch.countDown();
    }

    override def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = {
      try {
        val buf = buffer.nioByteBuffer()
        res = _readChunk(buf, consumeResponse)
      }
      catch {
        case e: Throwable =>
          err = e;
      }

      latch.countDown();
    }

    def await(): ChunkResponse[T] = {
      latch.await()
      if (err != null)
        throw err;

      res
    }
  }

  private class MyRpcResponseCallback[T](consumeResponse: (ByteBuffer) => T) extends RpcResponseCallback {
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

  private def _getChunkedStream[T](request: Any, consumeResponse: (ByteBuffer) => T)(implicit m: Manifest[T]): Stream[T] = {
    //send start stream request
    //2ms
    val (ChunkResponse(streamId, _, hasMoreChunks, value)) =
      Await.result(_sendAndReceive[ChunkResponse[T]](
        (buf: ByteBuf) => {
          buf.writeObject(OpenStreamRequest(request))
        }, (buf: ByteBuffer) => {
          _readChunk[T](buf, consumeResponse)
        }), Duration.Inf)

    Stream.cons(value,
      if (hasMoreChunks) {
        _buildStream(streamId, 1, consumeResponse)
      }
      else {
        Stream.empty
      })
  }

  private def _readChunk[T](buf: ByteBuffer, consumeResponse: (ByteBuffer) => T): ChunkResponse[T] = {
    ChunkResponse[T](buf.getLong(),
      buf.getInt(),
      buf.get() != 0,
      consumeResponse(buf))
  }

  private def _buildStream[T](streamId: Long, chunkIndex: Int, consumeResponse: (ByteBuffer) => T): Stream[T] = {
    if (logger.isTraceEnabled)
      logger.trace(s"build stream: streamId=$streamId, chunkIndex=$chunkIndex")

    val callback = new MyChunkReceivedCallback[T](consumeResponse);
    val ChunkResponse(_, _, hasMoreChunks, t) = timing(false) {
      client.fetchChunk(streamId, chunkIndex, callback)
      callback.await()
    }

    Stream.cons(t,
      if (hasMoreChunks) {
        _buildStream(streamId, chunkIndex + 1, consumeResponse)
      }
      else {
        Stream.empty
      })
  }

  private def _getInputStream(streamId: String): InputStream = {
    val queue = new ArrayBlockingQueue[AnyRef](1);
    val END_OF_STREAM = new Object

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

  private def _sendAndReceive[T](produceRequest: (ByteBuf) => Unit, consumeResponse: (ByteBuffer) => T)(implicit m: Manifest[T]): Future[T] = {
    val buf = Unpooled.buffer(1024)
    produceRequest(buf)
    val callback = new MyRpcResponseCallback[T](consumeResponse);
    client.sendRpc(buf.nioBuffer, callback)
    implicit val ec: ExecutionContext = HippoClient.executionContext
    Future {
      callback.await()
    }
  }
}