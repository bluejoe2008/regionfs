package cn.regionfs.network

import java.io.InputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

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
  *
  * Hippo Transport Library enhances spark-commons with easy stream management & handling
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
  * HippoServer enhances TransportServer with stream manager(open, streaming fetch, close)
  * HippoClient enhances TransportClient with stream request and result boxing (as Stream[T])
  *
  */
trait ReceiveContext {
  def extraInput: ByteBuf

  def reply[T](response: T, extra: ((ByteBuf) => Unit)*);
}

case class OpenStreamRequest(streamRequest: Any) {

}

case class OpenStreamResponse(streamId: Long, hasMoreChunks: Boolean) {

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

object BufferedMessageStream {
  val executor = Executors.newSingleThreadExecutor();

  def create[T](bufferSize: Int, producer: (OutputBuffer[T]) => Unit)(implicit m: Manifest[T]) =
    new BufferedMessageStream[T](executor, bufferSize, producer);
}

trait OutputBuffer[T] {
  def push(value: T);
}

class BufferedMessageStream[T](executor: ExecutorService, bufferSize: Int, producer: (OutputBuffer[T]) => Unit)(implicit m: Manifest[T]) extends ChunkedMessageStream[T] {
  val buffer = new ArrayBlockingQueue[T](bufferSize);

  val future = executor.submit(new Runnable {
    override def run(): Unit = producer(new OutputBuffer[T]() {
      def push(value: T) = buffer.put(value)
    })
  })

  override def hasNext(): Boolean = !(future.isDone && buffer.isEmpty)

  def nextChunk(): Iterable[T] = {
    val first = buffer.take()
    val list = new java.util.ArrayList[T]();
    buffer.drainTo(list)
    list.add(0, first)
    println(list);
    JavaConversions.collectionAsScalaIterable(list)
  }

  override def close(): Unit = future.cancel(true)
}

trait HippoStreamManager {
  def openStream(): PartialFunction[Any, ManagedBuffer] = {
    throw new UnsupportedOperationException();
  }

  def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    throw new UnsupportedOperationException();
  }
}

class HippoStreamManagerAdapter(var streamManager: HippoStreamManager) extends StreamManager {
  val streamIdGen = new AtomicLong(System.currentTimeMillis());
  val streams = mutable.Map[Long, ChunkedStream]();

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
    streamManager.openStream()(request);
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

  def handleOpenStreamRequest(streamRequest: Any, callback: RpcResponseCallback) {
    val streamId: Long = streamIdGen.getAndIncrement();
    val stream = streamManager.openChunkedStream()(streamRequest)
    val output = Unpooled.buffer(1024);
    output.writeObject(OpenStreamResponse(streamId, stream.hasNext()));
    if (stream.hasNext()) {
      streams(streamId) = stream
    }

    callback.onSuccess(output.nioBuffer())
  }

  def handleCloseStreamRequest(streamId: Long, callback: RpcResponseCallback): Unit = {
    streams(streamId).close
    streams -= streamId
  }
}

trait HippoRpcHandler extends HippoStreamManager {
  def receive(ctx: ReceiveContext): PartialFunction[Any, Unit];
}

object HippoServer extends Logging {
  //WEIRLD: this makes next Upooled.buffer() call run fast
  Unpooled.buffer(1)

  def create(module: String, rpcHandler: HippoRpcHandler, port: Int = -1, host: String = null): HippoServer = {
    val configProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
    val conf: TransportConf = new TransportConf(module, configProvider)

    val handler: RpcHandler = new RpcHandler() {

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
          }

          val message = input.readObject();
          message match {
            case OpenStreamRequest(streamRequest) =>
              streamManagerAdapter.handleOpenStreamRequest(streamRequest, callback)

            case CloseStreamRequest(streamId) =>
              streamManagerAdapter.handleCloseStreamRequest(streamId, callback)

            case _ => {
              rpcHandler.receive(ctx)(message)
            }
          }
        }
        catch {
          case e: Throwable => callback.onFailure(e)
        }
      }

      val streamManagerAdapter = new HippoStreamManagerAdapter(rpcHandler);

      override def getStreamManager: StreamManager = streamManagerAdapter
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

  override def getChunkedInputStream(request: Any): InputStream = {
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

  override def getChunkedStream[T](request: Any)(implicit m: Manifest[T]): Stream[T] = {
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
    val OpenStreamResponse(streamId, hasMoreChunks) =
      Await.result(ask[OpenStreamResponse](OpenStreamRequest(request)), Duration.Inf);

    if (!hasMoreChunks) {
      Stream.empty
    }
    else {
      _buildStream(streamId, 0, consumeResponse)
    }
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
    val ChunkResponse(_, _, hasMoreChunks, values) = timing(false) {
      client.fetchChunk(streamId, chunkIndex, callback)
      callback.await()
    }

    Stream.cons(values,
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