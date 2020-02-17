import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CountDownLatch

import cn.graiph.regionfs.util.ByteBufferUtils._
import cn.graiph.regionfs.util.Profiler
import cn.graiph.regionfs.util.Profiler._
import io.netty.buffer.Unpooled
import org.apache.commons.io.IOUtils
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{ChunkReceivedCallback, RpcResponseCallback, StreamCallback, TransportClient}
import org.apache.spark.network.server.{NoOpRpcHandler, RpcHandler, StreamManager}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}
import org.junit.{Assert, Test}

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2020/2/14.
  */
class SparkRpcTest {

  Profiler.enableTiming = true
  val confProvider = new MapConfigProvider(JavaConversions.mapAsJavaMap(Map()))
  val server = {
    val handler: RpcHandler = new RpcHandler() {
      override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback) {
        if (false) {
          val bb = Unpooled.wrappedBuffer(message)

          val res = bb.readByte() match {
            case 1 => Unpooled.buffer().writeString(bb.readString().toUpperCase).nioBuffer()
            case 2 => Unpooled.buffer().writeInt(100).nioBuffer()
          }

          callback.onSuccess(res)
        }
        else {
          val res = message.get() match {
            case 1 => Unpooled.buffer().writeString(message.readString().toUpperCase).nioBuffer()
            case 2 => Unpooled.buffer().writeInt(100).nioBuffer()
          }

          callback.onSuccess(res)
        }
      }

      override def receive(client: TransportClient, message: ByteBuffer) {
        println(message.get(), message.readString(), message.getLong);
      }

      override def getStreamManager: StreamManager = {
        new StreamManager() {
          override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
            //println(s"received chunk request: $chunkIndex");
            new NioManagedBuffer(ByteBuffer.allocate(1024 * 1024))
          }

          override def openStream(streamId: String): ManagedBuffer = {
            streamId match {
              case "large" => new NioManagedBuffer(ByteBuffer.allocate(9999999));
              case "tiny" => new NioManagedBuffer(ByteBuffer.allocate(1024));
            }
          }
        }
      }
    }

    val conf: TransportConf = new TransportConf("test", confProvider)
    val context: TransportContext = new TransportContext(conf, handler)
    context.createServer("localhost", 1225, new util.ArrayList())
  }

  val clientFactory = {
    val conf: TransportConf = new TransportConf("test", confProvider)
    val context: TransportContext = new TransportContext(conf, new NoOpRpcHandler())
    context.createClientFactory
  }

  class MyChunkReceivedCallback extends ChunkReceivedCallback {
    val latch = new CountDownLatch(1);
    val out = new ByteArrayOutputStream();

    override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      latch.countDown()
    }

    override def onSuccess(chunkIndex: Int, buf: ManagedBuffer): Unit = {
      //println(s"received chunk response: $chunkIndex");
      IOUtils.copy(buf.createInputStream(), out);
      latch.countDown()
    }

    def getBytes() = {
      latch.await()
      out.toByteArray
    }
  }

  class MyStreamCallback extends StreamCallback {
    val out = new ByteArrayOutputStream();
    val latch = new CountDownLatch(1);
    var count = 0;

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      val tmp: Array[Byte] = new Array[Byte](buf.remaining)
      buf.get(tmp)
      out.write(tmp)

      //println(s"onData: streamId=$streamId, length=${buf.position()}, index=$count");
      count += 1
    }

    override def onComplete(streamId: String): Unit = {
      out.close()
      latch.countDown()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      latch.countDown()
      throw cause;
    }

    def getBytes() = {
      latch.await()
      out.toByteArray
    }
  }

  @Test
  def test1() {
    val client = clientFactory.createClient("localhost", 1225);
    client.send(Unpooled.buffer().writeByte(1.toByte).writeString("no reply").writeLong(999).nioBuffer());
    val cd = new CountDownLatch(1);
    client.sendRpc(
      Unpooled.buffer().writeByte(1.toByte).writeString("hello").writeLong(888).nioBuffer(), new RpcResponseCallback() {
        override def onFailure(e: Throwable): Unit = {
          cd.countDown();
        }

        override def onSuccess(response: ByteBuffer): Unit = {
          val answer = response.readString() //Unpooled.wrappedBuffer(response).readString();
          println(answer)
          Assert.assertEquals("HELLO", answer)
          cd.countDown();
        }
      })

    cd.await()

    //send large files
    val res = timing(true) {
      client.sendRpcSync(ByteBuffer.allocate(9999999).flip().asInstanceOf[ByteBuffer], 1000000)
    }

    Assert.assertEquals(100, res.getInt);

    //get chunks parallely
    timing(true) {
      val mcs = ArrayBuffer[MyChunkReceivedCallback]()
      for (i <- 1 to 9) {
        val mc = new MyChunkReceivedCallback()
        mcs += mc
        client.fetchChunk(0, i, mc);
      }

      mcs.foreach(_.getBytes())
    }

    //get chunks one by one
    timing(true) {
      for (i <- 1 to 9) {
        val mc = new MyChunkReceivedCallback()
        client.fetchChunk(0, i, mc);
        Assert.assertEquals(1024 * 1024, mc.getBytes().length);
      }
    }

    {
      val out = new MyStreamCallback();
      timing(true) {
        client.stream("large", out);
        out.getBytes();
      }

      Assert.assertEquals(9999999, out.getBytes().length)
    }

    {
      val out = new MyStreamCallback();
      timing(true) {
        client.stream("tiny", out);
        out.getBytes();
      }

      Assert.assertEquals(1024, out.getBytes().length)
    }
  }
}

