import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{Semaphore, TimeUnit}

import cn.graiph.regionfs.util.Profiler
import cn.graiph.regionfs.util.Profiler._
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallback, TransportClient}
import org.apache.spark.network.server.{NoOpRpcHandler, RpcHandler, StreamManager}
import org.apache.spark.network.util.{JavaUtils, MapConfigProvider, TransportConf}
import org.junit.{Assert, Test}

/**
  * Created by bluejoe on 2020/2/14.
  */
class SparkRpcTest {
  Profiler.enableTiming = true

  val server = {
    val handler: RpcHandler = new RpcHandler() {
      override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback) {
        callback.onSuccess(JavaUtils.stringToBytes(JavaUtils.bytesToString(message).toUpperCase))
      }

      override def receive(client: TransportClient, message: ByteBuffer) {
        println(JavaUtils.bytesToString(message));
      }

      override def getStreamManager: StreamManager = {
        new StreamManager() {
          override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
            throw new UnsupportedOperationException
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

    val conf: TransportConf = new TransportConf("test", MapConfigProvider.EMPTY)
    val context: TransportContext = new TransportContext(conf, handler)
    context.createServer("localhost", 1225, new util.ArrayList())
  }

  val clientFactory = {
    val conf: TransportConf = new TransportConf("test", MapConfigProvider.EMPTY)
    val context: TransportContext = new TransportContext(conf, new NoOpRpcHandler())
    context.createClientFactory
  }

  class MyStreamCallback extends StreamCallback {
    val out = new ByteArrayOutputStream();
    val sem: Semaphore = new Semaphore(0)
    var count = 0;

    def await(): Unit = {
      sem.tryAcquire(1, 5, TimeUnit.SECONDS)
    }

    override def onData(streamId: String, buf: ByteBuffer): Unit = {
      val tmp: Array[Byte] = new Array[Byte](buf.remaining)
      buf.get(tmp)
      out.write(tmp)

      println(s"onData: streamId=$streamId, count=$count");
      count += 1
    }

    override def onComplete(streamId: String): Unit = {
      out.close()
      sem.release()
    }

    override def onFailure(streamId: String, cause: Throwable): Unit = {
      sem.release()
      throw cause;
    }

    def getBytes() = out.toByteArray
  }

  @Test
  def test1() {
    val client = clientFactory.createClient("localhost", 1225);
    client.send(JavaUtils.stringToBytes("no reply"));
    val answer = JavaUtils.bytesToString(client.sendRpcSync(JavaUtils.stringToBytes("hello"), 1000000))
    Assert.assertEquals("HELLO", answer)

    {
      val out = new MyStreamCallback();
      timing(true) {
        client.stream("large", out);
        out.await();
        Assert.assertEquals(9999999, out.getBytes().length)
      }
    }

    {
      val out = new MyStreamCallback();
      timing(true) {
        client.stream("tiny", out);
        out.await();
      }
      Assert.assertEquals(1024, out.getBytes().length)
      println("ok!")
    }
  }
}

