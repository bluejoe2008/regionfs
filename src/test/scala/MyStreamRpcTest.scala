import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.regionfs.network._
import cn.regionfs.util.ByteBufferUtils._
import cn.regionfs.util.Profiler
import cn.regionfs.util.Profiler._
import io.netty.buffer.ByteBuf
import org.apache.commons.io.IOUtils
import org.junit.{Assert, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by bluejoe on 2020/2/18.
  */
case class SayHelloRequest(str: String) {

}

case class SayHelloResponse(str: String) {

}

case class LargeBufferRequest(len: Int) {

}

case class ReadFile(path: String) {

}

object MyStreamServer {
  val server = StreamingServer.create("test", new StreamingRpcHandler() {

    override def receive(request: Any, ctx: RequestContext): Unit = {
      println(Thread.currentThread());
      request match {
        case SayHelloRequest(msg) =>
          ctx.reply(SayHelloResponse(msg.toUpperCase()))
        case LargeBufferRequest(len) =>
          ctx.replyBuffer(_.writeBytes(new Array[Byte](len)))
      }
    }

    override def receiveBuffer(request: ByteBuffer, ctx: RequestContext): Unit = {
      ctx.replyBuffer(_.writeInt(request.remaining()))
    }

    override def openStream(request: Any): CloseableStream = {
      request match {
        case ReadFile(path) =>
          //60us
          val fis = timing(false) {
            new FileInputStream(new File(path))
          }

          new CloseableStream() {
            override def writeNextChunk(buf: ByteBuf): Boolean = {
              val written =
                timing(false) {
                  buf.writeBytes(fis, 1024 * 10240)
                }

              written == 1024 * 10240
            }

            override def close(): Unit = {
              fis.close()
            }
          }
      }
    }
  }, 1224)
}

class MyStreamRpcTest {
  Profiler.enableTiming = true
  val server = MyStreamServer.server
  val client = StreamingClient.create("test", "localhost", 1224)

  @Test
  def test1(): Unit = {
    client.send(SayHelloRequest("hello")).receive[SayHelloResponse]().await(Duration.Inf)

    val res1 = timing(true) {
      client.send(SayHelloRequest("hello")).receive[SayHelloResponse]((x: ByteBuffer) => x.readObject[SayHelloResponse]()).await(Duration.Inf)
    }

    Assert.assertEquals("HELLO", res1.str);

    val res12 = timing(true) {
      client.send(SayHelloRequest("hello")).receive[SayHelloResponse]().await(Duration.Inf)
    }

    Assert.assertEquals("HELLO", res12.str);

    timing(true) {
      val len = client.send(LargeBufferRequest(9999999)).receive[Int]((x: ByteBuffer) => x.remaining()).await(Duration.Inf)
      Assert.assertEquals(9999999, len);
    }
  }

  @Test
  def testPutFiles(): Unit = {
    val res = timing(true) {
      client.sendBuffer((buf: ByteBuf) => {
        val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
        buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
        fos.close()
      }).receive((buf) => {
        buf.getInt
      }).await()
    }

    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res)
  }

  @Test
  def testGetFileStream(): Unit = {

    println(Thread.currentThread());
    client.send(SayHelloRequest("hello")).receive[SayHelloResponse]().await(Duration.Inf)

    timing(true) {
      Await.result(client.ask[Long]((buf: ByteBuf) => {
        buf.writeByte(3)
        buf.writeObject(ReadFile("./testdata/inputs/9999999"))
      }, _.getLong()), Duration.Inf)
    }

    timing(true) {
      val is = client.askStream(ReadFile("./testdata/inputs/9999999"));
      var read = 0;
      while (read != -1) {
        read = is.read()
      }
    }

    /*
    timing(true) {
      val is = new BufferedInputStream(client.askStream(ReadFile("./testdata/inputs/9999999")));
      var read = 0;
      while (read != -1) {
        read = is.read()
      }
    }
    */

    import scala.concurrent.ExecutionContext.Implicits.global

    //378ms
    timing(true) {
      val futures = (1 to 5).map { _ =>
        Future {
          IOUtils.toByteArray(client.askStream(ReadFile("./testdata/inputs/9999999")))
        }
      }

      futures.foreach(Await.result(_, Duration.Inf))
    }

    //483ms
    val bs2 = timing(true) {
      for (i <- 1 to 10) {
        IOUtils.toByteArray(client.askStream(ReadFile("./testdata/inputs/9999999")))
      }
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(client.askStream(ReadFile("./testdata/inputs/9999999"))));
  }
}
