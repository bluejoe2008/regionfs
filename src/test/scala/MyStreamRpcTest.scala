import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.regionfs.network._
import cn.regionfs.util.Profiler
import cn.regionfs.util.Profiler._
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.commons.io.IOUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
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

case class GetLargeBuffer(len: Int) {

}

case class ReadFile(path: String) {

}

case class GetSizedStream(len: Int) {

}

object MyStreamServer {
  val server = StreamingServer.create("test", new StreamingRpcHandler() {

    override def receive(request: Any, ctx: ReceiveContext): Unit = {
      //println(Thread.currentThread());
      request match {
        case SayHelloRequest(msg) =>
          ctx.reply(SayHelloResponse(msg.toUpperCase()))
      }
    }

    override def receiveBuffer(request: ByteBuffer, ctx: ReceiveContext): Unit = {
      ctx.reply(request.remaining())
    }

    override def openStream(request: Any): ManagedBuffer = {
      request match {
        case ReadFile(path) =>
          val fis = new FileInputStream(new File(path))
          val buf = Unpooled.buffer()
          buf.writeBytes(fis.getChannel, new File(path).length().toInt)
          new NettyManagedBuffer(buf)
      }
    }
  }, 1224)
}

class MyStreamRpcTest {
  Profiler.enableTiming = true
  val server = MyStreamServer.server
  val client = StreamingClient.create("test", "localhost", 1224)

  @Test
  def testRpc(): Unit = {
    Await.result(client.ask[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    val res1 = timing(true) {
      Await.result(client.ask[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)
    }

    Assert.assertEquals("HELLO", res1.str);
  }

  @Test
  def testPutFiles(): Unit = {
    val res = timing(true, 10) {
      Await.result(client.send[Int]((buf: ByteBuf) => {
        val fos = new FileInputStream(new File("./testdata/inputs/9999999"));
        buf.writeBytes(fos.getChannel, new File("./testdata/inputs/9999999").length().toInt)
        fos.close()
      }), Duration.Inf)
    }

    Assert.assertEquals(new File("./testdata/inputs/9999999").length(), res)
  }

  @Test
  def testGetStream(): Unit = {

    Await.result(client.ask[SayHelloResponse](SayHelloRequest("hello")), Duration.Inf)

    timing(true, 10) {
      val is = client.getInputStream(ReadFile("./testdata/inputs/9999999"));
      var read = 0;
      while (read != -1) {
        read = is.read()
      }
    }

    for (size <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"test fetching stream: size=$size")
      timing(true, 10) {
        IOUtils.toByteArray(client.getInputStream(ReadFile(s"./testdata/inputs/$size")))
      }
    }

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/999"))),
      IOUtils.toByteArray(client.getInputStream(ReadFile("./testdata/inputs/999")))
    );

    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      IOUtils.toByteArray(client.getInputStream(ReadFile("./testdata/inputs/9999999")))
    );

    import scala.concurrent.ExecutionContext.Implicits.global

    //378ms
    timing(true) {
      val futures = (1 to 5).map { _ =>
        Future {
          IOUtils.toByteArray(client.getInputStream(ReadFile("./testdata/inputs/9999999")))
        }
      }

      futures.foreach(Await.result(_, Duration.Inf))
    }
  }
}
