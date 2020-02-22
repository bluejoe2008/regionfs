import java.io.{File, FileInputStream}

import cn.regionfs.network._
import cn.regionfs.util.Profiler._
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}

/**
  * Created by bluejoe on 2020/2/22.
  */
case class SayHelloRequest(str: String) {

}

case class SayHelloResponse(str: String) {

}

case class ReadFileRequest(path: String) {

}

case class PutFileRequest(totalLength: Int) {

}

case class PutFileResponse(written: Int) {

}

case class GetManyResultsRequest(times: Int, chunkSize: Int, msg: String) {

}

case class GetBufferedResultsRequest(total: Int) {

}

object HippoRpcServerForTest {
  val server = HippoServer.create("test", new StreamingRpcHandler() {

    override def receive(ctx: ReceiveContext): PartialFunction[Any, Unit] = {
      case SayHelloRequest(msg) =>
        ctx.reply(SayHelloResponse(msg.toUpperCase()))

      case PutFileRequest(totalLength) =>
        ctx.reply(PutFileResponse(ctx.extraInput.readableBytes()))
    }

    override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
      case GetManyResultsRequest(times, chunkSize, msg) =>
        new ChunkedMessageStream[String]() {
          var count = 0;

          override def hasNext(): Boolean = count < times

          override def nextChunk(): Iterable[String] = {
            count += 1
            (1 to chunkSize).map(_ => msg)
          }

          override def close(): Unit = {}
        }

      case GetBufferedResultsRequest(total) =>
        BufferedMessageStream.create[String](10, (queue) => {
          for (i <- 1 to total) {
            queue.push(s"hello-$i");
            Thread.sleep(1);
          }
        })

      case ReadFileRequest(path) =>
        new ChunkedStream() {
          val fis = new FileInputStream(new File(path))
          val length = new File(path).length()
          var count = 0;

          override def hasNext(): Boolean = {
            count < length
          }

          def nextChunk(buf: ByteBuf): Unit = {
            val written =
              timing(false) {
                buf.writeBytes(fis, 1024 * 1024 * 10)
              }

            count += written
          }

          override def close(): Unit = {
            fis.close()
          }
        }
    }

    override def openStream(): PartialFunction[Any, ManagedBuffer] = {
      case ReadFileRequest(path) =>
        val fis = new FileInputStream(new File(path))
        val buf = Unpooled.buffer()
        buf.writeBytes(fis.getChannel, new File(path).length().toInt)
        new NettyManagedBuffer(buf)
    }
  }, 1224)
}