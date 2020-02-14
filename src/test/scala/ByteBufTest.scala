import java.nio.ByteBuffer

import cn.graiph.regionfs.util.Profiler
import io.netty.buffer.Unpooled
import org.junit.Test
import cn.graiph.regionfs.util.Profiler._

/**
  * Created by bluejoe on 2020/2/14.
  */
class ByteBufTest {
  Profiler.enableTiming = true

  @Test
  def testByteBuffer(): Unit = {
    val bb = timing(true){
      ByteBuffer.allocate(20)
    }
    println(bb.array(), bb.arrayOffset(), bb.position(), bb.remaining())
    bb.putInt(100)
    println(bb.array(), bb.arrayOffset(), bb.position(), bb.remaining())
    bb.position(bb.position()+2)
    bb.putInt(200)
    println(bb.array(), bb.arrayOffset(), bb.position(), bb.remaining())
  }
  
  @Test
  def testByteBuf(): Unit = {
    val bb = timing(true){
      Unpooled.buffer(10)
    }
    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())

    bb.writeInt(100)
    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())

    bb.writeZero(20)
    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())

    bb.readInt()
    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())
  }
}
