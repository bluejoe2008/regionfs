package regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.junit.{Assert, Test}

import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/2/8.
  */
class FileAdminTest extends FileTestBase {
  @Test
  def test1(): Unit = {
    println(admin.stat(Duration("4s")));
    println(admin.listFiles(Duration("4s")).take(100).toList)
    Assert.assertEquals(1 -> RpcAddress("localhost", 1224), admin.greet(1, Duration("4s")));
  }
}
