package regionfs

import java.io.File

import net.neoremind.kraps.rpc.RpcAddress
import org.junit.{Assert, Test}

import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/2/8.
  */
class FileAdminTest extends FileTestBase {
  @Test
  def test1(): Unit = {
    for (i <- BLOB_LENGTH) {
      super.writeFile(new File(s"./testdata/inputs/$i"))
    }

    println(admin.stat(Duration("4s")));
    println(admin.listFiles(Duration("4s")).take(100).toList)
    Assert.assertEquals(1 -> RpcAddress("localhost", 1224), admin.greet(1, Duration("4s")));
  }
}
