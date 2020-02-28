package regionfs

import cn.bluejoe.regionfs.client.FsAdmin
import net.neoremind.kraps.rpc.RpcAddress
import org.junit.{After, Assert, Before, Test}

/**
  * Created by bluejoe on 2020/2/8.
  */
class FileAdminTest extends FileTestBase {
  var admin: FsAdmin = null

  @Before
  def setup2(): Unit = {
    admin = new FsAdmin("localhost:2181")
  }

  @Test
  def test1(): Unit = {
    println(admin.stat());
    println(admin.listFiles().take(100).toList)
    Assert.assertEquals(1 -> RpcAddress("localhost", 1224), admin.greet(1));
  }

  @After
  def after2(): Unit = {
    admin.close
  }
}
