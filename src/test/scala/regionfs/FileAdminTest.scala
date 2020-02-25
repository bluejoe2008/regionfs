package regionfs

import cn.bluejoe.regionfs.client.FsAdmin
import org.junit.Test

/**
  * Created by bluejoe on 2020/2/8.
  */
class FileAdminTest extends FileTestBase {
  val admin = new FsAdmin("localhost:2181")

  @Test
  def test1(): Unit = {
    println(admin.stat());
    println(admin.listFiles().take(100).toList)
  }
}
