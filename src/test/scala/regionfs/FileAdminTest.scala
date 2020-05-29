package regionfs

import java.io.File

import net.neoremind.kraps.rpc.RpcAddress
import org.junit.{Assert, Before, Test}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/2/8.
  */
class FileAdminTest extends FileTestBase {
  @Before
  def setup2(): Unit = {
    for (i <- BLOB_LENGTH) {
      super.writeFile(new File(s"./testdata/inputs/$i"))
    }
  }

  @Test
  def test1(): Unit = {
    println(admin.stat(Duration("4s")));
    println(admin.listFiles(Duration("4s")).take(100).toList)
    Assert.assertEquals(1 -> RpcAddress("localhost", 1224), admin.greet(1, Duration("4s")));
  }

  @Test
  def testProcessFiles(): Unit = {
    val length1 = Await.result(admin.processFiles((files) => {
      files.map(_.length).sum
    }, (x: Iterable[Long]) => x.sum), Duration("4s"))

    val length2 = admin.listFiles(Duration("4s")).map(_._2).sum
    println(length1, length2)

    val count1 = Await.result(admin.processFiles((files) => {
      files.size
    }, (x: Iterable[Int]) => x.sum), Duration("4s"))

    val count2 = countFiles()

    Assert.assertEquals(count2, count1)
  }
}
