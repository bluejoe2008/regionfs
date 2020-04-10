package regionfs

import java.io.File

import org.apache.commons.io.FileUtils
import org.grapheco.regionfs.server.{FsNodeServer, NodeIdAlreadyExistException, RegionStoreLockedException}
import org.junit.{Assert, Before, Test}

/**
  * Created by bluejoe on 2020/3/24.
  */
class StartNodeServerTest {
  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/nodes"))
    new File("./testdata/nodes/node1").mkdirs()
    new File("./testdata/nodes/node2").mkdirs()
  }

  @Test
  def testNodeIdConflict(): Unit = {
    val server1 = FsNodeServer.create(Map(
      "node.id" -> "1", "zookeeper.address" -> "localhost:2181", "server.port" -> "1224", "data.storeDir" -> "./nodes/node1"),
      new File("./testdata"))

    new File("./testdata/nodes/node2").mkdirs()
    var server2: FsNodeServer = null
    try {
      server2 = FsNodeServer.create(Map(
        "node.id" -> "1", "zookeeper.address" -> "localhost:2181", "server.port" -> "1225", "data.storeDir" -> "./nodes/node2"),
        new File("./testdata"))

      Assert.assertTrue(false)
    }
    catch {
      case e: Throwable =>
        Assert.assertTrue(e.isInstanceOf[NodeIdAlreadyExistException])
    }

    server1.shutdown()
    if (server2 != null)
      server2.shutdown()
  }

  @Test
  def testNodePathConflict(): Unit = {
    val server1 = FsNodeServer.create(Map(
      "node.id" -> "1", "zookeeper.address" -> "localhost:2181", "server.port" -> "1224", "data.storeDir" -> "./nodes/node1"),
      new File("./testdata"))

    new File("./testdata/nodes/node2").mkdirs()
    var server2: FsNodeServer = null
    try {
      server2 = FsNodeServer.create(Map(
        "node.id" -> "2", "zookeeper.address" -> "localhost:2181", "server.port" -> "1225", "data.storeDir" -> "./nodes/node1"),
        new File("./testdata"))

      Assert.assertTrue(false)
    }
    catch {
      case e: Throwable =>
        Assert.assertTrue(e.isInstanceOf[RegionStoreLockedException])
    }

    server1.shutdown()
    if (server2 != null)
      server2.shutdown()
  }
}
