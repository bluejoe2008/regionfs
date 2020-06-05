package regionfs

import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.apache.zookeeper.server.ByteBufferInputStream
import org.grapheco.regionfs.server.Region
import org.junit.{Assert, Test}

import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/3/8.
  */
class FileIOWith3Node3ReplicaStrongConsistencyTest extends FileIOWith1Node1ReplicaTest {
  override val con = new StrongMultiNode

  private def assertRegion(nodeId: Int, regionId: Long)(op: (Region) => Unit) = {
    val region = servers(nodeId - 1).localRegionManager.regions(regionId)
    op(region)
  }

  @Test
  def testDistributed(): Unit = {
    //now we have started 3 nodes
    Assert.assertEquals(3, admin.getAvaliableNodes().size)

    val fid1 = super.writeFile(new File(s"./testdata/inputs/999"))
    //write on 1st node
    Assert.assertEquals(1, (fid1.regionId >> 16).toInt)
    //since replica=3, we should have 3 regions now
    Assert.assertEquals(3, admin.askRegionOwnerNodes(fid1.regionId, Duration("2s")).size)
    //now we have 1 region on each node
    Assert.assertEquals(1, admin.askRegionsOnNode(1, Duration("2s")).size)
    Assert.assertEquals(1, admin.askRegionsOnNode(2, Duration("2s")).size)
    Assert.assertEquals(1, admin.askRegionsOnNode(3, Duration("2s")).size)
    //call localRegionManager.regions to check
    Assert.assertEquals(1, servers(0).localRegionManager.regions.size)
    Assert.assertEquals(1, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(1, servers(2).localRegionManager.regions.size)

    for (nodeId <- 1 to 3) {
      assertRegion(nodeId, fid1.regionId) {
        (region) =>
          //first region on node1
          Assert.assertEquals((1 << 16) + 1, region.regionId);
          Assert.assertEquals(1, region.fileCount);
          Assert.assertEquals(1, region.revision);
          Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999"))),
            IOUtils.toByteArray(new ByteBufferInputStream(region.read(fid1.localId).get.buffer)))
      }
    }

    val fid2 = super.writeFile(new File(s"./testdata/inputs/9999"))
    Assert.assertEquals(2, (fid2.regionId >> 16).toInt)
    Assert.assertEquals(3, admin.askRegionOwnerNodes(fid2.regionId, Duration("2s")).size)
    //now we have 2 region on each node
    Assert.assertEquals(2, admin.askRegionsOnNode(2, Duration("2s")).size)
    Assert.assertEquals(2, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(2, servers(0).localRegionManager.regions.size)

    for (nodeId <- 1 to 3) {
      assertRegion(nodeId, fid2.regionId) {
        (region) =>
          //first region on node2
          Assert.assertEquals((2 << 16) + 1, region.regionId);
          Assert.assertEquals(1, region.fileCount);
          Assert.assertEquals(1, region.revision);
          Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/9999"))),
            IOUtils.toByteArray(new ByteBufferInputStream(region.read(fid2.localId).get.buffer)))
      }
    }

    val fid3 = super.writeFile(new File(s"./testdata/inputs/99999"))
    //write on node3
    Assert.assertEquals(3, (fid3.regionId >> 16).toInt)
    Assert.assertEquals(3, admin.askRegionOwnerNodes(fid3.regionId, Duration("2s")).size)

    Assert.assertEquals(3, servers(0).localRegionManager.regions.size)
    Assert.assertEquals(3, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(3, servers(2).localRegionManager.regions.size)

    val fid4 = super.writeFile(new File(s"./testdata/inputs/999"))
    //regionId=65538, nodeId=1
    Assert.assertEquals(1, (fid4.regionId >> 16).toInt)
    Assert.assertEquals(3, admin.askRegionOwnerNodes(fid4.regionId, Duration("2s")).size)

    Assert.assertEquals(4, servers(0).localRegionManager.regions.size)
    Assert.assertEquals(4, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(4, servers(2).localRegionManager.regions.size)

    for (nodeId <- 1 to 3) {
      assertRegion(nodeId, fid4.regionId) {
        (region) =>
          Assert.assertEquals(65538, region.regionId);
          Assert.assertEquals(1, region.fileCount);
          Assert.assertEquals(1, region.revision);
      }
    }

    Assert.assertEquals(4, servers(0).localRegionManager.regions.size)

    //write large files
    //node2
    super.writeFile(new File(s"./testdata/inputs/9999999"))
    //node3
    super.writeFile(new File(s"./testdata/inputs/9999999"))
    //node1
    super.writeFile(new File(s"./testdata/inputs/9999999"))
    //node2
    super.writeFile(new File(s"./testdata/inputs/9999999"))
    //node3
    super.writeFile(new File(s"./testdata/inputs/9999999"))

    //3*3=9
    Assert.assertEquals(9, servers(0).localRegionManager.regions.size)
    Assert.assertEquals(9, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(9, servers(2).localRegionManager.regions.size)
  }
}
