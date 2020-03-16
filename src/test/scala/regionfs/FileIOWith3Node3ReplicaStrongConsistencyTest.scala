package regionfs

import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.apache.zookeeper.server.ByteBufferInputStream
import org.grapheco.regionfs.server.Region
import org.junit.{Assert, Test}

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
    Assert.assertEquals(3, admin.getNodes().size)

    val fid1 = super.writeFile(new File(s"./testdata/inputs/999"))
    Assert.assertEquals(1, (fid1.regionId >> 16).toInt)
    //we should have 3 regions
    Assert.assertEquals(3, admin.getNodes(fid1.regionId).size)
    //now we have 1 region on node2
    Assert.assertEquals(1, admin.getRegions(1).size)
    //we have 1 region on node1
    Assert.assertEquals(1, servers(0).localRegionManager.regions.size)

    for (nodeId <- 1 to 3) {
      assertRegion(nodeId, fid1.regionId) {
        (region) =>
          Assert.assertEquals(65537, region.regionId);
          Assert.assertEquals(1, region.fileCount);
          Assert.assertEquals(1, region.revision);
          Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999"))),
            IOUtils.toByteArray(new ByteBufferInputStream(region.read(fid1.localId).get)))
      }
    }

    val fid2 = super.writeFile(new File(s"./testdata/inputs/9999"))
    Assert.assertEquals(2, (fid2.regionId >> 16).toInt)
    Assert.assertEquals(3, admin.getNodes(fid2.regionId).size)

    Assert.assertEquals(2, admin.getRegions(2).size)
    Assert.assertEquals(2, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(2, servers(0).localRegionManager.regions.size)

    for (nodeId <- 1 to 3) {
      assertRegion(nodeId, fid2.regionId) {
        (region) =>
          Assert.assertEquals(131073, region.regionId);
          Assert.assertEquals(1, region.fileCount);
          Assert.assertEquals(1, region.revision);
          Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/9999"))),
            IOUtils.toByteArray(new ByteBufferInputStream(region.read(fid2.localId).get)))
      }
    }

    val fid3 = super.writeFile(new File(s"./testdata/inputs/99999"))
    Assert.assertEquals(3, (fid3.regionId >> 16).toInt)
    Assert.assertEquals(3, admin.getNodes(fid3.regionId).size)

    Assert.assertEquals(3, servers(0).localRegionManager.regions.size)
    Assert.assertEquals(3, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(3, servers(2).localRegionManager.regions.size)

    val fid4 = super.writeFile(new File(s"./testdata/inputs/999"))
    Assert.assertEquals(1, (fid4.regionId >> 16).toInt)
    Assert.assertEquals(3, admin.getNodes(fid4.regionId).size)

    Assert.assertEquals(4, servers(0).localRegionManager.regions.size)
    Assert.assertEquals(4, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(4, servers(2).localRegionManager.regions.size)

    for (nodeId <- 1 to 3) {
      assertRegion(nodeId, fid4.regionId) {
        (region) =>
          Assert.assertEquals(65537, region.regionId);
          Assert.assertEquals(2, region.fileCount);
          Assert.assertEquals(2, region.revision);
      }
    }

    Assert.assertEquals(3, servers(0).localRegionManager.regions.size)

    //write large files
    super.writeFile(new File(s"./testdata/inputs/9999999"))
    super.writeFile(new File(s"./testdata/inputs/9999999"))
    super.writeFile(new File(s"./testdata/inputs/9999999"))
    super.writeFile(new File(s"./testdata/inputs/9999999"))

    Assert.assertEquals(6, servers(0).localRegionManager.regions.size)
    Assert.assertEquals(6, servers(1).localRegionManager.regions.size)
    Assert.assertEquals(6, servers(2).localRegionManager.regions.size)
  }
}
