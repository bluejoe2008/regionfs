package regionfs

import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.grapheco.hippo.util.ByteBufferInputStream
import org.grapheco.regionfs.FileId
import org.grapheco.regionfs.server.Region
import org.junit.{After, Assert, Test}

class FileIOWith3Node3ReplicaEventualConsistencyTest extends FileIOWith1Node1ReplicaTest {
  override val con = new EventualMultiNode

  private def assertRegion(nodeId: Int, regionId: Long)(op: (Region) => Unit) = {
    val region = servers(nodeId - 1).localRegionManager.regions(regionId)
    op(region)
  }

  private def getNodeId(fid: FileId): Int = (fid.regionId >> 16).toInt

  @After
  def sleepBeforeShutdown(): Unit = {
    println("wait PrimaryRegionWatcher to sync regions...")
    servers.foreach(_.primaryRegionWatcher.foreach(_.stop()))
  }

  @Test
  def testRegionSync(): Unit = {
    //we have started 3 nodes
    Assert.assertEquals(3, admin.getNodes().size)

    val fid1 = super.writeFile(new File(s"./testdata/inputs/999"))
    Assert.assertEquals(1, getNodeId(fid1))
    Assert.assertEquals(3, admin.getNodes(fid1.regionId).size)
    Assert.assertEquals(1, admin.getRegions(1).size)
    Assert.assertEquals(1, servers(0).localRegionManager.regions.size)

    assertRegion(1, fid1.regionId) {
      (region) =>
        Assert.assertEquals(65537, region.regionId);
        Assert.assertEquals(1, region.fileCount);
        Assert.assertEquals(1, region.revision);
        Assert.assertEquals(true, region.isPrimary);
    }

    //regions on node-2,3 have not be synced
    assertRegion(2, fid1.regionId) {
      (region) =>
        Assert.assertEquals(65537, region.regionId);
        Assert.assertEquals(0, region.fileCount);
        Assert.assertEquals(0, region.revision);
        Assert.assertEquals(false, region.isPrimary);
    }

    assertRegion(3, fid1.regionId) {
      (region) =>
        Assert.assertEquals(65537, region.regionId);
        Assert.assertEquals(0, region.fileCount);
        Assert.assertEquals(0, region.revision);
        Assert.assertEquals(false, region.isPrimary);
    }

    Thread.sleep(4000);

    //now regions will be synced
    assertRegion(2, fid1.regionId) {
      (region) =>
        Assert.assertEquals(65537, region.regionId);
        Assert.assertEquals(1, region.fileCount);
        Assert.assertEquals(1, region.revision);
        Assert.assertEquals(false, region.isPrimary);

        Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999"))),
          IOUtils.toByteArray(new ByteBufferInputStream(region.read(fid1.localId).get)))
    }

    assertRegion(3, fid1.regionId) {
      (region) =>
        Assert.assertEquals(65537, region.regionId);
        Assert.assertEquals(1, region.fileCount);
        Assert.assertEquals(1, region.revision);
        Assert.assertEquals(false, region.isPrimary);

        Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999"))),
          IOUtils.toByteArray(new ByteBufferInputStream(region.read(fid1.localId).get)))
    }

    //now, we write large files
    (1 to 10).foreach(x => super.writeFile(new File(s"./testdata/inputs/9999999")))
    val fid2 = super.writeFile(new File(s"./testdata/inputs/9999999"))
    Thread.sleep(4000);

    (1 to 3).foreach { nodeId =>
      assertRegion(nodeId, fid2.regionId) {
        (region) =>
          Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/9999999"))),
            IOUtils.toByteArray(new ByteBufferInputStream(region.read(fid1.localId).get)))
      }
    }
  }
}