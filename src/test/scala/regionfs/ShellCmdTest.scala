package regionfs

import org.grapheco.regionfs.tool.RegionFSCmd
import org.junit.Test

/**
  * Created by bluejoe on 2020/2/8.
  */
class ShellCmdTest extends FileTestBase {
  override val con = new MultiNode

  @Test
  def testStat(): Unit = {
    RegionFSCmd.main("put -zk localhost:2181 ./pom.xml ./README.md".split(" "));
    RegionFSCmd.main("stat-all -zk localhost:2181".split(" "));
  }
}
