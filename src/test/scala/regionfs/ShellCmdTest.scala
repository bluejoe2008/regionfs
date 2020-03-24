package regionfs

import org.grapheco.regionfs.tool.RegionFsCmd
import org.junit.Test

/**
  * Created by bluejoe on 2020/2/8.
  */
class ShellCmdTest extends FileTestBase {
  override val con = new StrongMultiNode

  @Test
  def testStat(): Unit = {
    RegionFsCmd.main("stat-all -zk localhost:2181".split(" "));
    RegionFsCmd.main("put -zk localhost:2181 ./pom.xml ./README.md".split(" "));
    RegionFsCmd.main("stat-all -zk localhost:2181".split(" "));
  }
}
