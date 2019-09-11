import java.io.File

import cn.graiph.regionfs.shell.StartNodeServer
import org.apache.commons.io.FileUtils
import org.junit.Test

/**
  * Created by bluejoe on 2019/9/3.
  */
class FsServerTest {
  @Test
  def cleanNodes(): Unit = {
    Array("./testdata/nodes/node1", "./testdata/nodes/node2", "./testdata/nodes/node3").
      foreach(x => FileUtils.cleanDirectory(new File(x)))
  }
}
