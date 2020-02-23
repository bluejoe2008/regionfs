package regionfs


import java.io.File

import cn.regionfs.server.FsNodeServer

/**
  * Created by bluejoe on 2019/8/31.
  */
object NodeServerForTest {
  val server = FsNodeServer.create(new File("./node1.conf"))
  Thread.sleep(1000)
}