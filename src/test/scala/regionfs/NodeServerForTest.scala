package regionfs


import java.io.File

import cn.bluejoe.regionfs.server.FsNodeServer

/**
  * Created by bluejoe on 2019/8/31.
  */
object NodeServerForTest {
  val configFile = new File("./node1.conf")
  val maybeServer = try {
    //this server will not startup due to lock by annother process
    val server = FsNodeServer.create(configFile)
    Thread.sleep(1000)
    Some(server)
  }
  catch {
    case e: Throwable => None
  }
}