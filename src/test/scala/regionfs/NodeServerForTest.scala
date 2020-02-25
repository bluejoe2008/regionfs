package regionfs


import java.io.File

import cn.bluejoe.regionfs.server.FsNodeServer

/**
  * Created by bluejoe on 2019/8/31.
  */
object NodeServerForTest {
  val configFile = new File("./node1.conf")
  val server = try {
    //this server will not startup due to lock by annother process
    FsNodeServer.create(configFile)
    Thread.sleep(1000)
  }
  catch {
    case e:Throwable => null
  }
}