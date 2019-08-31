package cn.graiph.blobfs.shell

import java.io.File

import cn.graiph.blobfs.FsNodeServer

/**
  * Created by bluejoe on 2019/8/31.
  */
object StartNodeServer {
  def main(args: Array[String]) {
    if (args.length != 1)
      throw new RuntimeException("conf file is required!")

    val server = FsNodeServer.build(new File(args(0)))
    server.start()
  }
}
