package cn.bluejoe.regionfs.shell

import java.io.File

import cn.bluejoe.regionfs.server.FsNodeServer

/**
  * Created by bluejoe on 2019/8/31.
  */
object ShellNodeServerStarter {
  def main(args: Array[String]) {
    if (args.length != 1)
      throw new RuntimeException("conf file is required!")

    val server = FsNodeServer.create(new File(args(0)))
    server.startup()
  }
}
