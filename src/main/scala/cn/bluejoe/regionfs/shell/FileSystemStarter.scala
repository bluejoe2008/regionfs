package cn.bluejoe.regionfs.shell

import java.io.File

import cn.bluejoe.regionfs.FileSystemStarter

/**
  * Created by bluejoe on 2020/2/6.
  */
object ShellFileSystemStarter {
  def main(args: Array[String]) {
    new FileSystemStarter().start(new File(args(0)));
  }
}
