package cn.graiph.regionfs.shell

import java.io.File

import cn.graiph.regionfs.FileSystemStarter

/**
  * Created by bluejoe on 2020/2/6.
  */
object ShellFileSystemStarter {
  def main(args: Array[String]) {
    new FileSystemStarter().start(new File(args(0)));
  }
}
