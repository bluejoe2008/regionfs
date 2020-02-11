import cn.graiph.regionfs.shell.{ShellFileSystemStarter, ShellNodeServerStarter}

/**
  * Created by bluejoe on 2019/8/31.
  */
object StartSingleTestServer {
  def main(args: Array[String]) {
    ShellFileSystemStarter.main(Array("./regionfs.conf"));
    ShellNodeServerStarter.main(Array("./node1.conf"))
  }
}

object StartAnyTestServer {
  //args: ./node1.conf
  def main(args: Array[String]) {
    ShellNodeServerStarter.main(args)
  }
}