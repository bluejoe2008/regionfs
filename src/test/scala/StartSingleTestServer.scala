import cn.graiph.regionfs.shell.{ShellNodeServerStarter}

/**
  * Created by bluejoe on 2019/8/31.
  */
object StartSingleTestServer {
  def main(args: Array[String]) {
    //example args: ./node1.conf
    ShellNodeServerStarter.main(args)
  }
}
