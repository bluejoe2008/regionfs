import cn.graiph.regionfs.shell.StartNodeServer

/**
  * Created by bluejoe on 2019/8/31.
  */
object StartSingleTestServer {
  def main(args: Array[String]) {
    //example args: ./node1.conf
    StartNodeServer.main(Array[String]("node1.conf"))
  }
}
