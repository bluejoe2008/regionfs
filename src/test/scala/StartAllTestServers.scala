import java.io.File

import cn.graiph.blobfs.shell.StartNodeServer
import org.apache.commons.io.FileUtils
import org.junit.Test

/**
  * Created by bluejoe on 2019/8/25.
  */

object StartAllTestServers {
  def main(args: Array[String]) {

    val test = new FsServerTest();
    test.cleanNodes();

    val threads = Array("./node1.conf", "./node2.conf", "./node3.conf").map { x =>
      new Thread(new Runnable {
        override def run(): Unit = {
          StartNodeServer.main(Array(x))
        }
      })
    }

    threads.foreach(_.start())
    threads(0).join()
  }
}

