import java.io.File

import cn.graiph.blobfs.shell.StartNodeServer
import org.apache.commons.io.FileUtils

/**
  * Created by bluejoe on 2019/8/25.
  */

object FsServerTest {
  def main(args: Array[String]) {
    Array("./testdata/node1", "./testdata/node2", "./testdata/node3").
      foreach(x => FileUtils.cleanDirectory(new File(x)))

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