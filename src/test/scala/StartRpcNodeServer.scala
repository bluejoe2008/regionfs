import java.io.File

import cn.graiph.blobfs.{FileWriteCursor, FsRpcServer}

/**
  * Created by bluejoe on 2019/8/23.
  */
object StartRpcNodeServer {
  def main(args: Array[String]) {
    new FsRpcServer(new File("./"), "localhost",
      1224, FileWriteCursor(1, new File("./"), 1001, 0, 0)).start();
  }
}
