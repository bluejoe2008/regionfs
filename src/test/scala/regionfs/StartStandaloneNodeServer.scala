package regionfs

import java.io.File

import org.grapheco.regionfs.server.FsNodeServer

/**
  * Created by bluejoe on 2020/2/24.
  */
object StartStandaloneNodeServer {
  def main(args: Array[String]) {
    if (args.length != 1) {
      println(s"USAGE: ${this.getClass.getSimpleName.dropRight(1)} <conf-file-path>")
    }
    else {
      val server = FsNodeServer.create(new File(args(0)))
      server.awaitTermination()
    }
  }
}
