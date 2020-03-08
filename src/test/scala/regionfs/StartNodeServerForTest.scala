package regionfs

import java.io.File

import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.server.FsNodeServer

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2020/3/8.
  */
object StartNodeServerForTest extends Logging {
  val zookeeperString = "localhost:2181"
  var servers = ArrayBuffer[FsNodeServer]()

  val SERVER_NODE_ID = Array(1 -> 1224, 2 -> 1225, 3 -> 1226)
  val confs = SERVER_NODE_ID.map(x => {
    Map[String, String](
      "zookeeper.address" -> zookeeperString,
      "server.host" -> "localhost",
      "server.port" -> s"${x._2}",
      "data.storeDir" -> new File(s"./testdata/nodes/node${x._1}").getCanonicalFile.getAbsolutePath,
      "node.id" -> s"${x._1}"
    )
  })

  for (conf <- confs) {
    try {
      new File(conf("data.storeDir")).mkdirs()
      servers += FsNodeServer.create(conf)
    }
    catch {
      case e: Throwable => {
        logger.warn(e.getMessage)
      }
    }
  }

  servers.foreach(server =>
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(5000)
        server.awaitTermination()
      }
    }).start()
  )

  Thread.sleep(1000)
}
