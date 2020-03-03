package regionfs

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer

import org.grapheco.commons.util.{Logging, Profiler}
import org.grapheco.regionfs.FileId
import org.grapheco.regionfs.client.FsClient
import org.grapheco.regionfs.server.FsNodeServer
import org.junit.{After, Before}
import org.grapheco.regionfs.util.ByteBufferConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileTestBase extends Logging {
  Profiler.enableTiming = true
  val BLOB_LENGTH = Array[Long](999, 9999, 99999, 999999, 9999999)

  val configFile = new File("./node1.conf")
  var server: FsNodeServer = null
  var client: FsClient = null

  @Before
  def setup() {
    try {
      //this server will not startup due to lock by annother process
      server = FsNodeServer.create(configFile)
      //Thread.sleep(1000)
    }
    catch {
      case e: Throwable => {
        logger.warn(e.getMessage)
        server = null
      }
    }

    client = new FsClient("localhost:2181")

    for (i <- BLOB_LENGTH) {
      makeFile(new File(s"./testdata/inputs/$i"), i)
    }
  }

  @After
  def after(): Unit = {
    if (server != null)
      server.shutdown()
    if (client != null)
      client.close

    //waiting zookeeper disconnect
    //Thread.sleep(1000)
  }

  def writeFile(src: File): FileId = {
    Await.result(client.writeFile(src), Duration("4s"))
  }

  def writeFile(text: String): FileId = {
    writeFile(text.getBytes)
  }

  private def writeFile(bytes: Array[Byte]): FileId = {
    Await.result(client.writeFile(ByteBuffer.wrap(bytes)), Duration("4s"))
  }

  private def writeFileAsync(bytes: Array[Byte]): Future[FileId] = {
    val fid = client.writeFile(ByteBuffer.wrap(bytes))
    fid
  }

  def writeFileAsync(text: String): Future[FileId] = {
    writeFileAsync(text.getBytes)
  }

  def writeFileAsync(src: File): Future[FileId] = {
    val fid = client.writeFile(src)
    fid
  }

  def makeFile(dst: File, length: Long): Unit = {
    val fos = new FileOutputStream(dst)
    var n: Long = 0
    while (n < length) {
      val left: Int = Math.min((length - n).toInt, 10240)
      fos.write((0 to left - 1).map(x => ('a' + x % 26).toByte).toArray)
      n += left
    }

    fos.close()
  }
}
