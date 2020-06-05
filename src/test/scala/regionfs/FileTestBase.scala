package regionfs

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer

import org.apache.commons.io.{FileUtils, IOUtils}
import org.grapheco.commons.util.{Logging, Profiler}
import org.grapheco.regionfs.client.{FsAdmin, FsClient}
import org.grapheco.regionfs.server.{FsNodeServer, Region, RegionEvent, RegionEventListener}
import org.grapheco.regionfs.util.ByteBufferConversions._
import org.grapheco.regionfs.{FileId, GlobalSettingWriter}
import org.junit.{After, Before}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileTestBase extends Logging {
  val con: TestCondition = new SingleNode()

  val nullRegionEventListener = new RegionEventListener {
    override def handleRegionEvent(event: RegionEvent): Unit = {}
  }

  val BLOB_LENGTH = Array[Long](999, 2048, 9999, 99999, 999999, 9999999)

  var servers = ArrayBuffer[FsNodeServer]()
  var client: FsClient = null
  var admin: FsAdmin = null

  def readBytes(f: File): Array[Byte] = {
    IOUtils.toByteArray(new FileInputStream(f))
  }

  def primaryRegionOf(regionId: Long): Region = {
    servers.find(_.nodeId == (regionId >> 16).toInt).head.localRegionManager.get(regionId).get
  }

  def countFiles(region: Region): Int = {
    region.listFiles().size
  }

  def countFiles() =
    admin.stat(Duration("4s")).nodeStats.map(_.regionStats.map(_.fileCount).sum).sum

  def readBytes(fid: FileId): Array[Byte] = {
    Await.result(toBytesAsync(fid), Duration("10s"))
  }

  def toBytesAsync(fid: FileId): Future[Array[Byte]] = {
    client.readFile(fid, (is) => {
      IOUtils.toByteArray(is)
    })
  }

  @Before
  def setup() {
    FileUtils.deleteDirectory(new File("./testdata/nodes"));

    Profiler.enableTiming = true
    new GlobalSettingWriter().write(con.GLOBAL_SETTING);

    //this server will not startup due to lock by annother process
    val confs = con.SERVER_NODE_ID.map(x => {
      Map[String, String](
        "zookeeper.address" -> con.zookeeperString,
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

    admin = new FsAdmin(con.zookeeperString)
    client = admin

    new File(s"./testdata/inputs").mkdirs()
    for (i <- BLOB_LENGTH) {
      makeFile(new File(s"./testdata/inputs/$i"), i)
    }
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

  @After
  def after(): Unit = {
    if (client != null)
      client.close

    servers.foreach(_.shutdown())
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

  def writeFileAsync(text: String): Future[FileId] = {
    writeFileAsync(text.getBytes)
  }

  private def writeFileAsync(bytes: Array[Byte]): Future[FileId] = {
    val fid = client.writeFile(ByteBuffer.wrap(bytes))
    fid
  }

  def writeFileAsync(src: File): Future[FileId] = {
    val fid = client.writeFile(src)
    fid
  }
}
