package org.grapheco.regionfs.tool

import java.nio.file.Paths
import java.util.Objects

import jnr.ffi.Pointer
import jnr.ffi.types.{off_t, size_t}
import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.FileId
import org.grapheco.regionfs.client.FsAdmin
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/3/27.
  */
object MountRegionFsFuseService {
  def main(args: Array[String]) {
    val path = "/tmp/regionfs/"
    val zks = "localhost:2181"
    val fs = new RegionFsFuse(zks)

    fs.mount(Paths.get(path), true, true);
  }
}

class RegionFsFuse(zks: String) extends FuseStubFS {
  val admin = new FsAdmin(zks)

  override def open(path: String, fi: FuseFileInfo): Int = {
    val fid = path2FileId(path)

    if (fid.isEmpty) {
      -ErrorCodes.ENOENT
    }
    else {
      0
    }
  }

  private def path2FileId(path: String): Option[FileId] = {
    val idString =
      if (path.startsWith("/")) {
        path.substring("/".length)
      }
      else {
        path
      }

    try {
      Some(FileId.fromBase64String(idString))
    }
    catch {
      case _ => None
    }
  }

  override def getattr(path: String, stat: FileStat): Int = {
    if (Objects.equals(path, "/")) {
      stat.st_mode.set(FileStat.S_IFDIR | 0x1ed)
      stat.st_nlink.set(2)
      0
    }
    else {
      val fid = path2FileId(path)

      if (fid.isDefined) {
        stat.st_mode.set(FileStat.S_IFREG | 0x124)
        stat.st_nlink.set(1)
        stat.st_size.set(Await.result(admin.readFile(fid.get, (is) => {
          val bytes = IOUtils.toByteArray(is)
          bytes.length
        }), Duration.Inf))
        0
      }
      else {
        -ErrorCodes.ENOENT
      }
    }
  }

  override def read(path: String, buf: Pointer, @size_t size: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
    val fid = path2FileId(path)

    if (fid.isDefined) {
      Await.result(admin.readFile(fid.get, (is) => {
        val bytes = IOUtils.toByteArray(is)
        buf.put(0, bytes, 0, bytes.length)
        bytes.length
      }), Duration.Inf)
    }
    else {
      -ErrorCodes.ENOENT
    }
  }

  override def readdir(path: String, buf: Pointer, filter: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = {
    if (!("/" == path)) {
      -ErrorCodes.ENOENT
    }
    else {
      filter.apply(buf, ".", null, 0)
      filter.apply(buf, "..", null, 0)

      admin.listFiles(Duration.Inf).foreach(x =>
        filter.apply(buf, x._1.toBase64String(), null, 0))
      0
    }
  }
}
