package cn.regionfs.server

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, Executors}

import cn.regionfs.{StreamingResult, NodeAddress}
import cn.regionfs.util.Logging
import org.apache.commons.io.IOUtils

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

/**
  * Created by bluejoe on 2020/2/5.
  */
class RpcStreams() extends Logging {
  val streams = mutable.Map[Long, RpcStream]()
  val idgen = new AtomicLong(System.currentTimeMillis())
  val threadPool = Executors.newFixedThreadPool(5);

  def create(produce: (Output) => Unit, pageSize: Int): RpcStream = {
    val streamId = idgen.incrementAndGet()
    val tx = new RpcStream(streamId, pageSize, produce, threadPool)
    streams += streamId -> tx
    tx
  }

  def remove(streamId: Long) = streams.remove(streamId)

  def get(streamId: Long): RpcStream = {
    streams(streamId)
  }
}

class RpcStream(val streamId: Long, pageSize: Int, produce: (Output) => Unit, threadPool: ExecutorService) {
  val resultBuffer = new OutputBuffer(pageSize);
  val future = threadPool.submit(new Runnable {
    override def run(): Unit = {
      produce(resultBuffer)
    }
  })

  def nextPage(): (Iterator[_], Boolean) = {
    resultBuffer.readNextPage
  }

  def close(): Unit = {
    future.cancel(true)
  }
}

trait Output {
  def push(result: StreamingResult): Unit

  def pushEOF(): Unit
}

class OutputBuffer(pageSize: Int) extends Output {
  val buffer = new ArrayBlockingQueue[StreamingResult](pageSize * 2);
  var reachEOF = false;

  def push(result: StreamingResult): Unit = {
    if (reachEOF) {
      throw new RegionFsServersException(s"EOF is committed");
    }

    buffer.put(result)
  }

  def pushEOF(): Unit = {
    this.synchronized {
      reachEOF = true;
    }
  }

  def readNextPage(): (Iterator[StreamingResult], Boolean) = {
    val page = new java.util.ArrayList[StreamingResult]();
    val one = buffer.take();
    page.add(one);

    buffer.drainTo(page, pageSize)
    JavaConversions.asScalaIterator(page.iterator) -> !(reachEOF && buffer.isEmpty)
  }
}

/**
  * a TransTx stores chunks for a blob
  * a TxQueue manages all running FileTasks
  * each TransTx has an unique id (transactionId)
  */
class FileTransmissionQueue() extends Logging {
  val transactionalTasks = mutable.Map[Long, FileTransmission]()
  val idgen = new AtomicLong(System.currentTimeMillis())

  def create(region: Region, totalLength: Long): FileTransmission = {
    val transId = idgen.incrementAndGet()
    val tx = new FileTransmission(transId, region, totalLength)

    transactionalTasks += transId -> tx
    tx
  }

  def remove(transId: Long) = transactionalTasks.remove(transId)

  def get(transId: Long): FileTransmission = {
    transactionalTasks(transId)
  }
}

class FileTransmission(val txId: Long, val region: Region, val totalLength: Long) extends Logging {
  //besides this node, neighbour nodes will store replica chunks on the same time
  //neighbourTransactionIds is used to save these ids allocated for replica blob task
  val neighbourTransactionIds = mutable.Map[NodeAddress, Long]()

  def addNeighbourTransactionId(address: NodeAddress, transId: Long): Unit = {
    neighbourTransactionIds += address -> transId
  }

  def getNeighbourTransactionIds() = neighbourTransactionIds.toMap

  case class Chunk(file: File, length: Int, index: Int) {
  }

  //create a new file
  val chunks = ArrayBuffer[Chunk]()
  val actualBytesWritten = new AtomicLong(0)

  //combine all chunks as a complete blob file
  private def combine(transId: Long): File = {
    if (chunks.length == 1) {
      chunks(0).file
    }
    else {
      //create a combined file
      val tmpFile = File.createTempFile(s"regionfs-$transId-", "")
      val fos: FileOutputStream = new FileOutputStream(tmpFile, true)
      chunks.sortBy(_.index).foreach { chunk =>
        val cis = new FileInputStream(chunk.file)
        IOUtils.copy(cis, fos)
        cis.close()
        chunk.file.delete()
      }

      fos.close()
      tmpFile
    }
  }

  //save one chunk, if this is the last chunk, then write all chunks into region
  def writeChunk(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int): Option[Long] = {
    if (logger.isDebugEnabled)
      logger.debug(s"writing chunk: $transId-$chunkIndex, length=$chunkLength")

    //save this chunk into a chunk file
    val tmpFile = this.synchronized {
      File.createTempFile(s"regionfs-$transId-", ".chunk")
    }

    val fos: FileOutputStream = new FileOutputStream(tmpFile)
    IOUtils.copy(new ByteArrayInputStream(chunkBytes.slice(0, chunkLength)), fos)
    fos.close()

    chunks.synchronized {
      chunks += Chunk(tmpFile, chunkLength, chunkIndex)
    }

    val actualBytes = actualBytesWritten.addAndGet(chunkLength)

    //end of file? all chunks are ready!
    if (actualBytes >= totalLength) {
      //combine all chunks to a complete blob
      val combinedFile = combine(transId);
      //save into region
      val localId = region.write(() => new FileInputStream(combinedFile))

      combinedFile.delete()
      Some(localId)
    }
    else {
      None
    }
  }
}
