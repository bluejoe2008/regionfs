package org.grapheco.regionfs.tool

import java.io.{File, FileOutputStream, InputStream}

import net.neoremind.kraps.rpc.RpcAddress
import org.apache.commons.cli._
import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.client.FsAdmin
import org.grapheco.regionfs.server.RegionInfo
import org.grapheco.regionfs.util.ByteBufferConversions._
import org.grapheco.regionfs.util.ZooKeeperClient
import org.grapheco.regionfs.{FileId, GlobalSettingWriter}

import scala.collection.JavaConversions
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/2/25.
  */
object RegionFsCmd extends CommandsLauncher {
  override val commands = Array[(String, String, ShellCommandExecutor)](
    ("help", "print usage information", null),
    ("stat", "report statistics of all nodes (or a given node)", new StatShellCommandExecutor()),
    ("greet", "notify all nodes (or a given node) to print a message to be noticed", new GreetShellCommandExecutor()),
    ("config", "dispaly (or set) global setting", new ConfigShellCommandExecutor()),
    ("clean", "clean data on all nodes (or a given node)", new CleanDataShellCommandExecutor()),
    ("shutdown", "shutdown all nodes (or a given node)", new ShutdownShellCommandExecutor()),
    ("put", "put local files into regionfs", new PutFilesShellCommandExecutor()),
    ("get", "get remote files", new GetFilesShellCommandExecutor()),
    ("regions", "list regions on all nodes (or a given node)", new ListRegionsShellCommandExecutor()),
    ("delete", "delete remote files", new DeleteFilesShellCommandExecutor())
  )
  override val launcherName: String = "rfs"
}

class ConfigShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("conf")
      .argName("globalSettingFile")
      .desc("conf file path of global setting, e.g conf/global.conf")
      .hasArg
      .required(false)
      .build())

    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(false)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    (commandLine.hasOption("conf"), commandLine.hasOption("zk")) match {
      case (true, _) =>
        val configFile = new File(commandLine.getOptionValue("conf")).getAbsoluteFile.getCanonicalFile
        println(s"loading global setting from file `${configFile.getPath}`")
        new GlobalSettingWriter().write(configFile)
        println("global setting is successfully configured.");

      case (_, true) =>
        val zk = ZooKeeperClient.create(commandLine.getOptionValue("zk"))
        println("global setting {")
        JavaConversions.mapAsScalaMap(zk.loadGlobalSetting().props).foreach(x => {
          println(s"\t${x._1} = ${x._2}")
        })
        println("}");
      case (false, false) =>
        println("either `-conf` or `-zk` is required");
    }
  }
}

private class StatShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("node")
      .argName("nodeid")
      .desc("node id, e.g 1")
      .hasArg
      .required(false)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val list =
      if (commandLine.hasOption("node")) {
        val nodeId: Int = commandLine.getOptionValue("node").toInt
        List(admin.statNode(nodeId, Duration("4s")))
      }
      else {
        admin.stat(Duration("4s")).nodeStats
      }

    for (ns <- list) {
      println(s"[node-${ns.nodeId}](address=${ns.address})")
      ns.regionStats.foreach { y =>
        println(s"    ╰┈┈┈[region-${y.regionId}](file number=${y.fileCount}, total size=${y.totalSize})")
      }
    }

    admin.close
  }
}

private class ListRegionsShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("node")
      .argName("nodeid")
      .desc("node id, e.g 1")
      .hasArg
      .required(false)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val list: Array[(Int, RpcAddress, Array[RegionInfo])] =
      if (commandLine.hasOption("node")) {
        val nodeId: Int = commandLine.getOptionValue("node").toInt
        Array(Tuple3(nodeId, admin.mapNodeWithAddress(nodeId),
          admin.askRegionsOnNode(nodeId, Duration("4s"))))
      }
      else {
        admin.getAvaliableNodes().map { nodeId =>
          Tuple3(nodeId, admin.mapNodeWithAddress(nodeId), admin.askRegionsOnNode(nodeId, Duration("4s")))
        }.toArray
      }

    for (ns <- list) {
      println(s"[node-${ns._1}](address=${ns._2})")
      ns._3.foreach { ri =>
        println(s"    ╰┈┈┈[region-${ri.regionId}](cursor=${ri.revision}, total size=${ri.length}, primary=${ri.isPrimary}, writable=${ri.isWritable})")
      }
    }

    admin.close
  }
}

private class GreetShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("node")
      .argName("nodeid")
      .desc("node id, e.g 1")
      .hasArg
      .required(false)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val list: Array[Int] =
      if (commandLine.hasOption("node")) {
        Array(commandLine.getOptionValue("node").toInt)
      }
      else {
        admin.getAvaliableNodes().toArray
      }

    for (ns <- list) {
      val (nodeId, addr) = admin.greet(ns, Duration("4s"))
      println(s"greeted node-$nodeId on $addr.")
    }

    admin.close
  }
}

private class ShutdownShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("f")
      .argName("force")
      .desc("force this operation")
      .hasArg(false)
      .required(true)
      .build())

    options.addOption(Option.builder("node")
      .argName("nodeid")
      .desc("node id, e.g 1")
      .hasArg
      .required(false)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val list: Array[Int] =
      if (commandLine.hasOption("node")) {
        Array(commandLine.getOptionValue("node").toInt)
      }
      else {
        admin.getAvaliableNodes().toArray
      }

    list.foreach { nodeId =>
      val (_, address) = admin.shutdownNode(nodeId, Duration("4s"))
      println(s"shutdowning node-$nodeId on $address...")
    }

    admin.close
  }
}

private class CleanDataShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("f")
      .argName("force")
      .desc("force this operation")
      .hasArg(false)
      .required(false)
      .build())

    options.addOption(Option.builder("node")
      .argName("nodeid")
      .desc("node id, e.g 1")
      .hasArg
      .required(false)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val list: Array[Int] =
      if (commandLine.hasOption("node")) {
        Array(commandLine.getOptionValue("node").toInt)
      }
      else {
        admin.getAvaliableNodes().toArray
      }

    list.foreach { nodeId =>
      val addr = admin.cleanNodeData(nodeId, Duration("4s"))
      println(s"cleaned data on $addr...")
    }

    admin.close
  }
}

private class PutFilesShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val args = commandLine.getArgs
    //noinspection EmptyCheck
    if (args.isEmpty) {
      throw new ParseException(s"file path is required")
    }

    val wrongs = args.map(new File(_)).find(!_.exists())
    //noinspection EmptyCheck
    if (wrongs.isDefined) {
      throw new ParseException(s"wrong file path: ${wrongs.map(_.getCanonicalFile.getAbsolutePath).mkString(",")}")
    }

    println(s"putting ${args.length} file(s):")

    for (path <- args) {
      val file = new File(path)

      val id = Await.result(admin.writeFile(file), Duration("4s"))
      println(s"\t${file.getAbsoluteFile.getCanonicalPath} -> ${FileId.toBase64String(id)}")
    }

    admin.close
  }
}

private class DeleteFilesShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val args = commandLine.getArgs
    //noinspection EmptyCheck
    if (args.isEmpty) {
      throw new ParseException(s"file id is required")
    }

    println(s"deleting ${args.length} file(s):")

    for (arg <- args) {
      val id = FileId.fromBase64String(arg)
      Await.result(admin.deleteFile(id), Duration("4s"))
      println(s"$arg")
    }

    admin.close
  }
}

private class GetFilesShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("zk")
      .argName("zkString")
      .desc("zookeeper address, e.g localhost:2181")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("dir")
      .argName("localdir")
      .desc("local directory for saving file")
      .hasArg
      .required(false)
      .build())
  }

  trait FileOutput {
    def write(label: String, id: FileId, is: InputStream)

    def done()
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val args = commandLine.getArgs
    //noinspection EmptyCheck
    if (args.isEmpty) {
      throw new ParseException(s"file id is required")
    }

    val output: FileOutput =
      if (commandLine.hasOption("dir")) {
        val dir = new File(commandLine.getOptionValue("dir"))
        if (!dir.exists()) {
          throw new ParseException(s"wrong file dir: ${dir.getCanonicalFile.getAbsolutePath}")
        }

        new FileOutput {
          override def write(label: String, id: FileId, is: InputStream): Unit = {
            val file = new File(dir, label)
            val os = new FileOutputStream(file)
            IOUtils.copy(is, os)
            os.close()
            println(s"\t$label->${file.getAbsoluteFile.getCanonicalPath}")
          }

          override def done(): Unit = {

          }
        }
      }
      else {
        new FileOutput {
          override def write(label: String, id: FileId, is: InputStream): Unit = {
            println(s"\r\n==============<<<$label>>>===============\r\n")
            val os = System.out
            IOUtils.copy(is, os)
          }

          override def done(): Unit = {

          }
        }
      }

    println(s"getting ${args.length} file(s):")

    for (arg <- args) {
      val id = FileId.fromBase64String(arg)
      Await.result(admin.readFile(id, (is) => {
        output.write(arg, id, is)
      }), Duration("10s"))
    }

    output.done()
    admin.close
  }
}