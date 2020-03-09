package org.grapheco.regionfs.tool

import java.io.{File, FileOutputStream, InputStream}

import org.apache.commons.cli._
import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.client.FsAdmin
import org.grapheco.regionfs.server.FsNodeServer
import org.grapheco.regionfs.{FileId, GlobalConfigWriter}
import org.grapheco.regionfs.util.ByteBufferConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/2/25.
  */
object RegionFSCmd {
  val commands = Array[(String, String, ShellCommandExecutor)](
    ("help", "print usage information", null),
    ("stat-all", "report statistics of all nodes", new StatAllShellCommandExecutor()),
    ("greet", "notify a node server to print a message to be noticed", new GreetShellCommandExecutor()),
    ("stat-node", "report statistics of a node", new StatNodeShellCommandExecutor()),
    ("start-local-node", "start a local node server", new StartNodeShellCommandExecutor()),
    ("config", "configure global setting", new ConfigShellCommandExecutor()),
    ("clean-all", "clean data on a node", new CleanAllShellCommandExecutor()),
    ("clean-node", "clean data on all nodes", new CleanNodeShellCommandExecutor()),
    ("shutdown-all", "shutdown all nodes", new ShutdownAllShellCommandExecutor()),
    ("shutdown-node", "shutdown a node", new ShutdownNodeShellCommandExecutor()),
    ("put", "put local files into regionfs", new PutFilesShellCommandExecutor()),
    ("get", "get remote files", new GetFilesShellCommandExecutor()),
    ("delete", "delete remote files", new DeleteFilesShellCommandExecutor())
  )

  commands.filter(_._3 != null).foreach(x => x._3.init(Array("rfs", x._1)))

  //mvn exec:java -Dexec.mainClass="org.grapheco.regionfs.tool.RegionFSCmd" -Dexec.args="stat" -DskipTests
  def main(args: Array[String]) {
    if (args.length < 1) {
      printError("no command designated");
    }
    else {
      args(0).toLowerCase() match {
        case "help" =>
          printUsage()
        case cmd: String =>
          val opt = commands.find(_._1.equals(cmd.toLowerCase()))
          if (opt.isDefined) {
            opt.get._3.parseAndRun(args.takeRight(args.length - 1))
          }
          else {
            printError(s"unrecognized command: $cmd");
          }
      }
    }
  }

  private def printError(msg: String): Unit = {
    println(msg)
    printUsage()
  }

  private def printUsage(): Unit = {
    val maxlen = commands.map(_._1.length).max
    println("rfs <command> [args]")
    println("commands:")
    commands.sortBy(_._1).foreach { en =>
      val space = {
        (1 to (maxlen + 4 - en._1.length)).map(_ => " ").mkString("")
      }
      println(s"\t${en._1}${space}${en._2}")
    }
  }
}

trait ShellCommandExecutor {
  val commandNamePath = ArrayBuffer[String]()

  def init(cmds: Array[String]): Unit = {
    commandNamePath ++= cmds
  }

  lazy val OPTIONS: Options = {
    val ops = new Options();
    buildOptions(ops);
    ops
  }

  def buildOptions(options: Options)

  def parseAndRun(args: Array[String]): Unit = {
    val commandLineParser = new DefaultParser();

    try {
      val commandLine = commandLineParser.parse(OPTIONS, args);
      run(commandLine)
    }
    catch {
      case e: ParseException =>
        println(e.getMessage());
        printUsage();
    }
  }

  def run(commandLine: CommandLine)

  private def printUsage(): Unit = {
    val formatter = new HelpFormatter();
    formatter.printHelp(s"${commandNamePath.mkString(" ")}", OPTIONS, true);
    System.out.println();
  }
}

class ConfigShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("conf")
      .argName("globalConfigFile")
      .desc("conf file path of global setting, e.g conf/global.conf")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    new GlobalConfigWriter().write(new File(commandLine.getOptionValue("conf")))
    println("cluster is successfully configured.");
  }
}

class StatAllShellCommandExecutor extends ShellCommandExecutor {
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
    println(s"[region-fs]")
    admin.stat(Duration("4s")).nodeStats.foreach { x =>
      println(s"    ╰┈┈┈[node-${x.nodeId}](address=${x.address})")
      x.regionStats.foreach { y =>
        println(s"    ┆       ╰┈┈┈[region-${y.regionId}](file number=${y.fileCount}, total size=${y.totalSize})")
      }
    }

    admin.close
  }
}

class StatNodeShellCommandExecutor extends ShellCommandExecutor {
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
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val ns = admin.statNode(commandLine.getOptionValue("node").toInt, Duration("4s"));
    println(s"[node-${ns.nodeId}](address=${ns.address})")
    ns.regionStats.foreach { y =>
      println(s"    ╰┈┈┈[region-${y.regionId}](file number=${y.fileCount}, total size=${y.totalSize})")
    }

    admin.close
  }
}

class GreetShellCommandExecutor extends ShellCommandExecutor {
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
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val (nodeId, addr) = admin.greet(commandLine.getOptionValue("node").toInt, Duration("4s"))
    println(s"greeted node-${nodeId} on ${addr}.")

    admin.close
  }
}

class StartNodeShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("conf")
      .argName("nodeConfigFile")
      .desc("conf file path of local node server, e.g conf/node.conf")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val server = FsNodeServer.create(new File(commandLine.getOptionValue("conf")))
    server.awaitTermination()
  }
}

class ShutdownAllShellCommandExecutor extends ShellCommandExecutor {
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
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    admin.shutdownAllNodes(Duration("4s")).foreach { x =>
      println(s"shutdowning node-${x._1} on ${x._2}...")
    }
    admin.close
  }
}

class ShutdownNodeShellCommandExecutor extends ShellCommandExecutor {
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
      .required(true)
      .build())

    options.addOption(Option.builder("f")
      .argName("force")
      .desc("force this operation")
      .hasArg(false)
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val (nodeId, address) = admin.shutdownNode(commandLine.getOptionValue("node").toInt, Duration("4s"))
    println(s"shutdowning node-$nodeId on address...")
    admin.close
  }
}

class CleanAllShellCommandExecutor extends ShellCommandExecutor {
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
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val addrs = admin.cleanAllData(Duration("4s"))
    addrs.foreach(addr => println(s"cleaned data on $addr..."))
    admin.close
  }
}

class CleanNodeShellCommandExecutor extends ShellCommandExecutor {
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
      .required(true)
      .build())

    options.addOption(Option.builder("f")
      .argName("force")
      .desc("force this operation")
      .hasArg(false)
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val addr = admin.cleanNodeData(commandLine.getOptionValue("node").toInt, Duration("4s"))
    println(s"cleaned data on $addr...")
    admin.close
  }
}

class PutFilesShellCommandExecutor extends ShellCommandExecutor {
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
    val args = commandLine.getArgs;
    if (args.length == 0) {
      throw new ParseException(s"file path is required");
    }

    val wrongs = args.map(new File(_)).find(!_.exists())
    if (!wrongs.isEmpty) {
      throw new ParseException(s"wrong file path: ${wrongs.map(_.getCanonicalFile.getAbsolutePath).mkString(",")}");
    }

    println(s"putting ${args.length} file(s):");

    for (path <- args) {
      val file = new File(path)

      val id = Await.result(admin.writeFile(file), Duration("4s"))
      println(s"\t${file.getAbsoluteFile.getCanonicalPath} -> ${FileId.toBase64String(id)}")
    }

    admin.close
  }
}

class DeleteFilesShellCommandExecutor extends ShellCommandExecutor {
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
    val args = commandLine.getArgs;
    if (args.length == 0) {
      throw new ParseException(s"file id is required");
    }

    println(s"deleting ${args.length} file(s):");

    for (arg <- args) {
      val id = FileId.fromBase64String(arg)
      Await.result(admin.deleteFile(id), Duration("4s"));
      println(s"${arg}");
    }

    admin.close
  }
}

class GetFilesShellCommandExecutor extends ShellCommandExecutor {
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
    def write(label: String, id: FileId, is: InputStream);

    def done();
  }

  override def run(commandLine: CommandLine): Unit = {
    val admin: FsAdmin = new FsAdmin(commandLine.getOptionValue("zk"))
    val args = commandLine.getArgs;
    if (args.length == 0) {
      throw new ParseException(s"file id is required");
    }

    val output: FileOutput =
      if (commandLine.hasOption("dir")) {
        val dir = new File(commandLine.getOptionValue("dir"))
        if (!dir.exists()) {
          throw new ParseException(s"wrong file dir: ${dir.getCanonicalFile.getAbsolutePath}");
        }

        new FileOutput {
          override def write(label: String, id: FileId, is: InputStream): Unit = {
            val file = new File(dir, label)
            val os = new FileOutputStream(file);
            IOUtils.copy(is, os)
            os.close()
            println(s"\t${label}->${file.getAbsoluteFile.getCanonicalPath}")
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

    println(s"getting ${args.length} file(s):");

    for (arg <- args) {
      val id = FileId.fromBase64String(arg)
      val is = admin.readFile(id, Duration("4s"))
      output.write(arg, id, is)
      is.close()
    }

    output.done()
    admin.close
  }
}