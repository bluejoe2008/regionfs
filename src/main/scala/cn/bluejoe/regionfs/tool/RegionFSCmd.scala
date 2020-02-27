package cn.bluejoe.regionfs.tool

import java.io.File

import cn.bluejoe.regionfs.GlobalConfigConfigurer
import cn.bluejoe.regionfs.client.{FsAdmin, FsNodeClient}
import cn.bluejoe.regionfs.server.FsNodeServer
import org.apache.commons.cli._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2020/2/25.
  */
object RegionFSCmd {
  val commands = Array[(String, String, ShellCommandExecutor)](
    ("help", "print usage information", null),
    ("stat-all", "report statistics of all nodes", new StatShellCommandExecutor()),
    ("greet", "notify a node server to print a message to be noticed", new GreetShellCommandExecutor()),
    ("stat-node", "report statistics of a node", new StatShellCommandExecutor()),
    ("start-local-node", "start a local node server", new StartNodeShellCommandExecutor()),
    ("init", "initialize global setting", new InitShellCommandExecutor()),
    ("clean-all", "clean data on all nodes, or a node", new CleanAllShellCommandExecutor()),
    ("clean-node", "clean data on all nodes, or a node", new CleanNodeShellCommandExecutor()),
    ("shutdown-all", "shutdown all nodes, or a node", new ShutdownAllShellCommandExecutor()),
    ("shutdown-node", "shutdown all nodes, or a node", new ShutdownNodeShellCommandExecutor())
  )

  commands.filter(_._3 != null).foreach(x => x._3.init(Array("rfs", x._1)))

  //mvn exec:java -Dexec.mainClass="cn.bluejoe.regionfs.tool.RegionFSCmd" -Dexec.args="stat" -DskipTests
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

class InitShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("conf")
      .argName("globalConfigFile")
      .desc("conf file path of global setting, e.g conf/global.conf")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    new GlobalConfigConfigurer().config(new File(commandLine.getOptionValue("conf")))
    println("cluster is successfully initialized.");
  }
}

class StatShellCommandExecutor extends ShellCommandExecutor {
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
    admin.stat().nodeStats.foreach { x =>
      println(s"[node-${x.nodeId}](address=${x.address})")
      x.regionStats.foreach { y =>
        println(s"    ╰┈┈┈[region-${y.regionId}](file number=${y.fileCount}, total size=${y.totalSize})")
      }
    }

    admin.close
    FsNodeClient.finalize()
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
    val (nodeId, addr) = admin.greet(commandLine.getOptionValue("node").toInt)
    println(s"greeted node-${nodeId} on ${addr}.")

    admin.close
    FsNodeClient.finalize()
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
    server.startup()
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
    admin.shutdownAllNodes().foreach { x =>
      println(s"shutdowning node-${x._1} on ${x._2}...")
    }
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
    val (nodeId, address) = admin.shutdownNode(commandLine.getOptionValue("node").toInt)
    println(s"shutdowning node-$nodeId on address...")
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
    val addrs = admin.cleanAllData()
    addrs.foreach(addr => println(s"cleaned data on $addr..."))
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
    val addr = admin.cleanNodeData(commandLine.getOptionValue("node").toInt)
    println(s"cleaned data on $addr...")
  }
}