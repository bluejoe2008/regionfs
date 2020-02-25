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
    ("help", "print usage info", null),
    ("stat", "stat all nodes", new StatShellCommandExecutor()),
    ("node", "start a node server", new StartNodeShellCommandExecutor()),
    ("init", "initialize global setting", new ConfigShellCommandExecutor())
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
    println("rfs <command> [args]")
    println("commands:")
    commands.foreach { en =>
      println(s"\t${en._1}\t${en._2}")
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
      .desc("conf file path of global setting, e.g regionfs.conf")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    new GlobalConfigConfigurer().config(new File(commandLine.getOptionValue("conf")))
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

class StartNodeShellCommandExecutor extends ShellCommandExecutor {
  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("conf")
      .argName("nodeConfigFile")
      .desc("conf file path of local node server, e.g node1.conf")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val server = FsNodeServer.create(new File(commandLine.getOptionValue("conf")))
    server.startup()
  }
}