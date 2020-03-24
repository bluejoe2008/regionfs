package org.grapheco.regionfs.tool

import java.io.File

import org.apache.commons.cli._
import org.grapheco.regionfs.server.FsNodeServer

/**
  * Created by bluejoe on 2020/2/25.
  */
object StartLocalNodeServerCommand {
  def main(args: Array[String]) {
    new StartNodeShellCommandExecutor().init(Array("start-node")).parseAndRun(args)
  }
}

private class StartNodeShellCommandExecutor extends ShellCommandExecutor {
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