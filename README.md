# regionfs
distributed file system for blob

[![GitHub releases](https://img.shields.io/github/release/bluejoe2008/regionfs.svg)](https://github.com/bluejoe2008/regionfs/releases)
[![GitHub downloads](https://img.shields.io/github/downloads/bluejoe2008/regionfs/total.svg)](https://github.com/bluejoe2008/regionfs/releases)
[![GitHub issues](https://img.shields.io/github/issues/bluejoe2008/regionfs.svg)](https://github.com/bluejoe2008/regionfs/issues)
[![GitHub forks](https://img.shields.io/github/forks/bluejoe2008/regionfs.svg)](https://github.com/bluejoe2008/regionfs/network)
[![GitHub stars](https://img.shields.io/github/stars/bluejoe2008/regionfs.svg)](https://github.com/bluejoe2008/regionfs/stargazers)
[![GitHub license](https://img.shields.io/github/license/bluejoe2008/regionfs.svg)](https://github.com/bluejoe2008/regionfs/blob/master/LICENSE)

```
    ____             _             ___________
   / __ \___  ____ _(_)___  ____  / ____/ ___/
  / /_/ / _ \/ __ `/ / __ \/ __ \/ /_   \__ \
 / _, _/  __/ /_/ / / /_/ / / / / __/  ___/ /
/_/ |_|\___/\__, /_/\____/_/ /_/_/    /____/
           /____/

```

# how to use

NOTE: make sure zookeeper cluster is running, suppose it has connecting string: `localhost:2181,localhost:2182,localhost:2183`

## packaging

```
mvn package
```

this will create `regionfs-<version>.jar` in `./target`

## initializing rfs

1. write a configuration file for rfs global settings `./conf/global.conf`

```
zookeeper.address=localhost:2181
replica.num=1
#region.size.limit=1073741824
blob.crc.enabled=false
```

2. start gloal config setting

```
bin/rfs init -conf ./conf/global.conf
```

## start one node

1. write a configuration file for the node server `./conf/node.conf`

```
zookeeper.address=localhost:2181
server.host=localhost
server.port=1224
data.storeDir=../data/node/
node.id=1
```

NOTE: `node.id` should be unique in cluster

2. start the node server

```
bin/rfs start-local-node -conf ./conf/node.conf
```

## start all nodes

## rfs commands

```
rfs <command> [args]
commands:
	clean-all           clean data on a node
	clean-node          clean data on all nodes
	config              configure global setting
	delete              delete remote files
	get                 get remote files
	greet               notify a node server to print a message to be noticed
	help                print usage information
	put                 put local files into regionfs
	shutdown-all        shutdown all nodes
	shutdown-node       shutdown a node
	start-local-node    start a local node server
	stat-all            report statistics of all nodes
	stat-node           report statistics of a node
```

## using FsClient

add repository in `pom.xml`:

```
<dependency>
  <groupId>org.grapheco</groupId>
  <artifactId>regionfs</artifactId>
  <version>0.9-SNAPSHOT</version>
</dependency>
```

```
  val client = new FsClient("localhost:2181")
  val id = Await.result(client.writeFile(
      new FileInputStream(src), src.length), Duration.Inf)
```

more examples, see https://github.com/bluejoe2008/regionfs/blob/non-kraps/src/test/scala/regionfs/FileReadWriteTest.scala
