# regionfs
distributed file system for blob
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
	greet               notify a node server to print a message to be noticed
	help                print usage information
	init                initialize global setting
	shutdown-all        shutdown all nodes
	shutdown-node       shutdown a node
	start-local-node    start a local node server
	stat-all            report statistics of all nodes
	stat-node           report statistics of a node
```

## using FsClient

```
  val client = new FsClient("localhost:2181")
  val id = Await.result(client.writeFile(
      new FileInputStream(src), src.length), Duration.Inf)
```
