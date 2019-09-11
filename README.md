# regionfs
dfs for blob

# how to use

NOTE: make sure zookeeper cluster is running, suppose it has connecting str: localhost:2181,localhost:2182,localhost:2183

## start configserver

1. make a text file
```
#rfs.conf
replicaNums=2
```
2. start configserver
```
bin/start-config-server.sh rfs.conf
```

## start all nodes

1. make a text file
```
#nodes
node1
node2
node3
```

2. make sure each node owns a regionfs distribution with a conf file like that:
```
zookeeper.address=localhost:2181,localhost:2182,localhost:2183
server.host=localhost
server.port=1224
data.storeDir=./testdata/nodes/node1
node.id=1
```

in which, `node.id` is an unique id for current node server

3. starts
```
bin/start-all-nodes.sh nodes
```
