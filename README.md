# regionfs
dfs for blob

# how to use

NOTE: make sure zookeeper cluster is running, suppose it has connecting str: `localhost:2181,localhost:2182,localhost:2183`

## start configserver

1. prepare a text file
```
#rfs.conf
replicaNums=2
```
2. start configserver
```
bin/start-config-server.sh rfs.conf
```

## start all nodes

1. deploy the regionfs distribution to several nodes, each includes a `conf/node.conf` file like that:
```
#conf/node.conf
zookeeper.address=localhost:2181,localhost:2182,localhost:2183
server.host=localhost
server.port=1224
data.storeDir=./testdata/nodes/node1
node.id=1
```

NOTE: `node.id` should be unique in cluster

2. prepare a text file in one server
```
#nodes
node1
node2
node3
```

3. starts
```
bin/start-all-nodes.sh nodes
```

actually, this script will execute `bin/start-node.sh conf/node.conf` on each node

