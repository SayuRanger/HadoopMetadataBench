## Tips for Hadoop environment configuration

### core-site.xml

**`fs.defaultFS`**: hdfs://${HADOOP\_MASTER\_NODE}:9000

**`hadoop.tmp.dir`**: path of ${HADOOP\_TMP\_DIR}

### hdfs-site.xml

**`dfs.namenode.rpc-address`**: ${HADOOP\_MASTER\_NODE}:9000

**`dfs.namenode.handler.count`**: 200, we set larger value here for large scale experiment.

Reference: https://community.hortonworks.com/articles/43839/scaling-the-hdfs-namenode-part-2.html

### yarn-site.xml

**`yarn.resourcemanager.hostname`**: set hostname of ResourceManager, 
in our experiment, we use **`node220-3`**.
For all parameters that contains **`node220-3`**, please change to
hostname based on your environment.

**`yarn.resourcemanager.tcpamlaucher.address`**:
change **`10.0.0.14`** to the IP address of the node that run
central controller of the simulator.
  
**`yarn.nodemanager.resource.memory-mb`**,
**`yarn.nodemanager.resource.cpu-vcores`**,
**`yarn.scheduler.minimum-allocation-mb`**,
**`yarn.scheduler.maximum-allocation-mb`**:
adjust these values to set the maximum number of containers
supported on each node.

(**1c-yarn-site.xml**, **8c-yarn-site.xml**, **32c-yarn-site.xml** are examples to set **1, 8, 32** containers)

---
**Notes**:

Replace environment variable with actual string.
(e.g. **${HADOOP\_MASTER\_NODE}** is **`node220-3`** in our environment)
