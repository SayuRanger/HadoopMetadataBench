# Steps to run experiment

As an illustration, we summarize the steps to run extrapolated
TeraSort benchmark against Hadoop NameNode and ResourceManager.

Folder script/ includes all useful scripts and here is the list of scripts
that need to be launched in order to run experiment.

## Installing

Create experiment directories and build hadoop 2.7.3 on one node,
and deploy jar files to other nodes.

```
build-hadoop.sh
```

Build emulator package.
    
```
build-emulator.sh
```

## Running the tests

### Launch the experiment

```
run.sh ${workload}
```
   
Supported workload (currently):
   
- nn-terasort-dn100-c1
     
    NameNode TeraSort of 100 DataNodes, 1 container on each DataNode
   
- nn-terasort-dn50-c8
     
    NameNode TeraSort of 50 DataNodes, 8 containers on each DataNode

- rm-terasort-dn100-c1
     
    ResourceManager TeraSort of 100 DataNodes, 1 container on each DataNode

### Gather logs

Create **`tmp-log`** directory under current folder, and gather running
logs from all machines

```
collect-log.sh
```

### Calculate statistics

The *process-nn.py* calculates statistics including
throughput, average latency of each type of RPC, etc.

```
log-process/process-nn.py tmp-log
```

---
**Notes**

1. Make sure **JAVA\_HOME** is set to the path of Java library.

2. Other scripts will be used inside the above scripts.
   The most important script is *set-env.sh*, which sets all
   environment variables. The following variables
   need to be set correctly based on user environment:

    **`LOCAL`**: path to local directory for the experiment
     
    **`HADOOP_SRC_HOME`**: path to hadoop 2.7.3 source code
       
    **`HADOOP_MASTER_NODE`**: machine to launch Hadoop
    NameNode/ResourceManager

3. Hadoop environment setting

    Similar to running general MapReduce benchmarks on real systems,
    we have to configure Hadoop environment.
    For example, when testing ResourceManager, we need to adjust parameters
    related to CPU and memory to set expected number of containers
    on each NodeManager within **yarn-site.xml**.
    For configuration suggestion, please refer to doc/hadoop-config.md.

4. Hadoop source code
    
    In ResourceManager (RM) experiment, besides working as the
    RPC server, RM also send RPC requests to NodeManagers to
    start ApplicationMaster. Instead of launching RPC server on
    each NodeManager, we rewrite the **`AMLauncher.java`** as
    **`TCPAMLauncher.java`** to send these requests as TCP messages,
    in order to reduce overhead of NodeManager stubs.
    (under
hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/amlauncher).
    
    Hint: if you want to modify/add the source code,
    make sure to change pom.xml if needed.