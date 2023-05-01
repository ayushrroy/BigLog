# BigLog

Framework/Tool that monitors server logs and identifies anomalous behavior. The sections that follow set up the dev environment for the project. These are for Mac but should roughly be the same for Windows (`WSL2`) and Linux.

## Table of Contents

* [Installing Java](#installing-java)
* [Installing Scala](#installing-scala)
* [Installing Spark](#installing-spark)
* [Installing Kafka](#installing-kafka)
* [Start Zookeeper](#start-zookeeper)
* [Start Kafka](#start-kafka)
* [Create a Kafka topic](#create-a-kafka-topic)
* [Start the Producer](#start-the-producer)
* [Verifying logs on the Consumer](#verifying-logs-on-the-consumer)
* [Creating an SBT project in IntelliJ](#creating-an-sbt-project-in-intellij)
* [Example Stream](#example-stream)
* [Example Stream With ML](#example-stream-with-ml)

## Requirements

1. Java 11
2. Scala
3. Apache Kafka (and Zookeeper)
4. Apache Spark
5. IntelliJ

### Installing Java

```bash
❯ brew install openjdk@11
```

```bash
❯ java -version
openjdk version "11.0.11" 2021-04-20
OpenJDK Runtime Environment AdoptOpenJDK-11.0.11+9 (build 11.0.11+9)
OpenJDK 64-Bit Server VM AdoptOpenJDK-11.0.11+9 (build 11.0.11+9, mixed mode)
```

### Installing Scala

```bash
❯ brew install scala
```

```bash
❯ scala -version
Scala code runner version 3.2.2 -- Copyright 2002-2023, LAMP/EPFL
```

### Installing Spark

```bash
❯ brew install apache-spark
```

This will by default install Spark in `client` mode, instead of `cluster`, which is what we were using in `docker`.

```bash
❯ spark-shell
Spark context Web UI available at http://10.0.0.113:4040
Spark context available as 'sc' (master = local[*], app id = local-1682092096819).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/

Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.11)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```
> __Note__: Homebrew takes care of putting the Spark binaries in your `PATH`. You will have to locate them on Windows and Linux before running the above command.

### Installing Kafka

```bash
❯ brew install kafka
```

### Start Zookeeper

```bash
❯ zookeeper-server-start /usr/local/etc/zookeeper/zoo.cfg
```
> __Note__: Homebrew takes care of putting the Zookeeper binaries in your `PATH`. You will have to locate them on Windows and Linux before running the above command.

### Start Kafka

```bash
❯ kafka-server-start /usr/local/etc/kafka/server.properties
```
> __Note__: Homebrew takes care of putting the Kafka binaries in your `PATH`. You will have to locate them on Windows and Linux before running the above command.

### Create a Kafka topic

```bash
❯ kafka-topics --create --bootstrap-server localhost:9092 --topic log_topic
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic log_topic.
```

### Start the Producer

```bash
❯ python3 producer.py
```

### Verifying logs on the Consumer

```bash
❯ kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic log_topic
```

### Creating an SBT project in IntelliJ

1. Use the steps in [this link](https://docs.scala-lang.org/getting-started/intellij-track/building-a-scala-project-with-intellij-and-sbt.html) to create an SBT-based Scala project in IntelliJ. Feel free to use Maven instead if you are feeling adventurous.
2. Once created, add the `libraryDependencies` from the `build.sbt` file in this branch to your project's `build.sbt`.
3. Rename `Main.scala` in your project to `BigLog.scala`.
4. Replace the content of your project's `BigLog.scala` with the one in this branch.
5. Build the project. This should take a few seconds as SBT downloads the `spark-sql`, `spark-sql-kafka`, `spark-ml` dependencies.
6. If the build completes without any errors, run the project. After a few Spark initialization messages, you should start seeing the streamed DataFrames.
7. The run should terminate with an exit code of 0 after about 20 seconds. `Thread.sleep(20000)` controls this behaviour and stops the query after 20 seconds.
8. If you would like to see a continuous stream, use `.awaitTermination()` after `.start()`.

### Example Stream

Testing Spark-Kafka communication with `BigLog`.

```bash
-------------------------------------------
Batch: 0
-------------------------------------------
+------+-------------------+-----+--------------------+--------------------+
|LineId|          DateTime |Level|           Component|             Content|
+------+-------------------+-----+--------------------+--------------------+
|     1|2017-06-09 20:10:40| INFO|executor.CoarseGr...|Registered signal...|
|     2|2017-06-09 20:10:40| INFO|spark.SecurityMan...|Changing view acl...|
|     3|2017-06-09 20:10:40| INFO|spark.SecurityMan...|Changing modify a...|
|     4|2017-06-09 20:10:40| INFO|spark.SecurityMan...|SecurityManager: ...|
|     5|2017-06-09 20:10:41| INFO|spark.SecurityMan...|Changing view acl...|
|     6|2017-06-09 20:10:41| INFO|spark.SecurityMan...|Changing modify a...|
|     7|2017-06-09 20:10:41| INFO|spark.SecurityMan...|SecurityManager: ...|
|     8|2017-06-09 20:10:41| INFO|   slf4j.Slf4jLogger|Slf4jLogger start...|
|     9|2017-06-09 20:10:41| INFO|            Remoting|Starting remoting\n |
|    10|2017-06-09 20:10:41| INFO|            Remoting|Remoting started;...|
|    11|2017-06-09 20:10:41| INFO|          util.Utils|Successfully star...|
|    12|2017-06-09 20:10:41| INFO|storage.DiskBlock...|Created local dir...|
|    13|2017-06-09 20:10:41| INFO| storage.MemoryStore|MemoryStore start...|
|    14|2017-06-09 20:10:42| INFO|executor.CoarseGr...|Connecting to dri...|
|    15|2017-06-09 20:10:42| INFO|executor.CoarseGr...|Successfully regi...|
|    16|2017-06-09 20:10:42| INFO|   executor.Executor|Starting executor...|
|    17|2017-06-09 20:10:42| INFO|          util.Utils|Successfully star...|
|    18|2017-06-09 20:10:42| INFO|netty.NettyBlockT...|Server created on...|
|    19|2017-06-09 20:10:42| INFO|storage.BlockMana...|Trying to registe...|
|    20|2017-06-09 20:10:42| INFO|storage.BlockMana...|Registered BlockM...|
+------+-------------------+-----+--------------------+--------------------+
only showing top 20 rows
```

### Example Stream With ML

The Spark cluster application logs for this piece of the project come [from here](https://zenodo.org/record/3227177#.ZE7plS3ML0o). Please extract the `tar` and place it in the `data` dir before running `SimpleAnomalyDetectionML`.

```bash
-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+-----+----+--------------------+-------------------+-------------------+--------+-------+----------------+
|              Window|Stage|Task|          Timestamps|          StartTime|            EndTime|Duration|Outlier|PredictedOutlier|
+--------------------+-----+----+--------------------+-------------------+-------------------+--------+-------+----------------+
|{2017-06-09 17:08...|679.0|10.0|[2017-06-09 17:08...|2017-06-09 17:08:58|2017-06-09 17:08:59|     1.0|   true|            true|
|{2017-06-09 17:04...|267.0|29.0|[2017-06-09 17:04...|2017-06-09 17:04:37|2017-06-09 17:04:37|     0.0|  false|           false|
|{2017-06-09 17:04...|216.0|13.0|[2017-06-09 17:04...|2017-06-09 17:04:31|2017-06-09 17:04:31|     0.0|  false|           false|
|{2017-06-09 17:04...| 99.0|21.0|[2017-06-09 17:04...|2017-06-09 17:04:13|2017-06-09 17:04:13|     0.0|  false|           false|
|{2017-06-09 17:09...|814.0|34.0|[2017-06-09 17:09...|2017-06-09 17:09:14|2017-06-09 17:09:14|     0.0|  false|           false|
|{2017-06-09 17:04...|170.0|13.0|[2017-06-09 17:04...|2017-06-09 17:04:25|2017-06-09 17:04:25|     0.0|  false|           false|
|{2017-06-09 17:04...| 57.0|37.0|[2017-06-09 17:04...|2017-06-09 17:04:08|2017-06-09 17:04:08|     0.0|  false|           false|
|{2017-06-09 17:09...|691.0|34.0|[2017-06-09 17:09...|2017-06-09 17:09:00|2017-06-09 17:09:00|     0.0|  false|           false|
|{2017-06-09 17:09...|806.0|26.0|[2017-06-09 17:09...|2017-06-09 17:09:13|2017-06-09 17:09:13|     0.0|  false|           false|
|{2017-06-09 17:04...| 13.0|13.0|[2017-06-09 17:04...|2017-06-09 17:04:01|2017-06-09 17:04:01|     0.0|  false|           false|
|{2017-06-09 17:08...|597.0| 2.0|[2017-06-09 17:08...|2017-06-09 17:08:49|2017-06-09 17:08:49|     0.0|  false|           false|
|{2017-06-09 17:09...|730.0|18.0|[2017-06-09 17:09...|2017-06-09 17:09:04|2017-06-09 17:09:04|     0.0|  false|           false|
|{2017-06-09 17:09...|813.0|26.0|[2017-06-09 17:09...|2017-06-09 17:09:13|2017-06-09 17:09:14|     1.0|   true|            true|
|{2017-06-09 17:08...|487.0|18.0|[2017-06-09 17:08...|2017-06-09 17:08:36|2017-06-09 17:08:36|     0.0|  false|           false|
|{2017-06-09 17:04...|350.0|29.0|[2017-06-09 17:04...|2017-06-09 17:04:46|2017-06-09 17:04:47|     1.0|   true|            true|
|{2017-06-09 17:05...|413.0| 5.0|[2017-06-09 17:05...|2017-06-09 17:05:02|2017-06-09 17:05:02|     0.0|  false|           false|
|{2017-06-09 17:05...|417.0|29.0|[2017-06-09 17:05...|2017-06-09 17:05:03|2017-06-09 17:05:03|     0.0|  false|           false|
|{2017-06-09 17:04...| 79.0| 5.0|[2017-06-09 17:04...|2017-06-09 17:04:11|2017-06-09 17:04:11|     0.0|  false|           false|
|{2017-06-09 17:04...|236.0|29.0|[2017-06-09 17:04...|2017-06-09 17:04:33|2017-06-09 17:04:33|     0.0|  false|           false|
|{2017-06-09 17:04...|142.0|29.0|[2017-06-09 17:04...|2017-06-09 17:04:20|2017-06-09 17:04:20|     0.0|  false|           false|
+--------------------+-----+----+--------------------+-------------------+-------------------+--------+-------+----------------+
only showing top 20 rows
```

## Useful Links

- [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Getting Started With Scala](https://docs.scala-lang.org/getting-started/index.html)
- [Building a Scala Project With IntelliJ And SBT](https://docs.scala-lang.org/getting-started/intellij-track/building-a-scala-project-with-intellij-and-sbt.html)

### Authors

Sarthak Banerjee, Ajinkya Fotedar, James Hinton, Vinayak Kumar, Sydney May, Ayush Roy

### Versions

- v0 (4/11/23), Producer
- v1 (4/26/23), Pipelines
- v2 (4/30/23), ML Integration
