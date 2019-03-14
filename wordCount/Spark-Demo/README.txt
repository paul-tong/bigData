Spark Twitter Follower Count

Code author
-----------
Peng Tong

Installation
------------
These components are installed:
- JDK 1.8
- Scala 2.12.8
- Hadoop 2.8.5
- Spark 2.4.0 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/peng/tools/hadoop/hadoop-2.8.5
export SCALA_HOME=/home/peng/tools/scala/scala-2.12.8
export SPARK_HOME=/home/peng/tools/spark/spark-2.4.0-bin-without-hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop (default mode):
	make local
6) AWS EMR Spark: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws			-- check for successful execution with web interface 
	download-output-aws		-- after successful execution & termination
7) Reference: demo given by instructor.
