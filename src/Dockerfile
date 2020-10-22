FROM python:3.7-stretch

RUN apt-get update -y && apt-get install -y libzbar-dev bash gcc git libc-dev curl wget vim nano \
    && apt-get install -y ranger caca-utils highlight atool w3m poppler-utils mediainfo \
    && apt-get install -y iputils-ping telnet openssh-client net-tools \
    man unzip vim-tiny bc openssh-server thrift-compiler netcat sudo \
    && apt-get install -y build-essential \
    && apt-get autoremove -y \
    && apt-get clean

# Install OpenJDK 8
RUN \
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# install scala
RUN wget --no-verbose https://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.tgz
RUN tar -xzvf scala-2.11.8.tgz
RUN mv scala-2.11.8 /opt/scala

RUN rm -rf scala-2.11.8.tgz

ENV SCALA_HOME=/opt/scala
ENV PATH=$PATH:$SCALA_HOME/bin

#Install hadoop
RUN wget https://downloads.apache.org/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz
RUN tar -xzvf hadoop-3.2.1.tar.gz
RUN mv hadoop-3.2.1 /opt/hadoop
RUN rm -rf hadoop-3.2.1.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV CLASSPATH=$HADOOP_HOME/lib:$CLASSPATH
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV HADOOP_OPTS="-Djava.library.path=$HADOOP_COMMON_LIB_NATIVE_DIR"
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native


# install spark
RUN wget --no-verbose https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
RUN tar -xzvf spark-2.4.7-bin-hadoop2.7.tgz
RUN mv spark-2.4.7-bin-hadoop2.7 /opt/spark
RUN rm -rf spark-2.4.7-bin-hadoop2.7.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin

# download maven
RUN wget --no-verbose https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.2.5/apache-maven-3.2.5-bin.tar.gz
RUN tar -xzvf apache-maven-3.2.5-bin.tar.gz
RUN mv apache-maven-3.2.5 /opt/maven
RUN rm -rf apache-maven-3.2.5-bin.tar.gz

ENV M2_HOME=/opt/maven
ENV PATH=$PATH:$M2_HOME/bin

ENV MAVEN_HOME=/opt/maven
ENV PATH=$PATH:$MAVEN_HOME/bin
RUN mvn --version

#install sbt
RUN wget --no-verbose https://dl.bintray.com/sbt/native-packages/sbt/0.13.13/sbt-0.13.13.tgz
RUN tar -xzvf sbt-0.13.13.tgz
RUN mv sbt-launcher-packaging-0.13.13 /opt/sbt
RUN rm -rf sbt-0.13.13.tgz

ENV SBT_HOME=/opt/sbt
ENV PATH=$PATH:$SBT_HOME/bin

# RUN sbt sbtVersion

RUN pip install pyspark
# ENV PYSPARK_PYTHON=/usr/bin/python3

RUN mkdir -p /opt/spark/history
COPY src/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH

#Workiingg
COPY src/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/project
COPY ./src/main ./opt/project/app

COPY src/run.sh /run.sh
RUN chmod a+rwx /run.sh

COPY src/jupyter_run.sh /jupyter_run.sh
RUN chmod a+rwx /jupyter_run.sh

WORKDIR /opt/project