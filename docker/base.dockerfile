FROM ghcr.io/skiptests/astraea/spark:3.1.2

# install python dependencies
RUN pip3 install confluent-kafka==1.7.0 delta-spark==1.0.0 pyspark==3.1.2

# install java dependencies
WORKDIR /tmp
RUN wget https://dlcdn.apache.org//ant/ivy/2.5.0/apache-ivy-2.5.0-bin.zip
RUN unzip apache-ivy-2.5.0-bin.zip
WORKDIR apache-ivy-2.5.0
RUN java -jar ./ivy-2.5.0.jar -dependency io.delta delta-core_2.12 1.0.0
RUN java -jar ./ivy-2.5.0.jar -dependency org.apache.spark spark-sql-kafka-0-10_2.12 3.1.2
RUN java -jar ./ivy-2.5.0.jar -dependency org.apache.spark spark-token-provider-kafka-0-10_2.12 3.1.2
# 3.2.2 fixes the NEP (see https://issues.apache.org/jira/browse/HADOOP-16410)
RUN java -jar ./ivy-2.5.0.jar -dependency org.apache.hadoop hadoop-azure 3.2.2

# move back to spark home
WORKDIR /opt/spark