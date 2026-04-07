FROM eclipse-temurin:11-jre-jammy

ENV SPARK_VERSION=3.3.2
ENV HADOOP_PROFILE=3
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y curl python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE} $SPARK_HOME

RUN curl -fSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -fSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN curl -fSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar

RUN curl -fSL -o /opt/spark/jars/kafka-clients-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar

RUN curl -fSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.2/spark-token-provider-kafka-0-10_2.12-3.3.2.jar


RUN curl -fSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar


RUN pip3 install delta-spark==2.2.0

WORKDIR /opt/spark

ENTRYPOINT ["/bin/bash"]
CMD ["-c", "tail -f /dev/null"]
