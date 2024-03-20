# spark_project

change to spark on Yarn, code for Spark submit:
$SPARK_HOME/bin/spark-submit \
--class batch.LogApp \
--master yarn \
--name LogApp \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--jars $(echo $HBASE_HOME/lib/*.jar | tr ' ' ',') \
/home/hadoop/lib/spark.jar \
20190130


Code used in SQL:
create database spark;
use spark;
create table if not exists browser_stat (
day varchar(10) not null,
browser varchar(100) not null,
cnt int
) engine=innodb default charset=utf8;