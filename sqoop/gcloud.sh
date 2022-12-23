#CREATE INSTANCE
#find out how use confif file to store password in shell script
gcloud sql instances create mysql-instance \
    --availability-type=regional \
    --database-version=MYSQL_8_0 \
    --enable-bin-log --region=us-central1 \
    --root-password=password 

#CREATE DATABASE
gcloud sql databases create airport --instance=mysql-instance


#IMPORT SQL-FILES FROM GCS TO CLOUD-SQL TO CREATE AND POPULATE TABLES

#1 - CREATE GCS - SEND FILES TO GCS USING -M FLAG TO USE PARALLEL-THREAD
gsutil mb gs://sqoop-mysql
gsutil -m cp sql-files/* gs://sqoop-mysql/sql-files/
gsutil -m cp sqoop-jars/* gs://sqoop-mysql/sql-jars/ #it's to create dataproc cluster
# only 1 object -> gsutil cp sql-files/flights.sql gs://sqoop-mysql/sql-files/

#2 - CREATE AND POPULATE CLOUD-SQL TABLES THROUGH IMPORT FROM GCS

#3 - GRANT OBJECT-ADMIN ROLE TO CLOUD-SQL SERVICE-ACCOUNT IN ORDER TO ACCESS IT AND IMPORT
# describe the instance and get the serviceAccountEmailAddress
gcloud sql instances describe  mysql-instance
gsutil iam ch serviceAccount:{serviceAccountEmailAddress}:objectAdmin gs://sqoop-mysql

#IMPORT AIRPORT TABLE(GOT ERROR THROUGH CLI, SO DID THROUGH CONSOLE)
gcloud sql import sql mysql-instance gs://sqoop-mysql/sq-files/airports.sql --database=airport

#IMPORT FLIGHT TABLE
gcloud sql import sql mysql-instance gs://sqoop-mysql/sql-files/flights.sql --database=airport
gcloud sql import sql mysql-instance gs://sqoop-mysql/sql-files/flights_14_may.sql --database=airport


# CREATE DATAPROC CLUSTER TO ACCESS MY-SQL AND TO THEN RUN HADOOP 
# JOB WITH SQOOP (TOLL THAT TRANFER DATA BETWEEN HDFS AND RDBMS)

#INITIALIZATION ACTION - Getting up-to-date initialization actions from public storage and saving in my bucket
REGION="us-central1"
gsutil cp gs://goog-dataproc-initialization-actions-${REGION}/sqoop/sqoop.sh gs://sqoop-mysql/dataproc/initialization-actions/sqoop/sqoop.sh
gsutil cp gs://goog-dataproc-initialization-actions-${REGION}/hive-hcatalog/hive-hcatalog.sh gs://sqoop-mysql/dataproc/initialization-actions/hive-hcatalog/
gsutil cp gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh gs://sqoop-mysql/dataproc/initialization-actions/cloud-sql-proxy/


#CREATE DATAPROC CLUSTER

#VARIABLES
PROJECT_ID="dev-project-363923"
WAREHOUSE_BUCKET="sqoop-mysql"
REGION="us-central1"
CLUSTER_NAME="sqoop-job-cluster"
# INSTANCE_NAME="mysql-instance"
PWD_FILE="gs://sqoop-mysql/pass_files/mysql.txt"
INSTANCE_NAME="dev-project-363923:us-central1:mysql-instance"

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --scopes sql-admin \
    --region ${REGION} \
    --initialization-actions gs://sqoop-mysql/dataproc/initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh,gs://sqoop-mysql/dataproc/initialization-actions/hive-hcatalog/hive-hcatalog.sh,gs://sqoop-mysql/dataproc/initialization-actions/sqoop/sqoop.sh \
    --properties "hive:hive.metastore.warehouse.dir=gs://${WAREHOUSE_BUCKET}/datasets" \
    --metadata "hive-metastore-instance=${PROJECT}:${REGION}:hive-metastore" \
    --metadata "enable-cloud-sql-hive-metastore=false" \
    --metadata "enable-cloud-sql-proxy-on-workers=false" \
    --metadata=additional-cloud-sql-instances=${INSTANCE_NAME}=tcp:3307 #add the sql-instance we're going to access    


# HADOOP JOB THAT USE SQOOP TO RUN A SIMPLE QUERY IN CLOUD-SQL AND OUTPUT THE RESULTS AT THE SCREEN (LIKE CONNECT IN MYSQL)
 # Usin eval command (tool) that's used to run query and output in the console windows
 # eval: It allows users to execute user-defined queries against respective database servers and preview the result in the console
gcloud dataproc jobs submit hadoop \
--cluster=$CLUSTER_NAME --region="us-central1" \
--class=org.apache.sqoop.Sqoop \
--jars=gs://sqoop-mysql/sql-jars/sqoop_avro-tools-1.8.2.jar,gs://sqoop-mysql/sql-jars/sqoop_sqoop-1.4.7.jar,file://usr/share/java/mysql-connector-j-8.0.31.jar \
-- eval \
-Dmapreduce.job.user.classpath.first=true \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file="gs://sqoop-mysql/pass_files/mysql.txt" \
--query "select * from flights limit 10"



# HADOOP JOB THAT USE SQOOP RO TUN A IMPORT COMMAND TO LOAD FULL TABLE FROM MY-SQL TO GCS TROUGH DATAPROC CLUSTER (DEFAULT BAOUNDERY IS SETTED AS MIN AND MAX)
# It uses de default boundary query, using the min() and max() id.
table_name="flights"
bucket="gs://sqoop-mysql"
target_dir=$bucket/sqoop_output
cluster_name="sqoop-job-cluster"
pwd_file="gs://sqoop-mysql/pass_files/mysql.txt"

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region="us-central1" \
--class=org.apache.sqoop.Sqoop \
--jars=gs://sqoop-mysql/sql-jars/sqoop_avro-tools-1.8.2.jar,gs://sqoop-mysql/sql-jars/sqoop_sqoop-1.4.7.jar,file://usr/share/java/mysql-connector-j-8.0.31.jar \
-- import \
-Dmapreduce.job.classloader=true \
--driver com.mysql.cj.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--split-by id \
--table=$table_name \
-m 4 \
--target-dir=$target_dir --as-avrodatafile



# HADOOP JOB MASKING INCREMENTAL LOAD
#  Sqoop to INCREMENTAL load from CloudSQL to Storage as Avro that contains the steps:
#	1 - get data from GCS to Hive Warehouse - storage inside the cluster - (HDFS - stage files - Master node)
#	2 - get data from HDFS and load it in GSC (Block storage)
#Using check-column, last-value, incremental flags, and different target_dir variable.

table_name="flights"
target_dir="/incremental_files"
cluster_name="sqoop-job-cluster"
pwd_file="gs://sqoop-mysql/pass_files/mysql.txt"

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region="us-central1" \
--class=org.apache.sqoop.Sqoop \
--jars=gs://sqoop-mysql/sql-jars/sqoop_avro-tools-1.8.2.jar,gs://sqoop-mysql/sql-jars/sqoop_sqoop-1.4.7.jar,file://usr/share/java/mysql-connector-j-8.0.31.jar \
-- import \
-Dmapreduce.job.user.classpath.firts=true \
--driver com.mysql.cj.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--split-by id \
--table=$table_name \
--check-column $table_name
--last-value 2019-05-13
--incremental append
-m 4 \
--target-dir=$target_dir --as-avrodatafile


# Commando to log in master node and send files to storage
gcloud compute ssh $cluster_name \
--zone="us-central1-c" \
-- T 'hadoop distcp /incremental_files/*.avro gs://sqoop-mysql/incremental'
 


# HADOOP WITH BOUNDARY QUERIES
#Boundary Queries -> specify the range of values we want to import using the primary-key value. It gets the
#the max number specified and divided by the map number.
#Ex:  - -boundary-query=select 1,10 - m 4. —> It says 10/4, so 2 maps and the rest with more 2 lefts. 
# Simple table import - Avro Format - Boundary Query 

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region=us-central1 \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop_jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop_jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.42.jar \
-- import \
-Dmapreduce.job.classloader=true \
-Dmapreduce.output.basename="part.20190514_" \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--split-by id \
--table $table_name \
--boundary-query "select 1,190751" \
-m 4 \
--warehouse-dir $target_dir --as-avrodatafile