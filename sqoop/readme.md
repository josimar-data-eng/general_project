
## It's a pipeline that Use Sqoop, that's a tool used to transfer data between relational database and HDFS through hadoop jobs. In that case we're going to use CloudSQL as RDBMS and Dataproc Cluster(Storage inside the cluster is a HDFS system) to export data from Cloud-SQL to storage.


Create MySQL on CloudSQL

 - 1 - Instantiate CloudSQL Instance
 - 2 - Creata database
 - 3 - Import data from GCS as a SQL-FILE(File with SQL commands) to Cloud-SQL in order to create and populate tables by reading sql 	commands (It can be a common CSV file as well only with the rows)



## Eval job
 - Usin eval command (tool) that's used to run query and output in the console windows
 - It allows users to execute user-defined queries against respective database servers and preview the result in the console
 
## Import Job Full Load
 - Submit a Hadoop job that uses Sqoop to FULL load from Cloud SQL to Storage as text or Avro (that’s useful for bigquery)
 It uses de default boundary query, using the min() and max() id.

## Import Job Incremental Load
 - Submit a Hadoop job that uses Sqoop to INCREMENTAL load from CloudSQL to Storage as Avro that contains the steps:
	1 - get data from GCS and load it inside the cluster (HDFS - stage files)
	2 -get data from HDFS and load it in GSC (Block storage)

## Import with Boundary
 - Boundary Queries -> specify the range of values we want to import using the primary-key value. It gets the
the max number specified and divided by the map number.
Ex:  - -boundary-query=select 1,10 - m 4. —> It says 10/4, so 2 maps and the rest with more 2 lefts. 
