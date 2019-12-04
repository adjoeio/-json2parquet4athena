
<img src="logo.png" alt="drawing" width="200"/>

## Description
Adjoe's **incremental Json2Parquet4Athena converter** is a SPARK job which is meant to be used for ...
* converting tables batch-wise
    * FROM **date-partitioned JSON** data in S3: 
    
    ```s3://<bucket_name>/.../<table_name>/dt=<year><month><day>```
    
    * TO the same table in **PARQUET format** while keeping the partition structure of the JSON table.
    * Example: the partition ```dt=20191114``` references data that was created at 2019-11-14.

You can use Json2Parquet4Athena together with **Adjoe's Dynalake**, which you can find here: https://github.com/adjoeio/dynalake

*In case you use **Dynalake**, you will automatically fullfill the requirements for this Json2Parquet converter.*

We pay special attention to the following aspects:
* The result of the job can be interpreted by **AWS Athena**. This is because we do **schema normalization**:
    * Lowercase all (also nested) columns and merge conflicting columns.
    * Ensure that all partitions have the same (nested) columns without reading the complete JSON-formatted table completely.
    * Sort the columns (also nested) alphabetically.
    * Use legacy PARQUET format to write the data.
    * Remove duplicates (also from array columns).
       
* The job is meant to be run once daily to **convert new data incrementally**:
    * Only the data which has arrived since the last import will be processed.
    * If the schema changes in a way which requires a complete recalculation (Athena will not be able to read it as is) of old partitions (e.g. new nested columns), it will be performed.
    
* If the job fails (e.g. due to removed AWS spot instances), the next attempt will quickly be able to resume calculation where it failed the last time and also check for erroneous partitions and recalculate those.

## Why would you want to use it?
If your application streams data to S3 in JSON format continously and you want it to be persisted in your data lake in a more efficient format (such as PARQUET), you normally stumble upon the following problems:
* Most of the time, you do not want to convert all tables completely every day. So you want to keep track what was already converted and act accordingly. This will keep costs low. 
* You want to fix schema inconsistencies:
    * the order of (nested) columns change due to application code changes
        * AWS Athena Documentation states: "Schema updates ... do not work on tables with complex or nested data types, such as arrays and structs."
        * https://docs.aws.amazon.com/athena/latest/ug/handling-schema-updates-chapter.html
    * the same column exists in different cases: 
        * ```someCamelCasedStuff``` vs ```somecamelcasedstuff```
        *  Athena is case-insensitive and will find duplicate columns. That will cause an error.
* Sometimes, streaming application data to S3 creates duplicates which should be removed during conversion.

All those points are tackled by **Json2Parquet4Athena**. It helped us reduce costs alot and speed up Athena queries. Hopefully, it does so for you too. :-)


## Requirements
*In case you use **Dynalake** (https://github.com/adjoeio/dynalake), you will automatically fullfill the requirements for this Json2Parquet converter.*
Your JSON tables are located in S3 having the following directory structure (partitioning):
```
s3
└── <bucket_name>
    └── <db_path>
        └── <json_fmt_path>
            └── <table 1>
                ├── dt=00000000
                    └── ...
                ├── dt=20190522
                    └── ...
                ├── dt=20190523
                    └── ...
                ├── dt=20190524
                    └── ...
                └── dt=20190525
                    ├── <some filename>.json.gz
                    └── <another filename>.json.gz
                    └── ...
            ...

            └── <table N>
                ├── dt=00000000
                    └── ...
                ├── dt=20190522
                    └── ...
                ├── dt=20190523
                    └── ...
                ├── dt=20190524
                    └── ...
                └── dt=20190525
                    ├── <some filename>.json.gz
                    └── <another filename>.json.gz
                    └── ...

```


Pay attention that the partitions are per date having the directory format: ```dt=YYYYMMDD```

The result of the job will be put to:
```
s3
└── <bucket_name>
    └── <db_path>
        └── <parquet_fmt_path>
            └── <table 1>
                ├── dt=00000000
                    └── ...
                ├── dt=20190522
                    └── ...
                ├── dt=20190523
                    └── ...
                ├── dt=20190524
                    └── ...
                └── dt=20190525
                    ├── <some filename>.snappy.parquet
                    └── <another filename>.snappy.parquet
                    └── ...
            ...

            └── <table N>
                ├── dt=00000000
                    └── ...
                ├── dt=20190522
                    └── ...
                ├── dt=20190523
                    └── ...
                ├── dt=20190524
                    └── ...
                └── dt=20190525
                    ├── <some filename>.snappy.parquet
                    └── <another filename>.snappy.parquet
                    └── ...

```

## How to build?
Create a fat jar via (being in the root folder of the project):
```
sbt assembly
```

## How to deploy?
Push the fat jar from 
```
<project_root>/target/scala-2.11/AdjoeIncrementalJson2ParquetAthenaImporter-assembly-0.1.jar
``` 
to S3 to the folder ```jar_folder```.

## How to use in AWS EMR?
1) Use the command defined in the ```build.sbt``` to start an EMR SPARK job by using the following sbt command::
    
    
```
sbt "convert2parquet <jar_folder> <bucket_name> <db_path> <json_fmt_path> <parquet_fmt_path> <tmp> <progress_table_name> <region> <cluster_id> <max_nesting_depth>"
```
* ```<jar_folder>``` references the location of the jar. For example: 
```
s3://MyTestBucket/AdjoeIncrementalJson2ParquetAthenaImporter-assembly-0.1.jar
```
* ```<bucket_name>``` references the s3 buckets where the tables are located. This will also be the bucket where temporary data will be stored.
* ```<db_path>``` references the subfolder in ```<bucket_name>``` where your tables are stored.
* ```<json_fmt_path>``` references the subfolder of ```<db_path>``` which actually contains the folders representing the tables directly:
    * e.g. a table Users and OrderItems would be contained in 
        * ```s3://<bucket_name>/<db_path>/<json_fmt_path>/Users```
        * ```s3://<bucket_name>/<db_path>/<json_fmt_path>/OrderItems```
        * For details the the 'Requirements' section.
        
* ```<parquet_fmt_path>```references the output folder which is contained in  ```s3://<bucket_name>/<db_path>```.
    * e.g. it will contain the following folders from the example above after conversion:
        *  ```s3://<bucket_name>/<db_path>/<parquet_fmt_path>/Users```
        *  ```s3://<bucket_name>/<db_path>/<parquet_fmt_path>/OrderItems```
        
* ```<tmp>``` defines the directory in bucket ```<bucket_name>``` where it writes temporary data:  
```
s3://<bucket_name>/<tmp>
```  
* ```<progress_table_name>``` defines the subdir of ```<bucket_name>``` where the progress of the job is maintained
```
s3://<bucket_name>/<progress_table_name>
```  
* The string representing the region of the AWS region, you want to use, is contained in ```region```. 
    * e.g. ```EU_CENTRAL_1```
    
* ```<cluster_id>``` is the id that references the EMR cluster in which the spark program will be executed.
* Finally and with a big impact on runtime performance, you got to define ```<max_nesting_depth>``` as an Integer greater or equal to 0. It will determine how deep the schema normalization process will traverse the nested column structure of any given table. Json2Parquet4Athena will determine if a schema is sufficiently traversed, but in case you got some deeply nested columns, you might want to limit how much time will be spent. Choose it to low and you might still end up with deeply nested schema conflicts that Athena can not handle. Choose it to high and the job might get computationally very expensive depending on the nesting depth of your tables.

## How to use in Spark in general?
Since this is a normal Spark job, you can use ```spark-submit``` to execute it by extracting (and modifying) the **spark-job properties** which you can also find in the ```build.sbt```. Some of the properties you see are essential for the job. Others like memory options should be tweaked for your special use case to reduce cost or guarantee execution stability.
```
val generalStepConfigClientMode = "--deploy-mode,client,--master,yarn,--conf,yarn.scheduler.capacity.maximum-am-resource-percent=0.95,--conf,spark.default.parallelism=100"
val memoryParameters = "--conf,spark.driver.maxResultSize=1G,--conf,spark.executor.extraJavaOptions=-Xss1024m,--driver-java-options,-Xss1024m,--conf,spark.executor.memory=40g,--conf,spark.executor.memoryOverhead=15g,--conf,spark.driver.memory=5g,--conf,spark.driver.memoryOverhead=15g"
val dataHandlingParameters = "--conf,spark.sql.caseSensitive=true,--conf,spark.sql.parquet.mergeSchema=false,--conf,spark.hadoop.fs.s3a.experimental.input.fadvise=normal,--conf,spark.hadoop.fs.s3a.fast.upload=true,--conf,spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem,--conf,spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,--conf,spark.sql.parquet.filterPushdown=true,--conf,spark.speculation=false,--conf,spark.sql.parquet.fs.optimized.committer.optimization-enabled=true"
```

If you have questions, on how to use it. Feel free to ask.


## What is coming up?
Since Athena can handle certain kinds of updates which we currently do not consider like adding new columns in the root of the schema, we are going to not recalculate the complete table when those situations occur. It is important to note, that we use the Athena format "PARQUET: Read by Name" which allows for a wider range of root updates. It has not been test with "Read by Index".