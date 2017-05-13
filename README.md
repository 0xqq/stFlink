# stFlink library
Support for spatio-temporal queries over data streams using the Apache Flink Streaming API, Table API and SQL.

# Apache Flink 1.2.0 build instructions

Base Apache Flink version: latest stable release 1.2.0 
[http://www.apache.org/dyn/closer.lua/flink/flink-1.2.0/flink-1.2.0-src.tgz]

Official documentation:
[https://ci.apache.org/projects/flink/flink-docs-release-1.2/setup/building.html]

We recommend using the provided Apache Flink 1.2.0 source code in this repo since it's proven to work well with provided stFlink library implementation

## Build Apache Flink 1.2.0 for Scala 2.11

**Prerequisites:**
* Maven 3.3.x
* Scala 2.11.x
* Java JDK 8

**Notes:**
* By default, Flink builds for Scala 2.10, need to manually change to 2.11
* For Maven 3.3.x build has to be done in two steps: First in the base directory, then in the distribution project.

**Build Steps:** 

* Change scala version to 2.11 - execute the following in the base Flink dir:

`flink-1.2.0> tools/change.scala.version.sh 2.11`

* Start build in the base direktory - specify concrete Scala version as an additional build parameter. Example to build against Scala 2.11.6:

`flink-1.2.0> mvn clean install -DskipTests -Dscala.version=2.11.6`

* Separately build the distribution project:

`flink-1.2.0> cd flink-dist`

`flink-1.2.0/flink-dist> mvn clean install -Dscala.version=2.11.6`

# stFlink Library build instructions

## Code structure

Source code is divided in several packages:

* **hr.fer.stflink.core** - spatio-temporal data types, discrete model implementation
* **hr.fer.stflink.queries** - example queries (Q1-Q5) implemented over different Apache Flink APIs
  * **hr.fer.stflink.queries.streaming_api** - implementation using Apache Flink Streaming API
  * **hr.fer.stflink.queries.table_api** - implementation using Apache Flink Table API
  * **hr.fer.stflink.queries.sql** - implementation using the Apache Flink SQL (under development)

## Running the examples

Following instructions have been tested and are proven to work well on Ubuntu 16.04 Xenial 64bit linux distribution

### 1. Update stFlink library's pom.xml file 

Update stFlink library's pom.xml file and set its mainClass property to the desired query - e.g. to run query Q1 implemented over the Table API:

```
<transformers>
   <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
      <mainClass>**hr.fer.stflink.queries.table_api.Q1**</mainClass>
   </transformer>
</transformers>
```

### 2. Build stFlink library source

Navigate to stFlink's library root folder (`<repo root>/stFlink`) and run the following command:

`mvn clean package -Pbuild-jar`

Resulting .jar file (`stFlink-1.0-SNAPSHOT.jar`) can be found in the `<stFlink root folder>/target` folder

### 3. Run selected query over the GeoLife dataset

Download the dataset (bigdata.txt) from [here](https://drive.google.com/open?id=0B5iQrw8ThlP0MjBVcHhmUUw5YTA) and store it somewhere locally.

**Console 1: Run Apache Flink local instance and wait for the output**

* Navigate to your Apache Flink 1.2.0 installation *bin* folder (`<apache flink 1.2.0 source folder>/build-target/bin`)
* Run Apache Flink 1.2.0 local instance:

`./start-local.sh`

* Query output is logged in Flink's log file, so use the same console to wait for the output:

`tail -f <apache flink 1.2.0 source folder>/build-target/log/flink-<username>-jobmanager-0-ubuntu.out`

**Console 2: Read GeoLife dataset**

* Read GeoLife dataset and redirect it's output to the local socket (127.0.0.1:9999):

`cat bigdata.txt | (sleep 7; while true; do read buf; echo $buf; sleep 0.1; done) | nc -lk 9999'`

We use delay of 7 seconds to give some time for Apache Flink to start the query.

**Console 3: Run selected query**

* Navigate to Apache Flink bin folder (`<apache flink 1.2.0 source folder>/build-target/bin`) and run previously built .jar file:

`flink run <stFlink root folder>/target/stFlink-1.0-SNAPSHOT.jar`

Query results can be vieved in real-time in Console 1.


















