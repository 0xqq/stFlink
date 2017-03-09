# stFlink operations
Implementing support for spatio-temporal operations using the Apache Flink Table API 

# Build Instructions

Base Apache Flink version: latest stable release 1.2.0 
[http://www.apache.org/dyn/closer.lua/flink/flink-1.2.0/flink-1.2.0-src.tgz]

Official documentation:
[https://ci.apache.org/projects/flink/flink-docs-release-1.2/setup/building.html]

## Build for Scala 2.11

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
