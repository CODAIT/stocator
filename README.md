Stocator - Storage Connector for Apache Spark
==============================
Apache Spark can work with multiple data sources that include object stores like Amazon S3, OpenStack Swift, IBM SoftLayer, and more. To access an object store, Spark uses Hadoop modules that contain drivers to the various object stores. 

Spark needs only small set of object store functionalities. Specifically, Spark requires object listing, objects creation, read objects, and getting data partitions. Hadoop drivers, however, must be compliant with the Hadoop eco system. This means they support many more operations, such as shell operations on directories, including move, copy, rename, etc. (these are not native object store operations). Moreover, Hadoop Map Reduce Client is designed to work with file systems and not object stores. The temp files and folders it uses for every write operation are renamed, copied, and deleted. This leads to dozens of useless requests targeted at the object store. It’s clear that Hadoop is designed to work with file systems and not object stores.

Stocator is implicitly designed for the object stores, it has very a different architecture from the existing Hadoop driver. It doesn’t depends on robust Hadoop modules and interacts directly with object stores. 

Stocator is a generic connector, that may contain various implementations for object stores. It initially provided with complete Swift driver, based on the JOSS package. Stocator can be very easily extended with more object store implementations, like support for Amazon S3.

## Major features
* Implements HDFS interface
* No need to change or recompile Spark
* Doesn’t create any temporary folders or files for write operations. Each Spark's task generates only one object in the object store
* There are no notions of directories (usually defined as empty files)
* Object's name may contain "/"
* Containers / buckets are automatically created
* (Swift Driver) Uses streaming for object uploads, without knowing object size. This is unique to OpenStack Swift
* (Swift Driver) Tested to read large objects (over 16GB)

## Build procedure

Checkout the Stocator source `https://github.com/SparkTC/stocator.git`

### JOSS and Keystone V3 API
Swift driver uses JOSS to access OpenStack Swift. Currently JOSS supports Keystone V2 and Swift auth models. 
Some object store providers expose Keystone V3 API (for example IBM Bluemix Object Store). The patch extending JOSS with Keystone V3 support exists, but is not yet merged into JOSS.
Below are instructions how to install JOSS locally and compile with Keystone V3 support

* git clone https://github.com/javaswift/joss
* cd joss
* copy `keystone-v3-0.9.10-0001.patch` from `../stocator/additions/joss/patch` to `joss`
* git apply keystone-v3-0.9.10-0001.patch
* mvn clean install

This will compile JOSS and install it into local maven repository. 
You are now ready to compile Stocator.

### How to build Stocator
* Change directory to `stocator`
* Execute `mvn install`

## Usage with Apache Spark
New Swift's driver can be accessed via new schema `swift2d://`.
The configuration template located under `conf/core-site.xml.template`.

### Reference the new driver in the core-site.xml
Add driver reference in the `conf/core-site.xml` of Spark

	<property>
   		<name>fs.swift2d.impl</name>
   		<value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
	</property>

#### Swift Driver configuration
The following is the list of the configuration keys.

| Key | Info | Default value |
| --- | ------------ | ------------- |
|fs.swift2d.service.lvm.auth.url | Mandatory |
|fs.swift2d.service.lvm.public | Optional. Values: true, false | false | 
|fs.swift2d.service.lvm.tenant | Mandatory |
|fs.swift2d.service.lvm.password |  Mandatory |
|fs.swift2d.service.lvm.username | Mandatory |
|fs.swift2d.service.lvm.block.size | Block size in MB | 128MB |
|fs.swift2d.service.lvm.region | Mandatory for Keystone V3 | dallas
|fs.swift2d.service.lvm.auth.method | Optional. Values: keystone, swiftauth, keystoneV3| keystoneV3

Below is the internal Keystone V3 mapping

| Driver configuration key | Keystone V3 key |
| ------------------------ | --------------- |
| fs.swift2d.service.lvm.username | user id |
| fs.swift2d.service.lvm.tenant | project id |

#### Example of core-site.xml keys
##### Keystone V2

	<property>
    	<name>fs.swift2d.service.lvm.auth.url</name>
    	<value>http://IP:PORT/v2.0/tokens</value>
	</property>
	<property>
    	<name>fs.swift2d.service.lvm.public</name>
    	<value>true</value>
	</property>
	<property>
    	<name>fs.swift2d.service.lvm.tenant</name>
    	<value>TENANT</value>
	</property>
	<property>
    	<name>fs.swift2d.service.lvm.password</name>
    	<value>PASSWORD</value>
	</property>
	<property>
    	<name>fs.swift2d.service.lvm.username</name>
    	<value>USERNAME</value>
	</property>
	<property>
    	<name>fs.swift2d.service.lvm.auth.method</name>
    	<value>keystone</value>
	</property>
	
##### SoftLayer Dallas 05

	<property>
    	<name>fs.swift2d.service.dal05.auth.url</name>
    	<value>https://dal05.objectstorage.softlayer.net/auth/v1.0/</value>
	</property>
	<property>
    	<name>fs.swift2d.service.dal05.public</name>
    	<value>true</value>
	</property>
	<property>
    	<name>fs.swift2d.service.dal05.tenant</name>
    	<value>TENANT</value>
	</property>
	<property>
    	<name>fs.swift2d.service.dal05.password</name>
    	<value>API KEY</value>
	</property>
	<property>
    	<name>fs.swift2d.service.dal05.username</name>
    	<value>USERNAME</value>
	</property>
	<property>
    	<name>fs.swift2d.service.dal05.auth.method</name>
    	<value>swiftauth</value>
	</property>
	
##### IBM Bluemix Object Service using Keystone V3

	<property>
    	<name>fs.swift2d.service.bmv3.auth.url</name>
    	<value>https://identity.open.softlayer.com/v3/auth/tokens</value>
	</property>
	<property>
    	<name>fs.swift2d.service.bmv3.public</name>
    	<value>true</value>
	</property>
	<property>
	    <name>fs.swift2d.service.bmv3.tenant</name>
	    <value>PROJECTID</value>
	</property>
	<property>
	    <name>fs.swift2d.service.bmv3.password</name>
	    <value>PASSWORD</value>
	</property>
	<property>
	    <name>fs.swift2d.service.bmv3.username</name>
	    <value>USERID</value>
	</property>
	<property>
	    <name>fs.swift2d.service.bmv3.auth.method</name>
	    <value>keystoneV3</value>
	</property>
	<property>
	    <name>fs.swift2d.service.bmv3.region</name>
	    <value>dallas</value>
	</property>


## Execution without compiling Spark
It is possible to execute Spark with the new driver, without compiling Spark.
Directory `stocator/target` contains standalone jar `stocator-1.0.0-jar-with-dependencies.jar`.
 
Run Spark with 

	./bin/spark-shell --jars stocator-1.0.0-jar-with-dependencies.jar
## Execution with Spark compilation

### Configure maven build in Spark
Both main `pom.xml` and `core/pom.xml` should be modified.

 main pom.xml

     <dependency>
          <groupId>com.ibm.stocator</groupId>
          <artifactId>stocator</artifactId>
          <version>1.0.0</version>
          <scope>${hadoop.deps.scope}</scope>
      </dependency>

 core/pom.xml

    <dependency>
          <groupId>com.ibm.stocator</groupId>
          <artifactId>stocator</artifactId>
     </dependency>

	
	
### Compile Spark
Compile Spark with Haddop support 

	mvn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests package

	
### Examples
#### Create new object in Swift.

	val data = Array(1, 2, 3, 4, 5, 6, 7, 8)
	val distData = sc.parallelize(data)
	distData.saveAsTextFile("swift2d://newcontainer.SERVICENAME/one1.txt")

Listing container `newcontainer` will display

	one1.txt
	one1.txt/_SUCCESS
	one1.txt/part-00000
	one1.txt/part-00001
	one1.txt/part-00002
	one1.txt/part-00003
	one1.txt/part-00004
	one1.txt/part-00005
	one1.txt/part-00006
	one1.txt/part-00007

### Running Terasort

Follow 

	https://github.com/ehiggs/spark-terasort
	
You can run Terasort as follows
Step 1:
	
	export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

Step 2:

	./bin/spark-submit --driver-memory 2g --class com.github.ehiggs.spark.terasort.TeraGen /spark-terasort/target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 1g swift2d://teradata.lvm/terasort_in
	
Step 3:

	./bin/spark-submit --driver-memory 2g --class com.github.ehiggs.spark.terasort.TeraSort /target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 1g swift2d://teradata.lvm/terasort_in swift2d://teradata.lvm/terasort_out  
	
Step 4:

	./bin/spark-submit --driver-memory 2g --class com.github.ehiggs.spark.terasort.TeraValidate /target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar swift2d://teradata.lvm/terasort_out swift2d://teradata.lvm/terasort_validate 
	
### Functional tests
Copy 

	src/test/resources/core-site.xml.tempate to src/test/resources/core-site.xml
 
Edit `src/test/resources/core-site.xml` and configure Swift access details. 
Functional tests will use container from `fs.swift2d.test.uri`. To use different container, change `drivertest` to different name. Container need not to be exists in advance and will be created automatically. 

## How to develop code
If you like to work on code, you can easily setup Eclipse project via

	mvn eclipse:eclipse

and import it into Eclipse workspace.

Please follow the [development guide](https://github.com/SparkTC/development-guidelines/blob/master/contributing-to-projects.md), prior you submit pull requests.

To easy the debug process, Please modify `conf/log4j.properties` and add

	log4j.logger.com.ibm.stocator=DEBUG

## Next steps
* Code tested with Hadoop 2.6.0.
* Token expiration was not tested (re-authentication)
* Amazon S3 support. It seems to be very easy to add support for S3. Just copy "swift" package into "s3" and use jet3st instead of Joss. One particular issue is that Amazon S3 requires content-length for object uploads. Hence each object creation should create local tmp file, calculate it's size and only then upload to S3.