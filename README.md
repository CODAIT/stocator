Stocator - Storage Connector for Apache Spark
==============================
Apache Spark can work with multiple data sources that include object stores like Amazon S3, OpenStack Swift, IBM SoftLayer, and more. To access an object store, Apache Spark uses Hadoop modules that contain drivers to the various object stores. 

Apache Spark needs only small set of object store functionalities. Specifically, Apache Spark requires object listing, objects creation, read objects, and getting data partitions. Hadoop drivers, however, must be compliant with the Hadoop eco system. This means they support many more operations, such as shell operations on directories, including move, copy, rename, etc. (these are not native object store operations). Moreover, Hadoop Map Reduce Client is designed to work with file systems and not object stores. The temp files and folders it uses for every write operation are renamed, copied, and deleted. This leads to dozens of useless requests targeted at the object store. It’s clear that Hadoop is designed to work with file systems and not object stores.

Stocator is implicitly designed for the object stores, it has very a different architecture from the existing Hadoop driver. It doesn’t depends on the Hadoop modules and interacts directly with object stores.

Stocator is a generic connector, that may contain various implementations for object stores. It initially provided with complete Swift driver, based on the JOSS package. Stocator can be very easily extended with more object store implementations, like support for Amazon S3.

## Major features
* Implements Hadoop FileSystem interface
* No need to change or recompile Apache Spark
* Doesn’t create any temporary folders or files for write operations. Each Spark's task generates only one object in the object store. Preserves existing Hadoop fault tolerance model.
* Object's name may contain "/" and thus simulate directory structure
* Containers / buckets are automatically created
* (Swift Driver) Uses streaming for object uploads, without knowing object size. This is unique to OpenStack Swift, removes the need to store object locally prior uploading it.
* (Swift Driver) Tested to read large objects (over 16GB)
* Supports Swiftauth, Keystone V2, Keystone V2 Password Scope Authentication
* Tested to work with vanilla Swift cluster, SoftLayer object store,  IBM Bluemix Object service
* Supports speculate mode
* (Swift Driver) Allows to access public containers, without authentication. For example

		sc.textFile("swift2d://dal05.objectstorage.softlayer.net/v1/AUTH_ID/CONT/data.csv")

## Build procedure

Checkout the Stocator source `https://github.com/SparkTC/stocator.git`

### How to build Stocator
* Change directory to `stocator`
* Execute `mvn install`
* If you want to build a jar with all thedependecies, please execute `mvn clean package -Pall-in-one`

## Usage with Apache Spark
Stocator allows to access Swift via unique schema `swift2d://`.
The configuration template located under `conf/core-site.xml.template`.
Stocator verifies that 

	mapreduce.fileoutputcommitter.marksuccessfuljobs=true

The default value of `mapreduce.fileoutputcommitter.marksuccessfuljobs` is `true`, therefore this key may not exists at all in the Apache Spark's configuration

### Reference the new driver in the core-site.xml
Add driver reference in the `conf/core-site.xml` of Spark

	<property>
   		<name>fs.swift2d.impl</name>
   		<value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
	</property>
	
   	
#### Swift Driver configuration
The following is the list of the configuration keys

| Key | Info | Default value |
| --- | ------------ | ------------- |
|fs.swift2d.service.SERVICE_NAME.auth.url | Mandatory |
|fs.swift2d.service.SERVICE_NAME.public | Optional. Values: true, false | false |
|fs.swift2d.service.SERVICE_NAME.tenant | Mandatory |
|fs.swift2d.service.SERVICE_NAME.password |  Mandatory |
|fs.swift2d.service.SERVICE_NAME.username | Mandatory |
|fs.swift2d.service.SERVICE_NAME.block.size | Block size in MB | 128MB |
|fs.swift2d.service.SERVICE_NAME.region | Mandatory for Keystone|
|fs.swift2d.service.SERVICE_NAME.auth.method | Optional. Values: keystone, swiftauth, keystoneV3| keystoneV3

Below is the internal Keystone V3 mapping

| Driver configuration key | Keystone V3 key |
| ------------------------ | --------------- |
| fs.swift2d.service.SERVICE_NAME.username | user id |
| fs.swift2d.service.SERVICE_NAME.tenant | project id |

#### Example of core-site.xml keys
##### Keystone V2

	<property>
       <name>fs.swift2d.service.SERVICE_NAME.auth.url</name>
    	<value>http://IP:PORT/v2.0/tokens</value>
	</property>
	<property>
       <name>fs.swift2d.service.SERVICE_NAME.public</name>
    	<value>true</value>
	</property>
	<property>
       <name>fs.swift2d.service.SERVICE_NAME.tenant</name>
    	<value>TENANT</value>
	</property>
	<property>
       <name>fs.swift2d.service.SERVICE_NAME.password</name>
    	<value>PASSWORD</value>
	</property>
	<property>
       <name>fs.swift2d.service.SERVICE_NAME.username</name>
    	<value>USERNAME</value>
	</property>
	<property>
       <name>fs.swift2d.service.SERVICE_NAME.auth.method</name>
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

In order to properly connect to an IBM Bluemix object store service, you need to open
that service in the IBM Bluemix dashboard and inspect the service credentials and update
the properties below with the correspondent values :

	<property>
	    <name>fs.swift2d.service.SERVICE_NAME.auth.url</name>
	    <value>https://identity.open.softlayer.com/v3/auth/tokens</value>
	</property>
	<property>
	    <name>fs.swift2d.service.SERVICE_NAME.public</name>
	    <value>true</value>
	</property>
	<property>
	    <name>fs.swift2d.service.SERVICE_NAME.tenant</name>
	    <value>PROJECTID</value>
	</property>
	<property>
	    <name>fs.swift2d.service.SERVICE_NAME.password</name>
	    <value>PASSWORD</value>
	</property>
	<property>
	    <name>fs.swift2d.service.SERVICE_NAME.username</name>
	    <value>USERID</value>
	</property>
	<property>
	    <name>fs.swift2d.service.SERVICE_NAME.auth.method</name>
	    <value>keystoneV3</value>
	</property>
	<property>
	    <name>fs.swift2d.service.SERVICE_NAME.region</name>
	    <value>dallas</value>
	</property>

## Providing configuration keys in run time
It's possible to provide configuration keys in run time, without keeping them in core-sites.xml. Just use SparkContext variable with

	sc.hadoopConfiguration.set("KEY","VALUE")

## Execution without compiling Apache Spark
It is possible to execute Apache Spark with the new driver, without compiling Apache Spark.
Directory `stocator/target` contains standalone jar `stocator-1.0.1-jar-with-dependencies.jar`.
 
Run Apache Spark with 

	./bin/spark-shell --jars stocator-1.0.1-jar-with-dependencies.jar

## Execution with Apache Spark compilation

### Configure maven build in Apache Spark
Both main `pom.xml` and `core/pom.xml` should be modified.
 
 add to the `<properties>` of the main pom.xml
	
	 	<stocator.version>1.0.1</stocator.version>

 add `stocator` dependency to the main pom.xml

     <dependency>
          <groupId>com.ibm.stocator</groupId>
          <artifactId>stocator</artifactId>
          <version>${stocator.version}</version>
          <scope>${hadoop.deps.scope}</scope>
      </dependency>

modify `core/pom.xml` to include `stocator`

    <dependency>
          <groupId>com.ibm.stocator</groupId>
          <artifactId>stocator</artifactId>
     </dependency>

	
	
### Compile Apache Spark
Compile Apache Spark with Haddop support 

	mvn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests package

	
### Examples
#### Create new object in Swift.

	val data = Array(1, 2, 3, 4, 5, 6, 7, 8)
	val distData = sc.parallelize(data)
	distData.saveAsTextFile("swift2d://newcontainer.SERVICE_NAME/one1.txt")

Listing container `newcontainer` directly with a REST client will display

	one1.txt
	one1.txt/_SUCCESS
	one1.txt/part-00000-taskid
	one1.txt/part-00001-taskid
	one1.txt/part-00002-taskid
	one1.txt/part-00003-taskid
	one1.txt/part-00004-taskid
	one1.txt/part-00005-taskid
	one1.txt/part-00006-taskid
	one1.txt/part-00007-taskid

### Running Terasort

Follow 

	https://github.com/ehiggs/spark-terasort
	
You can run Terasort as follows (remember to change the word "SERVICE_NAME" with your Swift SERVICE_NAME name)
Step 1:
	
	export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

Step 2:

	./bin/spark-submit --driver-memory 2g --class com.github.ehiggs.spark.terasort.TeraGen /spark-terasort/target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 1g swift2d://teradata.SERVICE_NAME/terasort_in
	
Step 3:

	./bin/spark-submit --driver-memory 2g --class com.github.ehiggs.spark.terasort.TeraSort /target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar 1g swift2d://teradata.SERVICE_NAME/terasort_in swift2d://teradata.SERVICE_NAME/terasort_out
	
Step 4:

	./bin/spark-submit --driver-memory 2g --class com.github.ehiggs.spark.terasort.TeraValidate /target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar swift2d://teradata.SERVICE_NAME/terasort_out swift2d://teradata.SERVICE_NAME/terasort_validate
	
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

	log4j.logger.com.ibm.stocator=ALL

## Before you sumit your pull request
We ask that you include a line similar to the following as part of your pull request comments: “DCO 1.1 Signed-off-by: Random J Developer“. “DCO” stands for “Developer Certificate of Origin,” and refers to the same text used in the Linux Kernel community. By adding this simple comment, you tell the community that you wrote the code you are contributing, or you have the right to pass on the code that you are contributing.

## Need more information?
### Stocator Mailing list
Join Stocator mailing list by sending email to `stocator+subscribe@googlegroups.com`.
Use `stocator@googlegroups.com` to post questions.

### Additional resources
Please follow our [wiki](https://github.com/SparkTC/stocator/wiki) for more details.
More information about Stocator can be find at

* [Fast Lane for Connecting Object Stores to Apache Spark](http://www.spark.tc/stocator-the-fast-lane-connecting-object-stores-to-spark/)
* [Exabytes, Elephants, Objects and Apache Spark](http://ibmresearchnews.blogspot.co.il/2016/02/exabytes-elephants-objects-and-spark.html)
