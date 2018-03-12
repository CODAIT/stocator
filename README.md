Stocator - Storage Connector for Apache Spark
==============================
Apache Spark can work with multiple data sources that include various object stores like IBM Cloud Object Storage, OpenStack Swift and more. To access an object store, Apache Spark uses Hadoop modules that contain connectors to the various object stores. 

Apache Spark needs only small set of object store functionalities. Specifically, Apache Spark requires object listing, objects creation, read objects, and getting data partitions. Hadoop connectors, however, must be compliant with the Hadoop ecosystem. This means they support many more operations, such as shell operations on directories, including move, copy, rename, etc. (these are not native object store operations). Moreover, Hadoop Map Reduce Client is designed to work with file systems and not object stores. The temp files and folders it uses for every write operation are renamed, copied, and deleted. This leads to dozens of useless requests targeted at the object store. It’s clear that Hadoop is designed to work with file systems and not object stores.

Stocator is implicitly designed for the object stores, it has very a different architecture from the existing Hadoop connector. It doesn’t depends on the Hadoop modules and interacts directly with object stores.

Stocator is a generic connector, that may contain various implementations for object stores. It shipped with OpenStack Swift and IBM Cloud Object Storage connectors. Stocator can be easily extended with more object store implementations.

## Major features
* Hadoop ecosystem compliant. Implements Hadoop FileSystem interface
* No need to change or recompile Apache Spark
* Stocator doesn’t create any temporary folders or files for write operations. Each Spark's task generates only one object in the object store. Preserves existing Hadoop fault tolerance model
* Object's name may contain "/" and thus simulate directory structures
* Containers / buckets are automatically created
* Supports speculate mode
* Stocator uses Apache httpcomponents.httpclient.version version 4.5.2 and up
 

## Stocator build procedure

* Checkout the master branch `https://github.com/SparkTC/stocator.git`
* Change directory to `stocator`
* Execute `mvn install`
* If you want to build a jar with all the dependencies, please execute 

		mvn clean package -Pall-in-one
 
## Usage with Apache Spark
Stocator can be used easily with Apache Spark. There are different ways to use Stocator

### Using spark-packages
Stocator deployed on spark-packages. This is the easiest form to integrate Stocator in Spark.
Just follow [stocator](https://spark-packages.org/package/SparkTC/stocator)

### Using Stocator without compiling Apache Spark

It is possible to execute Apache Spark with Stocator, without compiling Apache Spark. Just make sure that Stocator jars on the class path. Alternatively, build Stocator with 

		mvn clean package -Pall-in-one

Directory `stocator/target` contains standalone jar `stocator-X.Y.Z-jar-with-dependencies.jar`.
 
Run Apache Spark with 

	./bin/spark-shell --jars stocator-X.Y.Z-jar-with-dependencies.jar

### Using Stocator with Apache Spark compilation

Less recommended, only for advanced users who want to recompile Spark. Both main `pom.xml` and `core/pom.xml` of Spark should be modified.
 add to the `<properties>` of the main pom.xml
	
	<stocator.version>X.Y.Z</stocator.version>

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

	
Compile Apache Spark with Haddop support ( example of Hadoop 2.7.3 )

	mvn -Phadoop-2.7 -Dhadoop.version=2.7.3 -DskipTests package

## General requirements
Stocator verifies that 

	mapreduce.fileoutputcommitter.marksuccessfuljobs=true

If not modified, the default value of `mapreduce.fileoutputcommitter.marksuccessfuljobs` is `true`.

## Configuration keys
Stocator uses configuration keys that can be configured via `core-site.xml` or provided in run time without using `core-sites.xml`. To provide keys in run time use SparkContext variable with

	sc.hadoopConfiguration.set("KEY","VALUE")

For usage with `core-sites.xml`, see the configuration template located under `conf/core-site.xml.template`.


## Stocator and IBM Cloud Object Storage (COS)
Stocator allows to access IBM Cloud Object Service via `cos://` schema. The general URI is the form

	cos://<bucket>.<service>/object(s)

where `bucket` is object storage bucket and `<service>` identifies configuration group entry.

### Using multiple service names
Each `<service>` may be any name, without special characters. Each service may has it's specific credentials and has different endpoint. By using multiple `<service>` allows to use different endpoints simultaneously. 

For example,  `service=myObjectStore`, then URI will be of the form

	cos://<bucket>.myObjectStore/object(s)

and configuration keys will have prefix `fs.cos.myObjectStore`.
If none provided, the default value is `service`, e.g.

	cos://<bucket>.service/object(s)

and configuration keys will have prefix `fs.cos.service`.


### Reference Stocator in the core-site.xml

Configure Stocator in `conf/core-site.xml`

	<property>
		<name>fs.stocator.scheme.list</name>
		<value>cos</value>
	</property>
	<property>
		<name>fs.cos.impl</name>
		<value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
	</property>
	<property>
		<name>fs.stocator.cos.impl</name>
		<value>com.ibm.stocator.fs.cos.COSAPIClient</value>
	</property>
	<property>
		<name>fs.stocator.cos.scheme</name>
		<value>cos</value>
	</property>

### Configuration keys
Stocator COS connector expose "fs.cos." keys prefix. For backward compatibility Stocator also supports "fs.s3d" and "fs.s3a" prefix, where "fs.cos" has the highest priority and will overwrite other keys, if present.

#### COS Connector configuration
The following is the list of the configuration keys. `<service>` can be any value, for example `myCOS`

| Key | Info | Mandatory |
| --- | ------------ | ------------- |
|fs.cos.`<service>`.access.key | Access key  | mandatory
|fs.cos.`<service>`.secret.key  | Secret key | mandatory
|fs.cos.`<service>`.endpoint | COS endpoint | mandatory
|fs.cos.`<service>`.v2.signer.type |  Signer type  | optional

Example, configure `<service>` as `myCOS`:

	<property>
		<name>fs.cos.myCos.access.key</name>
		<value>ACCESS KEY</value>
	</property>
	<property>
		<name>fs.cos.myCos.endpoint</name>
		<value>http://s3-api.us-geo.objectstorage.softlayer.net</value>
	</property>
	<property>
		<name>fs.cos.myCos.secret.key</name>
		<value>SECRET KEY</value>
	</property>
	<property>
		<name>fs.cos.myCos.v2.signer.type</name>
		<value>false</value>
	</property>

Now you can use URI

	cos://mybucket.myCos/myobject(s)
#### COS Connector optional configuration tunning


| Key | Default | Info |
| --- | ------- | ------ |
| fs.cos.socket.send.buffer | 8*1024 | socket send buffer to be used in the client |
| fs.cos.socket.recv.buffer | 8*1024 |socket send buffer to be used in the client |
| fs.cos.paging.maximum| 5000 | number of records to get while paging through a directory listing |
| fs.cos.threads.max | 10 | the maximum number of threads to allow in the pool used by TransferManager |
| fs.cos.threads.keepalivetime| 60 |the time an idle thread waits before terminating |
| fs.cos.signing-algorithm | | override signature algorithm used for signing requests |
| fs.cos.connection.maximum| 10000 | number of simultaneous connections to the object store |
| fs.cos.attempts.maximum| 20 | number of times we should retry errors |
| fs.cos.block.size| 128 | size of a block in MB |
| fs.cos.connection.timeout | 800000 | amount of time (in ms) until we give up on a connection to the object store |
| fs.cos.connection.establish.timeout| 50000 | amount of time (in ms) until we give up trying to establish a connection to the object store |
| fs.cos.client.execution.timeout| 500000 | amount of time (in ms) to allow a client to complete the execution of an API call |
| fs.cos.client.request.timeout| 500000 | amount of time to wait (in ms) for a request to complete before giving up and timing out |
|fs.cos.connection.ssl.enabled |true | Enables or disables SSL connections to COS |
|fs.cos.proxy.host| | Hostname of the (optional) proxy server for COS connections |
|fs.cos.proxy.port| |Proxy server port. If this property is not set but fs.cos.proxy.host is, port 80 or 443 is assumed (consistent with the value of fs.cos.connection.ssl.enabled) |
| fs.cos.proxy.username| |Username for authenticating with proxy server |
| fs.cos.proxy.password| |Password for authenticating with proxy server |
| fs.cos.proxy.domain| |Domain for authenticating with proxy server |
| fs.cos.user.agent.prefix| |User agent prefix |

## Stocator and Object Storage based on OpenStack Swift API

Stocator allows to access OpenStack Swift API based object stores via unique schema `swift2d://`. 

* Uses streaming for object uploads, without knowing object size. This is unique to Swift connector and removes the need to store object locally prior uploading it.
* Supports Swiftauth, Keystone V2, Keystone V3 Password Scope Authentication
* Supports any object store that expose Swift API and supports different authentication models
* Supports access to public containers.  For example

		sc.textFile("swift2d://dal05.objectstorage.softlayer.net/v1/AUTH_ID/CONT/data.csv")


### Configure Stocator in the core-site.xml

Add the dependence to Stocator in `conf/core-site.xml`

	<property>
		<name>fs.stocator.scheme.list</name>
		<value>swift2d</value>
	</property>

If Swift connector used concurrently with COS connector, then also configure 

	<property>
		<name>fs.stocator.scheme.list</name>
		<value>swift2d,cos</value>
	</property>

Configure the rest of the keys

	<property>
		<name>fs.swift2d.impl</name>
		<value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
	</property>
	<property>
		<name>fs.stocator.swift2d.impl</name>
		<value>com.ibm.stocator.fs.swift.SwiftAPIClient</value>
	</property>
	<property>
		<name>fs.stocator.swift2d.scheme</name>
		<value>swift2d</value>
	</property>

	
   	
#### Swift connector configuration
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
|fs.swift2d.service.SERVICE_NAME.nonstreaming.upload | Optional. If set to true then any object upload will be stored locally in the temp file and uploaded on close method. Disable stocator streaming mode | false


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

#### Keystone V3 mapping to keys

| Driver configuration key | Keystone V3 key |
| ------------------------ | --------------- |
| fs.swift2d.service.SERVICE_NAME.username | user id |
| fs.swift2d.service.SERVICE_NAME.tenant | project id |

In order to properly connect to an IBM Bluemix object store service based on Swift API, you need to open that service in the IBM Bluemix dashboard and inspect the service credentials and update the properties below with the correspondent values :

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

###  Swift connector optional configuration

Below is the optional configuration that can be provided to Stocator and used internally to configure HttpClient.

| Configuration key | Default | Info |
| ------------------------ | --------------- |------|
| fs.stocator.MaxPerRoute | 25 | maximal connections per IP route |
| fs.stocator.MaxTotal | 50 | maximal concurrent connections |
| fs.stocator.SoTimeout | 1000 | low level socket timeout in milliseconds |
| fs.stocator.executionCount | 100 | number of retries for certain HTTP issues |
| fs.stocator.ReqConnectTimeout | 5000 | Request level connect timeout. Determines the timeout in milliseconds until a connection is established
| fs.stocator.ReqConnectionRequestTimeout | 5000 | Request level connection timeout. Returns the timeout in milliseconds used when requesting a connection from the connection manager
| fs.stocator.ReqSocketTimeout | 5000 | Defines the socket timeout (SO_TIMEOUT) in milliseconds, which is the timeout for waiting for data or, put differently, a maximum period inactivity between two consecutive data packets).
| fs.stocator.joss.synchronize.time | false | Will disable JOSS to synchronize time with the server. Setting this value to 'false' will badly impact on temp url. However this will reduce HEAD on account which might be problematic if the user doesn't has access rights to HEAD an account |

## Configure Stocator's schemas (optional)
By default Stocator exposes `swift2d://` for Swift API and `cos://` for IBM Cloud Object Storage. It possible to configure Stocator to expose different schema.

The following example, will configure Stocator to respond to `swift://` in addition to `swift2d://`. This is useful, so users doesn't need to modify existing jobs that already uses hadoop-openstack connector. Below the example, how to configure Stocator to respond both to `swift://` and `swift2d://`

	<property>
		<name>fs.stocator.scheme.list</name>
		<value>swift2d,swift</value>
	</property>
	<!-- configure stocator as swift2d:// -->
	<property>
		<name>fs.swift2d.impl</name>
		<value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
	</property>
	<property>
		<name>fs.stocator.swift2d.impl</name>
		<value>com.ibm.stocator.fs.swift.SwiftAPIClient</value>
	</property>
	<property>
		<name>fs.stocator.swift2d.scheme</name>
		<value>swift2d</value>
	</property>
	<!-- configure stocator as swift:// -->
	<property>
		<name>fs.swift.impl</name>
		<value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
	</property>
	<property>
		<name>fs.stocator.swift.impl</name>
		<value>com.ibm.stocator.fs.swift.SwiftAPIClient</value>
	</property>
	<property>
		<name>fs.stocator.swift.scheme</name>
		<value>swift</value>
	</property>


	
## Examples
#### Persists results in IBM Cloud Object Storage

	val data = Array(1, 2, 3, 4, 5, 6, 7, 8)
	val distData = sc.parallelize(data)
	distData.saveAsTextFile("cos://mydata.service/one1.txt")

Listing bucket `mydata` directly with a REST client will display

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

To ease the debugging process, Please modify `conf/log4j.properties` to

	log4j.logger.com.ibm.stocator=ALL

## Before you sumit your pull request
We ask that you include a line similar to the following as part of your pull request comments: 

	“DCO 1.1 Signed-off-by: Random J Developer“. 
	“DCO” stands for “Developer Certificate of Origin,” 
	
and refers to the same text used in the Linux Kernel community. By adding this simple comment, you tell the community that you wrote the code you are contributing, or you have the right to pass on the code that you are contributing.

## Need more information?
### Stocator Mailing list
Join Stocator mailing list by sending email to `stocator+subscribe@googlegroups.com`.
Use `stocator@googlegroups.com` to post questions.

### Additional resources
Please follow our [wiki](https://github.com/SparkTC/stocator/wiki) for more details.
More information about Stocator can be find at

* [Stocator: A High Performance Object Store Connector for Spark](https://arxiv.org/abs/1709.01812)
* [MapReduce and object stores – How can we do it better?](https://developer.ibm.com/code/2017/05/19/mapreduce-object-stores-can-better/)
* [Advantages and complexities of integrating Hadoop with object stores](https://www.ibm.com/blogs/cloud-computing/2017/05/integrating-hadoop-object-stores/)
* [Stocator on developerWorks Open](https://developer.ibm.com/open/openprojects/stocator/)
* [Fast Lane for Connecting Object Stores to Apache Spark](http://www.spark.tc/stocator-the-fast-lane-connecting-object-stores-to-spark/)
* [Exabytes, Elephants, Objects and Apache Spark](http://ibmresearchnews.blogspot.co.il/2016/02/exabytes-elephants-objects-and-spark.html)
* [Simulating E.T.
 or: how to insert individual files into object storage from within a map function in Apache Spark](https://medium.com/ibm-watson-data-lab/simulating-e-t-e34f4fa7a4f0)
* [Hadoop and object stores: Can we do it better?](https://conferences.oreilly.com/strata/strata-eu/public/schedule/detail/57598) Strata Data Conference, 23-25 May 2017, London, UK
* [VERY LARGE DATA FILES, OBJECT STORES, AND DEEP LEARNING—LESSONS LEARNED WHILE LOOKING FOR SIGNS OF EXTRA-TERRESTRIAL LIFE](https://spark-summit.org/2017/events/very-large-data-files-object-stores-and-deep-learning-lessons-learned-while-looking-for-signs-of-extra-terrestrial-life/) SPARK SUMMIT 2017 DATA SCIENCE AND ENGINEERING AT SCALE,
JUNE 5 – 7, 2017 SAN FRANCISCO


This research was supported by IOStack, an H2020 project of the EU Commission 
