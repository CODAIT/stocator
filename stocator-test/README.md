# Functional tests of Stocator with Apache Spark

This project contains various runtime tests of Stocator that can be executed over Apache Spark. Each test executed for different formats, like CSV, text files, Parquet and JSON. If one of the tests fails then entire Apache Spark job is failed with exception indicating which tests failed and the possible reason.

The tests can be run against any object storage exposing Swift or S3 API. 

### Requirements and setup

1. Any Apache Spark cluster, local or remote.
    
2. Edit resources/config.properties and configure your object storage.
	Use access and secret keys for HMAC authentication

		endpoint=<ENDPOINT>
		access.key=<ACCESS KEY>
		secret.key=<SECRET KEY>
		
  If used with IAM then configure IAM api key and service instance ID.

		iam.api.key=<API KEY>
		iam.service.id=<SERVICE_ID>		

3. Make sure that stocator jar is on the class path of the Apache Spark cluster. See Apache Spark documentation how to add additional jars to the executors and driver of Apache Spark.
		
4. Set `STOCATOR_HOME`
	
		export STOCATOR_HOME=<path to your Stocator folder>

5. Compile `stocator-test`		
		
		cd ${STOCATOR_HOME}/stocator-test
		mvn clean package
    
### Usage 

The usage pattern is 

	./bin/spark-submit   
	--master <SPARK URL>  
	--class com.ibm.stocator.test.Runner 
	${STOCATOR_HOME}/stocator-test/target/stocator-test-2.0.jar   
	${STOCATOR_HOME}/stocator-test/resources/very-small.csv 
	${STOCATOR_HOME}/stocator-test/resources/config.properties 
	true  - # true if recreate data and delete. "true" is preferable
	false - # false  disable flat listing. true enable default flat listing
	false - # enable / disable timeout test. 
	# if true, test will sleep for 10min and verify that connectons can be recreated.


Example with local Apache Spark cluster:

	./bin/spark-submit  --driver-memory 4G --master local[4]  
	--class com.ibm.stocator.test.Runner 
	${STOCATOR_HOME}/stocator-test/target/stocator-test-2.0.jar   
	${STOCATOR_HOME}/stocator-test/resources/very-small.csv 
	${STOCATOR_HOME}/stocator-test/resources/config.properties true false false

