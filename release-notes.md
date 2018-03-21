## Release notes

### Version 1.0.15

1. [cos] Fixing BUFFER_DIR to provide input for the temp files folders
2. [swift, cos] Introducing object store flat globber
3. [cos] partial to support + in object names
4. avoid mkdirs on the directory that already exists. This resolved bug with TeraGen where mkdir writes into folder with existing data

### Version 1.0.14

1. [swift] Improvements for the connections (when data is ready and not on create)
2. Updates of the dependencies
3. [cos] Improve exceptions when IAM credentials are not valid
4. [cos] Fixing list on the root level
5. [cos] Usage of Statistics class to report metrics of bytes read or write
6. [cos] Remove preconditions check to avoid issues with dependencies

### Version 1.0.13

1. Using sha-256 for temp files. This prevents issues with long names

### Version 1.0.12

1. Fixing Null Pointer Exception when runnign with output comitter version 1
2. [cos] New configuration key to define Guava cache size
3. [cos] Fixing content type for block uploads

### Version 1.0.11
1. Improve object read flows
2. Allign list with Hadoop connectors. New configuration flag defines previous flat listing or new nested listing
3. Introduce Guava caching for frequent objects. This greatly reduces number of HEAD calls
4. Bucket names now can include dots
5. Support for partitions, introduced in Spark 2.0.X
6. Imrove temp file generation for write flows
7. Improve Object Store Globber
8. Improve response errors. This fixed bug that reported NPE instead of not authenticated.
9. Block upload for COS connector. Based on disk
10. Implementation for isFile / isDirectory methods

### Version 1.0.10
1. Move to Hadoop 2.7.3
2. Code cleanup
3. Remove dependance on FSExceptionMessages
4. Adjust copyright headers
5. Fix parallel bucket creation
6. Custom user agent

### Version 1.0.9
1. Stocator COS support 
2. Fixing Stocator user agent
3. Fixing issues with streaming
4. Proper handle of the Filter for list operations
4. Moving JOSS to 0.9.15
5. Avoid duplicate get container
6. Making API to work with non US locale

### Version 1.0.8
1. Better debug prints
2. Reducing number of GET requests
3. Fixing list status
4. Fixing get file status on temp object
5. Remove duplicate call to get object length
6. Support for temp urls
7. Added thread pool for create method
8. Support spaces in the names
### Version 1.0.7
1. Modified JOSS to disable HEAD on account when accessing containers. This caused issues when user doesn't has access on account level, but only on container level.
2. Fixed regression caused by consumeQueitely. This fix improved read by 3 times
3. Added cache to contain object length and last modified time stamp. This cache is filled during list and usefull for Spark flows.
4. Removed need to HEAD object before GET. This reduces number of HEAD requests.

### Version 1.0.6
1. Continue improvements with container listing
2. Object upload now based on the Apache HttpClient 4.5.2
3. New configuration keys to tune connection properties
4. Moving Hadoop to 2.7.2
5. Adapting Stocator to work with Hadoop testDFSIO. This includes support for certain flows that required by Hadoop.
6. Continue improvements to logging.

### Version 1.0.5

1. Fixing object store globber. Resolving issues with  container listings
2. Introducing SwiftConnectionManager that is based on  PoolingHttpClientConnectionManager. This makes better connection utilizations both for SwiftAPIDirect and JOSS.
3. Resolving issues with 16 minutes timeouts. Using custom retry handler to retry failed attempts
4. Redesign SwiftOutputStream. This resolved various Parquet related issues, like EOF bug
5. Fixing double authentication calls during SwiftAPIClient init method
6. Supporting multiple schemas
7. Improving error messages
8. Better logging
9. Improving unitests

### Version 1.0.4

1.	Checking for 100-continue in write operations before uploading the data.
2. Fixing token expiration issues in write and read operations
3. Remoded object store HEAD request on the _temporary object
4. Improving unitests
5. Added capability to support different schemas, not just swift2d://
6. Moving JOSS to 0.9.12
7. Applying Apache Trademark guidelines to Readme