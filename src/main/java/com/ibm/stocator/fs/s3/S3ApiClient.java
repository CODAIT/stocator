/**
 * (C) Copyright IBM Corp. 2015, 2016
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.IStoreClient;

import static com.ibm.stocator.fs.s3.S3Constants.S3_ACCESS_KEY_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_BLOCK_SIZE_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_BUCKET_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_SECRET_KEY_PROPERTY;

/**
 * S3 back-end driver
 */
public class S3ApiClient implements IStoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(S3ApiClient.class);

  private int blockSize;
  private RestS3Service service;
  private AWSCredentials credentials;
  private String accessKey;
  private String secretKey;
  private String bucket;

  public S3ApiClient(URI filesystemURI, Configuration conf) throws IOException {
    LOG.debug("Init : {}", filesystemURI.toString());

    Properties props = ConfigurationHandler.initialize(filesystemURI, conf);
    blockSize = Integer.valueOf(props.getProperty(S3_BLOCK_SIZE_PROPERTY));
    bucket = props.getProperty(S3_BUCKET_PROPERTY);
    accessKey = props.getProperty(S3_ACCESS_KEY_PROPERTY);
    secretKey = props.getProperty(S3_SECRET_KEY_PROPERTY);
    credentials = new AWSCredentials(accessKey, secretKey);
    service = new RestS3Service(credentials);

  }


  public long getBlockSize() {
    return blockSize;
  }

  @Override
  public String getDataRoot() {
    return bucket;
  }

  private FileStatus makeHadoopFileStatusFromS3Obj(StorageObject o) {
    return new FileStatus(o.getContentLength(), o.isDirectoryPlaceholder(), 0,
        blockSize, o.getLastModifiedDate().getTime(), new Path(o.getName()));
  }

  @Override
  public FileStatus getObjectMetadata(String hostName, Path path) throws IOException {
    try {
      StorageObject objDetails = service.getObjectDetails(bucket, path.toString());
      return makeHadoopFileStatusFromS3Obj(objDetails);
    } catch (ServiceException e) {
      if (e.getResponseCode() == 404) {
        throw new FileNotFoundException("Cannot locate: " + path.toString() + e.getErrorMessage());
      } else {
        throw new IOException(e.getErrorMessage());
      }
    }
  }

  @Override
  public boolean exists(String hostName, Path path) throws IOException {
    try {
      service.getObjectDetails(bucket, path.toString());
      return true;
    } catch (ServiceException e) {
      if (e.getResponseCode() == 404) {
        return false;
      } else {
        throw new IOException(e.getErrorMessage());
      }
    }
  }

  @Override
  public FSDataInputStream getObject(String hostName, Path path) throws IOException {
    URI uri = path.toUri();
    String newpath = uri.getHost() + uri.getPath();
    S3Object obj = new S3Object(newpath);
    obj.setBucketName(bucket);
    return new FSDataInputStream(new S3InputStream(obj, service));
  }

  @Override
  public FileStatus[] listContainer(String hostName, Path path) throws IOException {
    try {
      S3Object[] objs = service.listObjects(bucket);
      FileStatus[] returnObjs = new FileStatus[objs.length];
      for (int i = 0; i < objs.length; i++) {
        returnObjs[i] = makeHadoopFileStatusFromS3Obj(objs[i]);
      }
      return returnObjs;
    } catch (ServiceException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public FSDataOutputStream createObject(String objName, String contentType, Statistics statistics)
      throws IOException {
    try {
      S3Object obj = new S3Object(objName);
      obj.setContentType(contentType);
      obj.setBucketName(bucket);
      return new FSDataOutputStream(new S3OutputStream(obj, service), statistics);
    } catch (ServiceException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public String getScheme() {
    return "s3";
  }

  @Override
  public boolean delete(String hostName, Path path, boolean recursive) throws IOException {
    try {
      service.deleteObject(bucket, path.toString());
      return true;
    } catch (ServiceException e) {
      return false;
    }
  }
}
