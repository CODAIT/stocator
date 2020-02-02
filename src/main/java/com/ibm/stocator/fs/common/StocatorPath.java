/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.stocator.fs.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.http.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_PART;
import static com.ibm.stocator.fs.common.Constants.HADOOP_SUCCESS;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;

public class StocatorPath {
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(StocatorPath.class);

  private String tempFileOriginator;
  private String[] tempIdentifiers;
  private String hostNameScheme;

  /**
   * Main constructor
   *
   * @param fileOriginator originator
   * @param conf Configuration
   * @param hostName hostname of the form scheme://bucket.service
   */
  public StocatorPath(String fileOriginator, Configuration conf, String hostName) {
    LOG.debug("StocatorPath generated with hostname {} and file originator {}", hostName,
        fileOriginator);
    tempFileOriginator = fileOriginator;
    hostNameScheme = hostName;
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      tempIdentifiers = conf.getStrings("fs.stocator.temp.identifier",
          "_temporary/st_ID/_temporary/attempt_ID/");
    } else if (conf != null) {
      tempIdentifiers = conf.getStrings("fs.stocator.temp.identifier",
          "_temporary/st_ID/_temporary/attempt_ID/");
    }
  }

  /**
   * Inspect the path and return true if path contains reserved "_temporary" or
   * the first entry from "fs.stocator.temp.identifier" if provided
   *
   * @param path to inspect
   * @return true if path contains temporary identifier
   */
  public boolean isTemporaryPath(Path path) {
    if (path != null) {
      return isTemporaryPath(path.toString());
    }
    return false;
  }

  /**
   * Inspect the path and return true if path contains reserved "_temporary" or
   * the first entry from "fs.stocator.temp.identifier" if provided
   *
   * @param path to inspect
   * @return true if path contains temporary identifier
   */
  public boolean isTemporaryPath(String path) {
    for (String tempPath : tempIdentifiers) {
      String[] tempPathComponents = tempPath.split("/");
      if (tempPathComponents.length > 0
          && path != null && path.contains(tempPathComponents[0].replace("ID", ""))) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method identifies if path targets temporary object, for example
   * scheme://a.service/one3.txt/_temporary
   * We mainly need this method for mkdirs operation
   *
   * @param path the input path
   * @return true if path targets temporary object
   */
  public boolean isTemporaryPathTarget(Path path) {
    LOG.trace("isTemporaryPathTarget for {}", path);
    if (path.toString().equals(hostNameScheme) || path.getParent() == null) {
      LOG.trace("Temporary target on the path eauals hostname or null parent {}", path);
      return false;
    }
    String name = path.getName();
    String parent = path.getParent().toString();
    for (String tempPath : tempIdentifiers) {
      String[] tempPathComponents = tempPath.split("/");
      if (parent.endsWith(tempPathComponents[0].replace("ID", ""))
          || name.startsWith(tempPathComponents[0].replace("ID", ""))) {
        LOG.debug("Temporary path identified on {}", path);
        return true;
      }
    }
    LOG.debug("Temporary path not identified for {}", path);
    return false;
  }

  /**
   * Transform scheme://hostname/a/b/_temporary/0 into scheme://hostname/a/b/
   * We mainly need this for mkdirs operations
   *
   * @param path input directory with temporary prefix
   * @return modified directory without temporary prefix
   */
  public String getBaseDirectory(String path) {
    if (path != null) {
      int finishIndex = path.indexOf(HADOOP_TEMPORARY);
      if (finishIndex > 0) {
        String newPath = path.substring(0, finishIndex);
        if (newPath.endsWith("/")) {
          return newPath.substring(0, newPath.length() - 1);
        }
        return newPath;
      }
    }
    return path;
  }

  /**
   * Transform temporary path to the final destination
   * For example:
   * scheme://a.service/data.txt/_temporary/0/_temporary/"
   *     + "attempt_201610052038_0001_m_000007_15";
   * transformed to  a/data.txt
   *
   * @param path the input path
   * @param addTaskId add task id composite
   * @param dataRoot the data root
   * @param addRoot add or not the data root to the path (bucket) as a prefix
   * @return object name without temporary path
   * @throws IOException if object name is missing
   */
  public String extractFinalKeyFromTemporaryPath(Path path, boolean addTaskId,
      String dataRoot, boolean addRoot) throws IOException {
    String res;
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      res = parseHadoopOutputCommitter(path, addTaskId, hostNameScheme);
    } else {
      res = extractNameFromTempPath(path, addTaskId, hostNameScheme);
    }
    if (!res.equals("")) {
      if (addRoot) {
        if (res != null && res.startsWith("/")) {
          res = res.substring(1);
        }
        return dataRoot + "/" + res;
      }
      return res;
    }
    return path.toString();
  }

  /**
   * Accept temporary path and return a final destination path
   *
   * @param path path name to modify
   * @return modified path name
   * @throws IOException if error
   */
  public Path modifyPathToFinalDestination(Path path) throws IOException {
    String res;
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      res = parseHadoopOutputCommitter(path, true, hostNameScheme);
    } else {
      res = extractNameFromTempPath(path, true, hostNameScheme);
    }
    return new Path(hostNameScheme, res);

  }

  /**
   * Accepts any object name. If object name is of the form
   * a/b/c/m.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   * .csv-attempt_20160317132wrong_0000_m_000000_1 Then
   * a/b/c/m.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv is
   * returned. Perform test that attempt_20160317132wrong_0000_m_000000_1 is
   * valid task id identifier
   *
   * @param objectKey object key
   * @return unified object name
   */
  public String nameWithoutTaskID(String objectKey) {
    int index = objectKey.indexOf("-" + HADOOP_ATTEMPT);
    if (index > 0) {
      String attempt = objectKey.substring(index + 1);
      try {
        if (attempt.indexOf(".") > 0) {
          attempt = attempt.substring(0, attempt.indexOf("."));
        }
        TaskAttemptID.forName(attempt);
        String res = objectKey.replace("-" + attempt, "");
        return res;
      } catch (IllegalArgumentException e) {
        return objectKey;
      }
    }
    return objectKey;
  }

  /**
   * Extracts from the object key an object name without part or _success
   * For example
   * data/a/d.parquet/part-00001-abb1f63385e9-attempt_20180503184805_0006_m_000007_0.snappy.parquet
   *
   * transformed into
   *
   * dataset/a/data.parquet/
   *
   * @param objectKey object key
   * @return unified name
   */
  public String removePartOrSuccess(String objectKey) {
    String res = objectKey;
    int index = objectKey.indexOf(HADOOP_PART);
    if (index > 0) {
      res = objectKey.substring(0, index);
    } else if (objectKey.indexOf(HADOOP_SUCCESS) > 0) {
      res =  objectKey.substring(0, objectKey.indexOf(HADOOP_SUCCESS));
    }
    return res;
  }

  /**
   * Main method to parse Hadoop OutputCommitter V1 or V2 as used by Hadoop M-R or Apache Spark
   * Method transforms object name from the temporary path.
   *
   * The input of the form
   *
   * schema://mydata.service/a/b/c/data.parquet/_temporary/0/_temporary/
   * attempt_201610052038_0001_m_000007_15/part-00001
   *
   * If addTaskIdCompositeName=true then input transformed into
   * a/b/c/data.parquet/part-00001-attempt_201610052038_0001_m_000007_15.parquet
   *
   * If addTaskIdCompositeName=false then input transformed into
   * a/b/c/data.parquet/part-00001.parquet
   *
   * @param fullPath path to extract from
   * @param addTaskIdCompositeName if true will add task-id to the object name
   * @param hostNameScheme the host name
   * @return transformed name
   * @throws IOException if object name is missing
   */
  private String parseHadoopOutputCommitter(Path fullPath,
      boolean addTaskIdCompositeName, String hostNameScheme) throws IOException {
    String path = fullPath.toString();
    String noPrefix = path;
    if (path.startsWith(hostNameScheme)) {
      noPrefix = path.substring(hostNameScheme.length());
    }
    int npIdx = noPrefix.indexOf(HADOOP_TEMPORARY);
    String objectName;
    if (npIdx >= 0) {
      if (npIdx == 0 || npIdx == 1 && noPrefix.startsWith("/")) {
        //no object name present, for example
        //schema://tone1.lvm/_temporary/0/_temporary/attempt_201610038_0001_m_000007_15/part-0007
        //schema://tone1.lvm_temporary/0/_temporary/attempt_201610038_0001_m_000007_15/part-0007
        throw new IOException("Object name is missing");
      } else {
        // Will give: schema://tone1.lvm/aa/bb/cc/one3.txt/
        objectName = noPrefix.substring(0, npIdx - 1);
        if (addTaskIdCompositeName) {
          String objName = null;
          String taskAttempt = Utils.extractTaskID(path, HADOOP_ATTEMPT);
          if (taskAttempt != null) {
            int fIndex = fullPath.toString().indexOf(taskAttempt + "/");
            if (fIndex > 0) {
              fIndex = fIndex + taskAttempt.length() + 1;
            }
            if (fIndex < fullPath.toString().length()) {
              objName = fullPath.toString().substring(fIndex);
            }
          }
          if (objName == null) {
            objName = fullPath.getName();
          }
          if (taskAttempt != null && !objName.startsWith(HADOOP_ATTEMPT)) {
            // We want to prepend the attempt before the extension
            String extension = extractExtension(objName);
            objName = objName.replace("." + extension, "") + "-" + taskAttempt;
            if (!extension.equals("")) {
              objName += "." + extension;
            }
          }
          objectName = objectName + "/" + objName;
        }
      }
      return objectName;
    }
    return noPrefix;
  }

  /**
   * A filename, for example, one3-attempt-01.txt.gz will return txt.gz
   * Need to make sure we take extension and not middle dot
   * COL2=myvalue1/COL1=my.org1/part-592fd006da8a.c000.snappy.parquet
   *
   * @param filename The path of filename to extract the extension from
   * @return The extension of the filename
   */
  private String extractExtension(String filename) {
    int st = filename.lastIndexOf("/");
    if ((st > 0) && (st < filename.length())) {
      filename = filename.substring(st + 1);
    }
    int startExtension = filename.indexOf('.');
    if (startExtension > 0) {
      return filename.substring(startExtension + 1);
    }
    return "";
  }

  /**
   * This is to process temporary path generated by non standard output committers
   * Code uses temporary path patterns
   * Not in use for Hadoop OutputCommitter V1 or V2
   *
   * @param p path
   * @param addTaskID add task id to the extracted name
   * @param hostName hostname used to register Stocator
   * @return
   */
  @Experimental
  private String extractNameFromTempPath(Path p, boolean addTaskID, String hostName) {
    LOG.trace("Extract name from {}", p.toString());
    String path = p.toString();
    // if path starts with host name - no need it, remove.
    if (path.startsWith(hostName)) {
      path = path.substring(hostName.length());
    }
    // loop over all temporary identifiers and see if match
    boolean match;
    String midName = "";
    String namePrefix;
    for (String tempPath : tempIdentifiers) {
      LOG.trace("Temp identifier {}",tempPath);
      String taskAttempt = null;
      match = true;
      String[] tempPathComponents = tempPath.split("/");
      int startIndex = path.indexOf(tempPathComponents[0].replace("ID", ""));
      // if the 1st one match - most likely we hit the right one.
      // otherwise - proceed to the next temporary structure
      if (startIndex < 0) {
        continue;
      }
      // get all the path components that are prefixed the temporary identifier
      namePrefix = path.substring(0, startIndex - 1);
      if (namePrefix.endsWith("_")) {
        break;
      }
      // we need to match temporary structure and take the rest
      String namePosix = path.substring(startIndex);
      // namePossix contains all temporary identifier and the following object name
      // split only as the number of temporary identifiers
      String[] posixSplit = namePosix.split("/", tempPathComponents.length + 1);
      // now loop over temporary identifiers and see if they match the path
      for (int i = 0; i < tempPathComponents.length; i++) {
        if (i > posixSplit.length - 1) {
          break;
        }
        if (tempPathComponents[i].equals("st_ID")) {
          continue;
        } else if (tempPathComponents[i].equals("_ADD_")) {
          midName = midName + "/" + posixSplit[i];
        } else if (!tempPathComponents[i].contains("ID")
                   && !tempPathComponents[i].equals(posixSplit[i])) {
          match = false;
          break;
        } else if (tempPathComponents[i].contains("ID")) { // not only contains, but also starts
          // split _task_tmp.-ext-10002, but temp component is _tmp.-ext-ID1
          // the temp identifier contains ID. We need to extract the value
          int split = tempPathComponents[i].indexOf("ID");
          String prefixID = tempPathComponents[i].substring(0, split);
          String suffixID = tempPathComponents[i].substring(split);
          if (!posixSplit[i].startsWith(prefixID)) {
            match = false;
            break;
          }
          if (addTaskID && suffixID.equals("ID")) {
            taskAttempt = Utils.extractTaskID(posixSplit[i], prefixID);
          }
        }
      }

      // check if match and then take the rest
      if (match) {
        // take the rest
        if (!midName.equals("")) {
          if (!midName.startsWith("/")) {
            namePrefix = namePrefix + "/" + namePrefix;
          } else {
            namePrefix = namePrefix + midName;
          }
        }
        if (posixSplit.length > tempPathComponents.length) {
          String objName = posixSplit[posixSplit.length - 1];
          if (taskAttempt != null) {
            String extension = extractExtension(objName);
            return namePrefix + "/"
                   + objName.replace("." + extension, "")
                   + "-" + taskAttempt
                   + "." + objName;
          }
          return namePrefix + "/" + objName;
        }
        return namePrefix;
      }
    }
    return path;
  }

}
