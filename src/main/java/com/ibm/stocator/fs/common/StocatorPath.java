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
   * @param fileOriginator originator
   * @param conf Configuration
   * @param hostName hostname
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

  public boolean isTemporaryPathContain(Path path) {
    for (String tempPath : tempIdentifiers) {
      String[] tempPathComponents = tempPath.split("/");
      if (tempPathComponents.length > 0
          && path.toString().contains(tempPathComponents[0].replace("ID", ""))) {
        return true;
      }
    }
    return false;
  }

  public boolean isTemporaryPathContain(String path) {
    for (String tempPath : tempIdentifiers) {
      String[] tempPathComponents = tempPath.split("/");
      if (tempPathComponents.length > 0
          && path.contains(tempPathComponents[0].replace("ID", ""))) {
        return true;
      }
    }
    return false;
  }

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
   * Get object name with data root
   *
   * @param fullPath the path
   * @param addTaskId add task id composite
   * @param dataRoot the data root
   * @param addRoot add or not the data root to the path
   * @return composite of data root and object name
   * @throws IOException if object name is missing
   */
  public String getObjectNameRoot(Path fullPath, boolean addTaskId,
      String dataRoot, boolean addRoot) throws IOException {
    String res;
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      res = parseHadoopFOutputCommitterV1(fullPath, addTaskId, hostNameScheme);
    } else {
      res = extractNameFromTempPath(fullPath, addTaskId, hostNameScheme);
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
    return fullPath.toString();
  }

  /**
   * @param p path
   * @param addTaskID add task id to the extracted name
   * @param hostName hostname used to register Stocator
   * @return
   */
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

  /**
   * Extract object name from path. If addTaskIdCompositeName=true then
   * schema://tone1.lvm/aa/bb/cc/one3.txt/_temporary/0/_temporary/
   * attempt_201610052038_0001_m_000007_15/part-00007 will extract get
   * aa/bb/cc/201610052038_0001_m_000007_15-one3.txt
   * otherwise object name will be aa/bb/cc/one3.txt
   *
   * @param fullPath path to extract from
   * @param addTaskIdCompositeName if true will add task-id to the object name
   * @param hostNameScheme the host name
   * @return new object name
   * @throws IOException if object name is missing
   */
  private String parseHadoopFOutputCommitterV1(Path fullPath,
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
        //no object name present
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
   *
   * @param filename The path of filename to extract the extension from
   * @return The extension of the filename
   */
  private String extractExtension(String filename) {
    int startExtension = filename.indexOf('.');
    if (startExtension > 0) {
      return filename.substring(startExtension + 1);
    }
    return "";
  }

  /**
   * Transform scheme://hostname/a/b/_temporary/0 into scheme://hostname/a/b/
   *
   * @param path input directory with temp prefix
   * @return modified directory
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
   * Accepts any object name. If object name of the form
   * a/b/c/gil.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   * .csv-attempt_20160317132wrong_0000_m_000000_1 Then a/b/c/gil.data is
   * returned. Code testing that attempt_20160317132wrong_0000_m_000000_1 is
   * valid task id identifier
   *
   * @param objectKey object key
   * @return unified object name
   */
  public String extractUnifiedObjectName(String objectKey) {
    return extractFromObjectKeyWithTaskID(objectKey, true);
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
    return extractFromObjectKeyWithTaskID(objectKey, false);
  }

  /**
   * Extracts from the object key an unified object name or name without task ID
   *
   * @param objectKey object key
   * @param isUnifiedObjectKey
   * @return unified name
   */
  private String extractFromObjectKeyWithTaskID(String objectKey, boolean isUnifiedObjectKey) {
    Path p = new Path(objectKey);
    int index = objectKey.indexOf("-" + HADOOP_ATTEMPT);
    if (index > 0) {
      String attempt = objectKey.substring(objectKey.lastIndexOf("-") + 1);
      try {
        if (attempt.indexOf(".") > 0) {
          attempt = attempt.substring(0, attempt.indexOf("."));
        }
        TaskAttemptID.forName(attempt);
        if (isUnifiedObjectKey) {
          return p.getParent().toString();
        } else {
          return objectKey.substring(0, index);
        }
      } catch (IllegalArgumentException e) {
        return objectKey;
      }
    } else if (isUnifiedObjectKey && objectKey.indexOf(HADOOP_SUCCESS) > 0) {
      return p.getParent().toString();
    }
    return objectKey;
  }

  /**
   * Extracts from the object key an unified object name or name without task ID
   *
   * @param objectKey object key
   * @return unified name
   */
  public String extractUnifiedName(String objectKey) {
    Path p = new Path(objectKey);
    int index = objectKey.indexOf(HADOOP_PART);
    if (index > 0) {
      return objectKey.substring(0, index);
    } else if (objectKey.indexOf(HADOOP_SUCCESS) > 0) {
      return objectKey.substring(0, objectKey.indexOf(HADOOP_SUCCESS));
    }
    return objectKey;
  }

}
