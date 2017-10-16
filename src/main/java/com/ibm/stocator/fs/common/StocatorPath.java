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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;
import static com.ibm.stocator.fs.common.Constants.TRASH_FOLDER;

public class StocatorPath {
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(StocatorPath.class);

  private String tempFileOriginator;
  private String tempIdentifier;
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
      tempIdentifier = HADOOP_TEMPORARY;
      tempIdentifiers = conf.getStrings("fs.stocator.temp.identifier",
          "_temporary/st_ID/_temporary/attempt_ID/");
    } else if (conf != null) {
      tempIdentifiers = conf.getStrings("fs.stocator.temp.identifier",
          "_temporary/st_ID/_temporary/attempt_ID/");
    }
  }

  public boolean isFileOutputComitter() {
    return tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1);
  }

  public boolean isTrashDestination(Path path) {
    if (path.toString().contains(TRASH_FOLDER)) {
      return true;
    }
    return false;
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
    LOG.debug("isTemporaryPathTarget for {}", path);
    if (path.toString().equals(hostNameScheme) || path.getParent() == null) {
      LOG.debug("Temporary target on the path eauals hostname or null parent {}", path);
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
    String res = "";
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      res =  parseHadoopFOutputCommitterV1(fullPath,
          addTaskId, hostNameScheme);
    } else {
      res = extractNameFromTempPath(fullPath, addTaskId, hostNameScheme, false);
    }
    if (!res.equals("")) {
      if (addRoot) {
        return dataRoot + "/" + res;
      }
      return res;
    }
    return fullPath.toString();
  }

  public String getGlobalPrefixName(Path fullPath,
      String dataRoot, boolean addRoot) throws IOException {
    String res = "";
    res = extractNameFromTempPath(fullPath, false, hostNameScheme, true);
    if (dataRoot.endsWith("/")) {
      dataRoot = dataRoot.substring(0,  dataRoot.length() - 1);
    }
    if (!res.equals("")) {
      if (addRoot) {
        return dataRoot + "/" + res;
      }
      return res;
    }
    return fullPath.toString();
  }

  public String getActualPath(Path fullPath, boolean addTaskIdCompositeName,
      String dataRoot) throws IOException {
    if (isTemporaryPathContain(fullPath)) {
      return hostNameScheme + getObjectNameRoot(fullPath, addTaskIdCompositeName,
          dataRoot, false);
    }
    return fullPath.toString();
  }

  /**
   * @param p path
   * @param addTaskID add task id to the extracted name
   * @param hostName hostname used to register Stocator
   * @param onlyPrefix return only prefix of the temp names
   * @return
   */
  private String extractNameFromTempPath(Path p, boolean addTaskID, String hostName,
      boolean onlyPrefix) {
    LOG.debug("Extract name from {}", p.toString());
    String path = p.toString();
    // if path starts with host name - no need it, remove.
    if (path.startsWith(hostName)) {
      path = path.substring(hostName.length());
    }
    // loop over all temporary identifiers and see if match
    boolean match = true;
    String midName = "";
    String namePrefix = "";
    for (String tempPath : tempIdentifiers) {
      LOG.debug("Temp identifier {}",tempPath);
      String taskAttempt = null;
      match = true;
      String[] tempPathComponents = tempPath.split("/");
      int startIndex = path.indexOf(tempPathComponents[0].replace("ID", ""));
      // if the 1st one match - most likely we hit the right one.
      // otherwise - proceed to the next temporary structure
      if (startIndex < 0) {
        match = false;
        continue;
      }
      // get all the path components that are prefixed the temporary identifier
      namePrefix = path.substring(0, startIndex - 1);
      if (namePrefix.endsWith("_")) {
        match = false;
        break;
      }
      if (onlyPrefix) {
        if (namePrefix.startsWith("/")) {
          return namePrefix.substring(1);
        }
        return namePrefix;
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
          if (addTaskID && suffixID != null && suffixID.equals("ID")) {
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
          if (taskAttempt != null) {
            return namePrefix + "/" + posixSplit[posixSplit.length - 1] + "-" + taskAttempt;
          }
          return namePrefix + "/" + posixSplit[posixSplit.length - 1];
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
   * @param path path to extract from
   * @param addTaskIdCompositeName if true will add task-id to the object name
   * @param hostNameScheme the host name
   * @return new object name
   * @throws IOException if object name is missing
   */
  private String parseHadoopFOutputCommitterV1(Path fullPath,
      boolean addTaskIdCompositeName, String hostNameScheme) throws IOException {
    String boundary = HADOOP_TEMPORARY;
    String path = fullPath.toString();
    String noPrefix = path.substring(hostNameScheme.length());
    int npIdx = noPrefix.indexOf(boundary);
    String objectName = "";
    if (npIdx >= 0) {
      if (npIdx == 0 || npIdx == 1 && noPrefix.startsWith("/")) {
        //no object name present
        //schema://tone1.lvm/_temporary/0/_temporary/attempt_201610038_0001_m_000007_15/part-0007
        //schema://tone1.lvm_temporary/0/_temporary/attempt_201610038_0001_m_000007_15/part-0007
        throw new IOException("Object name is missing");
      } else {
        //path matches pattern in javadoc
        objectName = noPrefix.substring(0, npIdx - 1);
        if (addTaskIdCompositeName) {
          String taskAttempt = Utils.extractTaskID(path, HADOOP_ATTEMPT);
          String objName = fullPath.getName();
          if (taskAttempt != null && !objName.startsWith(HADOOP_ATTEMPT)) {
            objName = fullPath.getName() + "-" + taskAttempt;
          }
          objectName = objectName + "/" + objName;
        }
      }
      return objectName;
    }
    return noPrefix;
  }

  public List<Tuple<String, String>> getAllPartitions(String path) {
    List<Tuple<String, String>> res = new ArrayList<>();
    if (path.contains("=")) {
      String[] components = path.split("/");
      for (String component : components) {
        int pos = component.indexOf("=");
        if (pos > 0) {
          String key = component.substring(0, pos);
          String value = component.substring(pos + 1);
          res.add(new Tuple<String, String>(key, value));
        }
      }
    }
    return res;
  }

  public boolean isPartitionTarget(Path path) {
    String name = path.getName();
    LOG.debug("Is partition target for {} from {}", name, path.toString());
    if (name != null && name.contains("=")) {
      return true;
    }
    return false;
  }

  public boolean isPartitionExists(Path path) {
    String name = path.toString();
    LOG.debug("Is partition target for {} from {}", name, path.toString());
    if (name != null && name.contains("=")) {
      return true;
    }
    return false;
  }

}
