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

import org.apache.hadoop.fs.Path;

import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;

public class StocatorPath {
  private String tempFileOriginator;
  private String tempIdentifier;

  public StocatorPath(String fileOriginator) {
    tempFileOriginator = fileOriginator;
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      tempIdentifier = HADOOP_TEMPORARY;
    }
  }

  public boolean isTemporaryPathContain(Path path) {
    if (path.toString().contains(tempIdentifier)) {
      return true;
    }
    return false;
  }

  public boolean isTemporaryPathContain(String path) {
    if (path.contains(tempIdentifier)) {
      return true;
    }
    return false;
  }

  public boolean isTemporaryPathTaget(Path path) {
    if (path.toString().endsWith(tempIdentifier)) {
      return true;
    }
    return false;
  }

  /**
   * Get object name with data root
   *
   * @param fullPath the path
   * @param addTaskIdCompositeName add task id composite
   * @param dataRoot the data root
   * @param hostNameScheme hostname
   * @return composite of data root and object name
   * @throws IOException if object name is missing
   */
  public String getObjectNameRoot(Path fullPath, boolean addTaskIdCompositeName,
      String dataRoot, String hostNameScheme) throws IOException {
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      return dataRoot + "/" + parseHadoopFOutputCommitterV1(fullPath,
          addTaskIdCompositeName, hostNameScheme);
    }
    return fullPath.toString();
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
          String taskAttempt = Utils.extractTaskID(path);
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

}
