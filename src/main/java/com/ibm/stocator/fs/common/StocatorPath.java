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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;
import static com.ibm.stocator.fs.common.Constants.HIVE_TMP1;
import static com.ibm.stocator.fs.common.Constants.HIVE_EXT1;
import static com.ibm.stocator.fs.common.Constants.TASK_HIVE_TMP1;
import static com.ibm.stocator.fs.common.Constants.HIVE_OUTPUT_V1;
import static com.ibm.stocator.fs.common.Constants.HCATALOG_V1;
import static com.ibm.stocator.fs.common.Constants.HCATALOG_STAGING_DEFAULT;
import static com.ibm.stocator.fs.common.Constants.HIVE_STAGING_DEFAULT;

public class StocatorPath {
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(StocatorPath.class);

  private String tempFileOriginator;
  private String tempIdentifier;

  public StocatorPath(String fileOriginator, Configuration conf) {
    tempFileOriginator = fileOriginator;
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      tempIdentifier = HADOOP_TEMPORARY;
    } else if (tempFileOriginator.equals(HIVE_OUTPUT_V1)) {
      String stagingDir = HIVE_STAGING_DEFAULT;
      if (conf != null) {
        stagingDir = conf.get("hive.exec.stagingdir",HIVE_STAGING_DEFAULT);
      }
      tempIdentifier = stagingDir + "_hive_";
      LOG.debug("Hive identified {}, staging {}", HIVE_OUTPUT_V1, tempIdentifier);
    } else if (tempFileOriginator.equals(HCATALOG_V1)) {
      tempIdentifier = HCATALOG_STAGING_DEFAULT;
    }
  }

  public boolean isHive() {
    return tempFileOriginator.equals(HIVE_OUTPUT_V1);
  }

  public boolean isTempName(Path f) {
    String name = f.getName();
    if (name.startsWith(HIVE_TMP1) || name.startsWith(HIVE_EXT1)) {
      return true;
    }
    return false;
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

  public boolean isTemporaryPathTarget(Path path) {
    String name = path.getName();
    if (path.getParent().toString().endsWith(tempIdentifier)) {
      return true;
    }
    if (name.startsWith(tempIdentifier)) {
      return true;
    }
    if (!name.startsWith(tempIdentifier)
            && !name.startsWith(HIVE_TMP1)
            && !name.startsWith(TASK_HIVE_TMP1)
            && !name.startsWith(HIVE_EXT1)) {
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
   * @param addRoot add or not the data root to the path
   * @return composite of data root and object name
   * @throws IOException if object name is missing
   */
  public String getObjectNameRoot(Path fullPath, boolean addTaskIdCompositeName,
      String dataRoot, String hostNameScheme, boolean addRoot) throws IOException {
    String res = "";
    if (tempFileOriginator.equals(DEFAULT_FOUTPUTCOMMITTER_V1)) {
      res =  parseHadoopFOutputCommitterV1(fullPath,
          addTaskIdCompositeName, hostNameScheme);
    } else if (tempFileOriginator.equals(HIVE_OUTPUT_V1)) {
      res =  parseHiveV1(fullPath, hostNameScheme);
    } else if (tempFileOriginator.equals(HCATALOG_V1)) {
      res = parseHcatalogV1(fullPath, hostNameScheme);
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
      String dataRoot, String hostNameScheme) throws IOException {
    if (isTemporaryPathContain(fullPath)) {
      return hostNameScheme + getObjectNameRoot(fullPath, addTaskIdCompositeName,
          dataRoot, hostNameScheme, false);
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

  /**
   * We need to handle
   * fruit_hive_dyn/.hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1/
   *    _tmp.-ext-10002/color=Yellow
   * fruit_hive_dyn/.hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1/
   *    _tmp.-ext-10002/color=Yellow/000000_0
   * @param fullPath the path
   * @param hostNameScheme scheme
   * @return
   */
  private String parseHiveV1(Path fullPath, String hostNameScheme) throws IOException {
    String path = fullPath.toString();
    String noPrefix = path.substring(hostNameScheme.length());
    int npIdx = noPrefix.indexOf(tempIdentifier);
    String objectName = "";
    if (npIdx >= 0) {
      if (npIdx == 0 || npIdx == 1 && noPrefix.startsWith("/")) {
        throw new IOException("Object name is missing");
      } else {
        //path matches pattern in javadoc
        objectName = noPrefix.substring(0, npIdx - 1);
        String objName = fullPath.getName();
        int ind = noPrefix.indexOf("/", noPrefix.indexOf(tempIdentifier));
        if (ind > 0) {
          String obj1 = noPrefix.substring(ind);
          if (obj1.startsWith("/") && obj1.startsWith("/" + HIVE_TMP1)) {
            int ind1 = obj1.indexOf("/", obj1.indexOf(HIVE_TMP1));
            if (ind1 > 0) {
              String obj2 = obj1.substring(ind1);
              return objectName + obj2.replace(HIVE_TMP1, "");
            }
            return objectName;
          } else if (obj1.startsWith("/") && obj1.startsWith("/" + HIVE_EXT1)) {
            int ind1 = obj1.indexOf("/", obj1.indexOf(HIVE_EXT1));
            if (ind1 > 0) {
              String obj2 = obj1.substring(ind1);
              return objectName + obj2.replace(HIVE_EXT1, "");
            }
            return objectName;
          } else if (obj1.startsWith("/") && obj1.startsWith("/" + TASK_HIVE_TMP1)) {
            int ind1 = obj1.indexOf("/", obj1.indexOf(TASK_HIVE_TMP1));
            String obj2 = obj1.substring(ind1);
            return objectName + obj2.replace(HIVE_TMP1, "");

          }
          return objectName + obj1;
        }
        return objectName;
      }
    }
    return noPrefix;
  }

  /**
   * We need to handle
   * /fruit_gil1/_DYN0.600389881457611886943120206775524854029/color=Green/
   *    _temporary/1/_temporary/attempt_1484176830822_0004_r_000003_0
   * @param fullPath the path
   * @param hostNameScheme scheme
   * @return
   */
  private String parseHcatalogV1(Path fullPath, String hostNameScheme) throws IOException {
    String path = fullPath.toString();
    String noPrefix = path.substring(hostNameScheme.length());
    int npIdx = noPrefix.indexOf(tempIdentifier);
    String objectName = "";
    if (npIdx >= 0) {
      if (npIdx == 0 || npIdx == 1 && noPrefix.startsWith("/")) {
        throw new IOException("Object name is missing");
      } else {
        //path matches pattern in javadoc
        objectName = noPrefix.substring(0, npIdx - 1);
        String objName = fullPath.getName();
        int ind = noPrefix.indexOf("/", noPrefix.indexOf(tempIdentifier));
        if (ind > 0) {
          String obj1 = noPrefix.substring(ind);
          if (obj1.startsWith("/") && obj1.startsWith("/" + HIVE_TMP1)) {
            int ind1 = obj1.indexOf("/", obj1.indexOf(HIVE_TMP1));
            String obj2 = obj1.substring(ind1);
            return objectName + obj2.replace(HIVE_TMP1, "");
          } else if (obj1.startsWith("/") && obj1.startsWith("/" + TASK_HIVE_TMP1)) {
            int ind1 = obj1.indexOf("/", obj1.indexOf(TASK_HIVE_TMP1));
            String obj2 = obj1.substring(ind1);
            return objectName + obj2.replace(HIVE_TMP1, "");

          }
          String obj2 = parseHadoopFOutputCommitterV1(new Path(hostNameScheme + obj1),
              true, hostNameScheme);
          return objectName + "/" + obj2;
        }
        return objectName;
      }
    }
    return noPrefix;
  }

  /**
   * We need to handle
   * fruit_hive_dyn/.hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1/
   *    _tmp.-ext-10002/color=Yellow
   * fruit_hive_dyn/.hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1/
   *    _tmp.-ext-10002/color=Yellow/000000_0
   * @param fullPath the path
   * @param hostNameScheme scheme
   * @return
   */
  private String parseHiveV2(Path fullPath, String hostNameScheme) throws IOException {
    String path = fullPath.toString();
    int ind1 = -1;
    if (path.contains(TASK_HIVE_TMP1)) {
      ind1 = path.indexOf(TASK_HIVE_TMP1);
    } else if (path.contains(HIVE_TMP1)) {
      ind1 = path.indexOf(HIVE_TMP1);
    }
    String fPart = path.substring(0, ind1);
    String sPart = path.substring(path.indexOf("/", ind1));
    return fPart + "-ext-10000" + sPart;
  }
}
