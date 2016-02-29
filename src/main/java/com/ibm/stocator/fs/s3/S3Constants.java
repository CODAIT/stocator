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

import com.ibm.stocator.fs.common.Constants;

/**
 * Constants used in the S3 REST protocol.
 */
public class S3Constants {
  public static final String S3_CONTAINER_PROPERTY = ".s3";

  public static final String BUCKET = ".bucket";
  public static final String S3_BUCKET_PROPERTY = Constants.S3_SERVICE_PREFIX + BUCKET;

  public static final String ACCESS_KEY = ".access";
  public static final String S3_ACCESS_KEY_PROPERTY = Constants.S3_SERVICE_PREFIX + ACCESS_KEY;

  public static final String SECRET_KEY = ".secret";
  public static final String S3_SECRET_KEY_PROPERTY = Constants.S3_SERVICE_PREFIX + SECRET_KEY;

  public static final String REGION = ".region";
  public static final String S3_REGION_PROPERTY = Constants.S3_SERVICE_PREFIX + REGION;

  public static final String BLOCK_SIZE = ".block.size";
  public static final String S3_BLOCK_SIZE_PROPERTY = Constants.S3_SERVICE_PREFIX + BLOCK_SIZE;

}
