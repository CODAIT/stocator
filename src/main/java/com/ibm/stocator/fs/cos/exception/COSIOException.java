/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.cos.exception;

import java.util.Map;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class COSIOException extends COSServiceIOException {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Instantiate.
   *
   * @param operation operation which triggered this
   * @param cause the underlying cause
   */
  public COSIOException(String operation, AmazonS3Exception cause) {
    super(operation, cause);
  }

  public AmazonS3Exception getCause() {
    return (AmazonS3Exception) super.getCause();
  }

  public String getErrorResponseXml() {
    return getCause().getErrorResponseXml();
  }

  public Map<String, String> getAdditionalDetails() {
    return getCause().getAdditionalDetails();
  }

  public String getExtendedRequestId() {
    return getCause().getExtendedRequestId();
  }

}
