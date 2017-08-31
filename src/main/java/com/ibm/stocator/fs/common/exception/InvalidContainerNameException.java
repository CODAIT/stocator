/**
 * (C) Copyright IBM Corp. 2016, 2017
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

package com.ibm.stocator.fs.common.exception;

public class InvalidContainerNameException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public static final String DESCRIPTION = "Bucket name is invalid";

  /**
   * Default constructor
   */
  public InvalidContainerNameException() {
    super(DESCRIPTION);
  }

  /**
   *
   * @param additionalInfo additional details to the exception message
   */
  public InvalidContainerNameException(String additionalInfo) {
    super(DESCRIPTION + ": " + additionalInfo);
  }

}
