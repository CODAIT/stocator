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

package com.ibm.stocator.fs.common.exception;

import java.io.IOException;

/**
 *Exception for closed connection to the object store
 */
public class ConnectionClosedException extends IOException {

  public static final String DESCRIPTION = "Connection is closed";

  /**
   * Default constructor
   */
  public ConnectionClosedException() {
    super(DESCRIPTION);
  }

  /**
   *
   * @param additionalInfo additional details to the exception message
   */
  public ConnectionClosedException(String additionalInfo) {
    super(DESCRIPTION + ": " + additionalInfo);
  }

}
