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

package com.ibm.stocator.fs.cos;

import java.util.Properties;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnetimeInitialization {
  private static OnetimeInitialization sInstance = null;
  private static String sUserAgentName = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(OnetimeInitialization.class);

  // A private Constructor prevents any other class from instantiating.
  private OnetimeInitialization() {
    sUserAgentName = initUserAgentName();
  }

  public static synchronized OnetimeInitialization getInstance() {
    if (sInstance == null) {
      sInstance = new OnetimeInitialization();
    }
    return sInstance;
  }

  private static String initUserAgentName() {
    String userAgentName = "stocator";
    Properties prop = new Properties();
    InputStream in = OnetimeInitialization.class.getResourceAsStream("/stocator.properties");
    try {
      prop.load(in);
      userAgentName += " " + prop.getProperty("stocator.version");
    } catch (Exception e) {
      // Do nothing and just ignore the exception.  We'll end up with
      // a stocator signature that has no version number, but hopefully
      // we won't fail the run.
      LOG.warn(e.getMessage());
    } finally {
      try {
        in.close();
      } catch (Exception e) {
        // Do nothing as per the comment above.
        LOG.warn(e.getMessage());
      }
    }
    LOG.trace("userAgent = {}", userAgentName);
    return userAgentName;
  }

  // other methods protected by singleton-ness would be here...

  public String getUserAgentName() {
    return sUserAgentName;
  }
}
