package com.ibm.stocator.metrics;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMetricUtilities {
  private static boolean sENABLED = false;
  public static final String DSNET_REQUEST_ID_HEADER = "X-Clv-Request-Id";

  private static final Logger LOG = LoggerFactory.getLogger(DataMetricUtilities.class);

  private DataMetricUtilities() {
  }

  public static void enableMetricsLogging() {
    sENABLED = true;
  }

  public static boolean isMetricsLoggingEnabled() {
    return sENABLED;
  }

  public static long getCurrentTimestamp() {
    long currentTime = 0L;
    if (sENABLED) {
      currentTime = System.currentTimeMillis();
    }
    return currentTime;
  }

  public static DataMetricCounter buildDataMetricCounter(long requestStartTime, String objectName,
      String methodName) {
    DataMetricCounter dataMetricCounter = null;
    if (sENABLED) {
      dataMetricCounter = new DataMetricCounter(objectName, methodName, requestStartTime);
    }
    return dataMetricCounter;
  }

  public static void updateDataMetricCounter(DataMetricCounter dataMetricCounter, long value,
      long requestStartTime) {
    if (sENABLED) {
      long requestDuration = DataMetricUtilities.getCurrentTimestamp() - requestStartTime;
      if (value < 0) {
        LOG.warn("Negative values for data metric are not allowed");
        return;
      }
      if (null == dataMetricCounter) {
        LOG.warn("dataMetricCounter is null. Not persisting the counter.");
        return;
      }
      dataMetricCounter.updateCounter(value, requestDuration);
    }
  }

  public static void updateDataMetricCounter(DataMetricCounter dataMetricCounter,
      HttpResponse httpResponse) {
    if (sENABLED) {
      Header clvRequestIdHeader = httpResponse
          .getFirstHeader(DataMetricUtilities.DSNET_REQUEST_ID_HEADER);
      if ((null != clvRequestIdHeader) && (null != dataMetricCounter)) {
        dataMetricCounter.setClvRequestId(clvRequestIdHeader.getValue());
      }
    }
  }

  public static String getCounterSummary(DataMetricCounter dataMetricCounter) {
    if (sENABLED) {
      if (null == dataMetricCounter) {
        LOG.error("Metric counter is null");
        return "";
      }
      return dataMetricCounter.finalizeAndGetCounterSummary();
    } else {
      return "";
    }
  }

  public static String getCounterSummary(DataMetricCounter dataMetricCounter,
      Header clvRequestIdHeader) {
    if (sENABLED) {
      if (null == dataMetricCounter) {
        LOG.error("Data metric counter is null");
        return "";
      }
      if (null != clvRequestIdHeader) {
        dataMetricCounter.setClvRequestId(clvRequestIdHeader.getValue());
      }
      return getCounterSummary(dataMetricCounter);
    } else {
      return "";
    }
  }

  public static long initMetric(String objectName, String logMessage) {
    return getCurrentTimestamp();
  }

  public static String closeMetric(String objectName, String methodName, boolean isContainer,
      long requestStartTime) {
    if (sENABLED) {
      DataMetricCounter dataMetricCounter = new DataMetricCounter(objectName, methodName,
          requestStartTime, isContainer);
      updateDataMetricCounter(dataMetricCounter, 0, requestStartTime);
      return getCounterSummary(dataMetricCounter);
    } else {
      return "";
    }
  }

}
