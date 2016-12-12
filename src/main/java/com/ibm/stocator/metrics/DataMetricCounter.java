package com.ibm.stocator.metrics;

import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

public class DataMetricCounter {

  private static final Logger LOG = LoggerFactory.getLogger(DataMetricCounter.class);
  @Expose
  private final String objectName;
  @Expose
  private final String operation;
  @Expose
  private final long startTime;
  @Expose
  private final long taskId;
  @Expose
  private boolean isContainer;
  @Expose
  private long endTime;
  @Expose
  private long elapsedTime;
  @Expose
  private String clvRequestId;
  @Expose
  private final WorkingThread workingThread;

  private static Gson sGSON;

  {
    GsonBuilder builder = new GsonBuilder();
    builder.excludeFieldsWithoutExposeAnnotation();
    sGSON = builder.create();
  }

  public DataMetricCounter(String objName, String methodName, long requestStartTime) {
    objectName = objName;
    operation = methodName;
    startTime = requestStartTime;
    workingThread = new WorkingThread();
    TaskContext taskContext = TaskContext.get();
    taskId = (null == taskContext ? -1 : taskContext.taskAttemptId());
  }

  public DataMetricCounter(String objName, String methodName, long requestStartTime,
      boolean isAContainer) {
    this(objName, methodName, requestStartTime);
    isContainer = isAContainer;
  }

  public static class WorkingThread {
    @Expose
    private long numBytes = 0;
    @Expose
    private long numOperations = 0;
    @Expose
    private long blockedTime = 0;

    private long lastUpdated = 0;
    @Expose
    private long threadId = -1;

    public WorkingThread() {
      setLastUpdatedNow();
      threadId = Thread.currentThread().getId();
    }

    private void setLastUpdatedNow() {
      lastUpdated = DataMetricUtilities.getCurrentTimestamp();
    }

    public long getNumBytes() {
      return numBytes;
    }

    public void setNumBytes(long v) {
      numBytes = v;
      ++numOperations;
      setLastUpdatedNow();
    }

    public void addNumBytes(long v) {
      numBytes += v;
      ++numOperations;
      setLastUpdatedNow();
    }

    public long getNumOperations() {
      return numOperations;
    }

    public long getBlockedTime() {
      return blockedTime;
    }

    public void addBlockedTime(long d) {
      blockedTime += d;
    }

    public long getLastUpdated() {
      return lastUpdated;
    }

    public long getTID() {
      return threadId;
    }
  }

  public String finalizeAndGetCounterSummary() {
    updateCommonTimes();
    String counterSummaryJson = sGSON.toJson(this);
    StringBuilder sb = new StringBuilder("\", \"metrics\":");
    sb.append(counterSummaryJson);
    sb.append(",\"e\":\"");
    return sb.toString();
  }

  private void updateCommonTimes() {
    elapsedTime = workingThread.getLastUpdated() - startTime;
  }

  public String getObjectName() {
    return objectName;
  }

  public String getDescription() {
    return operation;
  }

  public long getTaskID() {
    return taskId;
  }

  public String getClvRequestId() {
    return clvRequestId;
  }

  public void setClvRequestId(String requestId) {
    clvRequestId = requestId;
  }

  public void updateCounter(long numBytes, long blockedTime) {
    WorkingThread t = workingThread;
    if (Thread.currentThread().getId() != t.threadId) {
      LOG.error(String.format("Current thread is %d, counter thread is %d",
          Thread.currentThread().getId(), t.threadId));
    }
    t.addBlockedTime(blockedTime);
    t.addNumBytes(numBytes);
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

}
