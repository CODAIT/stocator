/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  (C) Copyright IBM Corp. 2020
 */

package com.ibm.stocator.test;

public class FlightsSchema {
  private int year;
  private int month;
  private int dayofMonth;
  private int dayOfWeek;
  private String depTime;
  private String cRSDepTime;
  private String arrTime;
  private String cRSArrTime;
  private String uniqueCarrier;
  private String flightNum;
  private String tailNum;
  private String actualElapsedTime;
  private String cRSElapsedTime;
  private String airTime;
  private String arrDelay;
  private String depDelay;
  private String origin;
  private String dest;
  private String distance;
  private String taxiIn;
  private String taxiOut;
  private String cancelled;
  private String cancellationCode;
  private String diverted;
  private String carrierDelay;
  private String weatherDelay;
  private String nASDelay;
  private String securityDelay;
  private String lateAircraftDelay;

  public int getYear() {
    return year;
  }

  public void setYear(int year) {
    this.year = year;
  }

  public int getMonth() {
    return month;
  }

  public void setMonth(int month) {
    this.month = month;
  }

  public int getDayofMonth() {
    return dayofMonth;
  }

  public void setDayofMonth(int dayofMonth) {
    this.dayofMonth = dayofMonth;
  }

  public int getDayOfWeek() {
    return dayOfWeek;
  }

  public void setDayOfWeek(int dayOfWeek) {
    this.dayOfWeek = dayOfWeek;
  }

  public String getDepTime() {
    return depTime;
  }

  public void setDepTime(String depTime) {
    this.depTime = depTime;
  }

  public String getcRSDepTime() {
    return cRSDepTime;
  }

  public void setcRSDepTime(String cRSDepTime) {
    this.cRSDepTime = cRSDepTime;
  }

  public String getArrTime() {
    return arrTime;
  }

  public void setArrTime(String arrTime) {
    this.arrTime = arrTime;
  }

  public String getcRSArrTime() {
    return cRSArrTime;
  }

  public void setcRSArrTime(String cRSArrTime) {
    this.cRSArrTime = cRSArrTime;
  }

  public String getUniqueCarrier() {
    return uniqueCarrier;
  }

  public void setUniqueCarrier(String uniqueCarrier) {
    this.uniqueCarrier = uniqueCarrier;
  }

  public String getFlightNum() {
    return flightNum;
  }

  public void setFlightNum(String flightNum) {
    this.flightNum = flightNum;
  }

  public String getTailNum() {
    return tailNum;
  }

  public void setTailNum(String tailNum) {
    this.tailNum = tailNum;
  }

  public String getActualElapsedTime() {
    return actualElapsedTime;
  }

  public void setActualElapsedTime(String actualElapsedTime) {
    this.actualElapsedTime = actualElapsedTime;
  }

  public String getcRSElapsedTime() {
    return cRSElapsedTime;
  }

  public void setcRSElapsedTime(String cRSElapsedTime) {
    this.cRSElapsedTime = cRSElapsedTime;
  }

  public String getAirTime() {
    return airTime;
  }

  public void setAirTime(String airTime) {
    this.airTime = airTime;
  }

  public String getArrDelay() {
    return arrDelay;
  }

  public void setArrDelay(String arrDelay) {
    this.arrDelay = arrDelay;
  }

  public String getDepDelay() {
    return depDelay;
  }

  public void setDepDelay(String depDelay) {
    this.depDelay = depDelay;
  }

  public String getOrigin() {
    return origin;
  }

  public void setOrigin(String origin) {
    this.origin = origin;
  }

  public String getDest() {
    return dest;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }

  public String getDistance() {
    return distance;
  }

  public void setDistance(String distance) {
    this.distance = distance;
  }

  public String getTaxiIn() {
    return taxiIn;
  }

  public void setTaxiIn(String taxiIn) {
    this.taxiIn = taxiIn;
  }

  public String getTaxiOut() {
    return taxiOut;
  }

  public void setTaxiOut(String taxiOut) {
    this.taxiOut = taxiOut;
  }

  public String getCancelled() {
    return cancelled;
  }

  public void setCancelled(String cancelled) {
    this.cancelled = cancelled;
  }

  public String getCancellationCode() {
    return cancellationCode;
  }

  public void setCancellationCode(String cancellationCode) {
    this.cancellationCode = cancellationCode;
  }

  public String getDiverted() {
    return diverted;
  }

  public void setDiverted(String diverted) {
    this.diverted = diverted;
  }

  public String getCarrierDelay() {
    return carrierDelay;
  }

  public void setCarrierDelay(String carrierDelay) {
    this.carrierDelay = carrierDelay;
  }

  public String getWeatherDelay() {
    return weatherDelay;
  }

  public void setWeatherDelay(String weatherDelay) {
    this.weatherDelay = weatherDelay;
  }

  public String getnASDelay() {
    return nASDelay;
  }

  public void setnASDelay(String nASDelay) {
    this.nASDelay = nASDelay;
  }

  public String getSecurityDelay() {
    return securityDelay;
  }

  public void setSecurityDelay(String securityDelay) {
    this.securityDelay = securityDelay;
  }

  public String getLateAircraftDelay() {
    return lateAircraftDelay;
  }

  public void setLateAircraftDelay(String lateAircraftDelay) {
    this.lateAircraftDelay = lateAircraftDelay;
  }

}
