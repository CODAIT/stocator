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

/**
 *
 * Tuple class
 *
 * @param <X> first coordinate
 * @param <Y> second coordinate
 */
public class Tuple<X, Y> {

  /*
   * First coordinate
   */
  public final X x;
  /*
   * Second coordinate
   */
  public final Y y;

  /**
   * Constructor
   *
   * @param xT first coordinate
   * @param yT second coordinate
   */
  public Tuple(X xT, Y yT) {
    x = xT;
    y = yT;
  }

  @Override
  public int hashCode() {
    return 42;
  }

  @Override
  public boolean equals(Object obj) {
    Tuple<X, Y> t = (Tuple<X, Y>) obj;
    return x.equals(t.x) && y.equals(t.y);
  }

  public String getPartition() {
    return x + "=" + y;
  }
}
