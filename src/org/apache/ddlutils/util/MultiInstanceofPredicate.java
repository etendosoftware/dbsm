package org.apache.ddlutils.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.collections4.Predicate;

/**
 * A predicate that tests whether the object is of one of the configured types.
 * 
 * @version $Revision: $
 */
public class MultiInstanceofPredicate implements Predicate {
  /** The types to check. */
  private Class[] _typesToCheck;

  /**
   * Creates a new predicate.
   * 
   * @param typesToCheck
   *          The types to check
   */
  public MultiInstanceofPredicate(Class[] typesToCheck) {
    _typesToCheck = typesToCheck;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean evaluate(Object obj) {
    if ((_typesToCheck == null) || (_typesToCheck.length == 0)) {
      return true;
    } else {
      Class typeOfObj = obj.getClass();

      for (int idx = 0; idx < _typesToCheck.length; idx++) {
        if (_typesToCheck[idx].isAssignableFrom(typeOfObj)) {
          return true;
        }
      }
      return false;
    }
  }

}
