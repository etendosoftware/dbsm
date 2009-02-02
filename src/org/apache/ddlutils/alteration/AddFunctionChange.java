package org.apache.ddlutils.alteration;

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

import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;

public class AddFunctionChange implements ModelChange {

  /** The new function. */
  private Function _newFunction;

  /**
   * Creates a new change object.
   * 
   * @param newFunction
   *          The new function
   */
  public AddFunctionChange(Function newFunction) {
    _newFunction = newFunction;
  }

  /**
   * Returns the new function.
   * 
   * @return The new function
   */
  public Function getNewFunction() {
    return _newFunction;
  }

  /**
   * {@inheritDoc}
   */
  public void apply(Database database, boolean caseSensitive) {
    try {
      database.addFunction((Function) _newFunction.clone());
    } catch (CloneNotSupportedException ex) {
      throw new DdlUtilsException(ex);
    }
  }

  @Override
  public String toString() {
    return "AddFunctionChange. Name: " + _newFunction.getName();
  }
}
