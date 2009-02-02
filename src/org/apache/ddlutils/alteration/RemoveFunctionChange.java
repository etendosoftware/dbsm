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

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;

/**
 * Represents the removal of a table from a model.
 * 
 * @version $Revision: $
 */
public class RemoveFunctionChange implements ModelChange {

  /** The function. */
  private Function _function;

  /**
   * Creates a remove change object.
   * 
   * @param function
   *          The function
   */
  public RemoveFunctionChange(Function function) {
    _function = function;
  }

  /**
   * Returns the function.
   * 
   * @return The function
   */
  public Function getFunction() {
    return _function;
  }

  /**
   * {@inheritDoc}
   */
  public void apply(Database database, boolean caseSensitive) {
    Function function = database.findFunction(_function.getName(), caseSensitive);

    database.removeFunction(function);
  }

  @Override
  public String toString() {
    return "RemoveFunctionChange. Name: " + _function.getName();
  }
}
