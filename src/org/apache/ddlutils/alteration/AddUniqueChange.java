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
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Unique;

/**
 * Represents the addition of an unique to a table.
 * 
 * @version $Revision: $
 */
public class AddUniqueChange extends TableChangeImplBase {
  /** The new unique. */
  private Unique _newUnique;

  /**
   * Creates a new change object.
   * 
   * @param table
   *          The table to add the unique to
   * @param newUnique
   *          The new unique
   */
  public AddUniqueChange(Table table, Unique newUnique) {
    super(table);
    _newUnique = newUnique;
  }

  /**
   * Returns the new unique.
   * 
   * @return The new unique
   */
  public Unique getNewUnique() {
    return _newUnique;
  }

  /**
   * {@inheritDoc}
   */
  public void apply(Database database, boolean caseSensitive) {
    Unique newUnique = null;

    try {
      newUnique = (Unique) _newUnique.clone();
    } catch (CloneNotSupportedException ex) {
      throw new DdlUtilsException(ex);
    }
    database.findTable(getChangedTable().getName(), caseSensitive).addUnique(newUnique);
  }

  @Override
  public String toString() {
    return "AddUniqueChange. Name: " + _newUnique.getName();
  }
}
