package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

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

/**
 * Represents the removal of an index from a table.
 * 
 * @version $Revision: $
 */
public class RemoveCheckChange extends TableChangeImplBase {

  /** The check to be removed. */
  private Check _check;

  private String _checkName;
  private String _tableName;

  public RemoveCheckChange() {

  }

  /**
   * Creates a new change object.
   * 
   * @param table
   *          The table to remove the check from
   * @param index
   *          The check
   */
  public RemoveCheckChange(Table table, Check check) {
    super(table);
    _check = check;
    _checkName = check.getName();
    _tableName = table.getName();
  }

  public RemoveCheckChange(String tableName, String checkName) {
    _checkName = checkName;
    _tableName = tableName;
  }

  /**
   * Returns the check.
   * 
   * @return The check
   */
  public Check getCheck() {
    return _check;
  }

  public void setCheck(Database model) {
    Table table = model.findTable(_tableName);
    if (table != null)
      _check = table.findCheck(_checkName);
  }

  public void setCheck(Check check) {
    _check = check;
  }

  /**
   * {@inheritDoc}
   */
  public void apply(Database database, boolean caseSensitive) {
    Table table = null;

    if (_tableName == null)
      table = database.findTable(getChangedTable().getName(), caseSensitive);
    else
      table = database.findTable(_tableName);

    Check check = null;

    if (_checkName == null)
      check = table.findCheck(_check.getName(), caseSensitive);
    else
      check = table.findCheck(_checkName);

    table.removeCheck(check);
  }

  public void applyInReverse(Database database, boolean caseSensitive) {
    if (_check == null) {
      System.out
          .println("Error while applying a RemoveCheckChange (the check wasn't found in the configuration script). Exporting the configuration script again should fix this problem.");
      return;
    }
    Table table = null;

    if (_tableName == null)
      table = database.findTable(getChangedTable().getName(), caseSensitive);
    else
      table = database.findTable(_tableName);

    // We will not try to apply the change if the table doesn't exist in the model
    // This could happen in update.database.mod if a configuration script has this change
    // which makes a reference to an object which doesn't belong to the module
    if (table != null) {
      table.addCheck(_check);
    }
  }

  public String getCheckName() {
    return _checkName;
  }

  public void setCheckName(String checkName) {
    _checkName = checkName;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  @Override
  public String toString() {
    return "RemoveCheckChange. Name: " + _checkName;
  }
}
