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

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

/**
 * Represents the change of the required constraint of a column. Since it is a boolean value, this
 * means the required constraint will simply be toggled.
 * 
 * @version $Revision: $
 */
public class ColumnRequiredChange extends TableChangeImplBase implements ColumnChange {
  /** The column. */
  private Column _column;
  private String _columnName;
  private String _tableName;
  private Boolean _required;

  public ColumnRequiredChange() {

  }

  /**
   * Creates a new change object.
   * 
   * @param table
   *          The table of the column
   * @param column
   *          The column
   */
  public ColumnRequiredChange(Table table, Column column) {
    super(table);
    _column = column;
  }

  /**
   * Returns the column.
   * 
   * @return The column
   */
  public Column getChangedColumn() {
    return _column;
  }

  public String getColumnName() {
    if (_columnName != null) {
      return _columnName;
    }
    if (_column != null) {
      return _column.getName();
    }
    return null;
  }

  public void setColumnName(String columnName) {
    _columnName = columnName;
  }

  public String getTableName() {
    if (_tableName != null) {
      return _tableName;
    }
    if (_table != null) {
      return _table.getName();
    }
    return null;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public boolean getRequired() {
    if (_required != null) {
      return !(_required.booleanValue());
    }
    return !(_column.isRequired());
  }

  public void setRequired(boolean required) {
    _required = !required;
  }

  /**
   * {@inheritDoc}
   */
  public void apply(Database database, boolean caseSensitive) {
    Table table = database.findTable(_tableName != null ? _tableName : getChangedTable().getName(),
        caseSensitive);
    Column column = table.findColumn(_columnName != null ? _columnName : _column.getName(),
        caseSensitive);

    boolean required = getRequired();
    if (_column != null) {
      _column.setRequired(required);
    }
    column.setRequired(required);
  }

  @Override
  public String toString() {
    return "ColumnRequiredChange. Column: " + getTableName() + "." + getColumnName()
        + " - required: " + getRequired();
  }

  public void applyInReverse(Database database, boolean caseSensitive) {
    Table table = database.findTable(_tableName != null ? _tableName : getChangedTable().getName(),
        caseSensitive);
    Column column = table.findColumn(_columnName != null ? _columnName : _column.getName(),
        caseSensitive);

    column.setRequired(!getRequired());

  }
}
