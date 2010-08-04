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
 * Represents the change of the size or scale of a column.
 * 
 * @version $Revision: $
 */
public class ColumnSizeChange extends TableChangeImplBase implements ColumnChange {
  /** The column. */
  private Column _column;
  /** The new size. */
  private int _newSize;
  /** The new scale. */
  private Integer _newScale;
  private int _oldSize = 0;
  private Integer _oldScale;
  private String _tablename;
  private String _columnname;

  /**
   * Creates a new change object.
   * 
   * @param table
   *          The table of the column
   * @param column
   *          The column
   * @param newSize
   *          The new size
   * @param newScale
   *          The new scale
   */

  public ColumnSizeChange() {

  }

  public ColumnSizeChange(Table table, Column column, int newSize, Integer newScale) {
    super(table);
    _column = column;
    _newSize = newSize;
    _newScale = newScale;
    _columnname = column.getName();
    _tablename = table.getName();
    _oldSize = column.getSizeAsInt();
    _oldScale = column.getScale();
  }

  /**
   * Returns the column.
   * 
   * @return The column
   */
  public Column getChangedColumn() {
    return _column;
  }

  /**
   * Returns the new size of the column.
   * 
   * @return The new size
   */
  public int getNewSize() {
    return _newSize;
  }

  /**
   * Returns the new scale of the column.
   * 
   * @return The new scale
   */
  public Integer getNewScale() {
    return _newScale;
  }

  /**
   * {@inheritDoc}
   */
  public void apply(Database database, boolean caseSensitive) {
    Table table = _table;
    if (table == null)
      table = database.findTable(_tablename, caseSensitive);

    // We will not try to apply the change if the table doesn't exist in the model
    // This could happen in update.database.mod if a configuration script has this change
    // which makes a reference to an object which doesn't belong to the module
    if (table == null) {
      return;
    }
    Column column = _column;
    if (column == null)
      column = table.findColumn(_columnname, caseSensitive);
    if (column != null)
      column.setSizeAndScale(_newSize, _newScale);

  }

  public void applyInReverse(Database database, boolean caseSensitive) {
    if (_oldSize == 0) {
      System.out
          .println("Error while applying a ColumnSizeChange in reverse (the old size of the column is 0). Exporting the configuration script again should fix this problem.");
    }
    Table table = _table;
    if (table == null)
      table = database.findTable(_tablename, caseSensitive);
    if (table == null) {
      System.out.println("Table wasn't found in database.");
      return;
    }

    Column column = _column;
    if (column == null)
      column = table.findColumn(_columnname, caseSensitive);
    if (column != null)
      column.setSizeAndScale(_oldSize, _oldScale);
    else
      System.out.println("Column " + getChangedColumn().getName() + " of table "
          + getChangedTable().getName() + " wasn't found in the database.");
  }

  public String getTablename() {
    return _tablename;
  }

  public void setTablename(String tablename) {
    _tablename = tablename;
  }

  public String getColumnname() {
    return _columnname;
  }

  public void setColumnname(String columnname) {
    _columnname = columnname;
  }

  public void setNewSize(int newSize) {
    _newSize = newSize;
  }

  public void setNewScale(String newScale) {
    _newScale = new Integer(newScale);
  }

  @Override
  public String toString() {
    String name;
    if (_column == null)
      name = "null";
    else
      name = _column.getName();
    return "ColumnSizeChange. Column: " + name;
  }

  public int getOldSize() {
    return _oldSize;
  }

  public void setOldSize(int size) {
    _oldSize = size;
  }

  public Integer getOldScale() {
    return _oldScale;
  }

  public void setOldScale(Integer scale) {
    _oldScale = scale;
  }

}
