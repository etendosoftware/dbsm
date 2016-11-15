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
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;

/**
 * Represents the removal of an index from a table.
 * 
 * @version $Revision: $
 */
public class RemoveIndexChange extends TableChangeImplBase {
  /** The index to be removed. */
  private Index _index;
  private String _indexName;
  private String _tableName;

  public RemoveIndexChange() {

  }

  /**
   * Creates a new change object.
   * 
   * @param table
   *          The table to remove the index from
   * @param index
   *          The index
   */
  public RemoveIndexChange(Table table, Index index) {
    super(table);
    _index = index;
    _tableName = table.getName();
    _indexName = index.getName();
  }

  /**
   * Returns the index.
   * 
   * @return The index
   */
  public Index getIndex() {
    return _index;
  }

  /**
   * Sets the index to be removed.
   * 
   * @param index
   *          the index to be removed
   */
  public void setIndex(Index index) {
    _index = index;
  }

  /**
   * Returns the name of the removed index.
   * 
   * @return The index name
   */
  public String getIndexName() {
    return _indexName;
  }

  /**
   * Sets the name of the removed index.
   * 
   * @param indexName
   *          the name of the index
   */
  public void setIndexName(String indexName) {
    _indexName = indexName;
  }

  /**
   * Returns the name of the table where the index is removed from.
   * 
   * @return The table name
   */
  public String getTableName() {
    return _tableName;
  }

  /**
   * Sets the name of the table where the index is removed from.
   * 
   * @param tableName
   *          the name of the table
   */
  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  /**
   * {@inheritDoc}
   */
  public void apply(Database database, boolean caseSensitive) {
    String tableName = _tableName != null ? _tableName : getChangedTable().getName();
    String indexName = _indexName != null ? _indexName : _index.getName();
    Table table = database.findTable(tableName, caseSensitive);
    Index index = table.findIndex(indexName, caseSensitive);

    table.removeIndex(index);
  }

  public void applyInReverse(Database database, boolean caseSensitive) {
    if (_index == null) {
      System.out
          .println("Error while applying a RemoveIndexChange (the index wasn't found in the configuration script). Exporting the configuration script again should fix this problem.");
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
      table.addIndex(_index);
    }
  }

  @Override
  public String toString() {
    String indexName = _indexName != null ? _indexName : _index.getName();
    return "RemoveIndexChange. Name: " + indexName;
  }
}
