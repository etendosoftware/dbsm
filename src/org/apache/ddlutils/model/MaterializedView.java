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

package org.apache.ddlutils.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Represents a database materialized view.
 * 
 * @version $Revision$
 */
public class MaterializedView implements IndexableModelObject, StructureObject, Cloneable {

  private View view;

  private ArrayList<Column> columns = new ArrayList<>();
  private ArrayList<Index> indices = new ArrayList<>();

  /** Creates a new instance of MaterializedViewView */
  public MaterializedView() {
    this(null);
  }

  public MaterializedView(String name) {
    this.view = new View(name);
  }

  /**
   * Returns the name of this view.
   * 
   * @return The name
   */
  @Override
  public String getName() {
    return view.getName();
  }

  /**
   * Sets the name of this view.
   * 
   * @param name
   *          The name
   */
  public void setName(String name) {
    this.view.setName(name);
  }

  /**
   * Returns the statement of this view.
   * 
   * @return The statement
   */
  public String getStatement() {
    return view.getStatement();
  }

  /**
   * Sets the statement of this view.
   * 
   * @param statement
   *          The statement
   */
  public void setStatement(String statement) {
    this.view.setStatement(statement);
  }

  /**
   * Returns the number of columns in this table.
   * 
   * @return The number of columns
   */
  public int getColumnCount() {
    return columns.size();
  }

  /**
   * Returns the column at the specified position.
   * 
   * @param idx
   *          The column index
   * @return The column at this position
   */
  public Column getColumn(int idx) {
    return columns.get(idx);
  }

  /**
   * Returns the columns in this table.
   * 
   * @return The columns
   */
  public Column[] getColumns() {
    return columns.toArray(new Column[columns.size()]);
  }

  /**
   * Adds the given column.
   * 
   * @param column
   *          The column
   */
  public void addColumn(Column column) {
    if (column != null) {
      column.setRequired(false);
      column.setType("OTHER");
      column.setSize(null);
      columns.add(column);
    }
  }

  /**
   * Adds the given column at the specified position.
   * 
   * @param idx
   *          The index where to add the column
   * @param column
   *          The column
   */
  public void addColumn(int idx, Column column) {
    if (column != null) {
      column.setRequired(false);
      column.setType("OTHER");
      column.setSize(null);
      columns.add(idx, column);
    }
  }

  /**
   * Adds the column after the given previous column.
   * 
   * @param previousColumn
   *          The column to add the new column after; use <code>null</code> for adding at the begin
   * @param column
   *          The column
   */
  public void addColumn(Column previousColumn, Column column) {
    if (column != null) {
      column.setRequired(false);
      column.setType("OTHER");
      column.setSize(null);
      if (previousColumn == null) {
        columns.add(0, column);
      } else {
        columns.add(columns.indexOf(previousColumn), column);
      }
    }
  }

  /**
   * Adds the given columns.
   * 
   * @param newColumns
   *          The columns
   */
  public void addColumns(Collection<Column> newColumns) {
    for (Iterator<Column> it = newColumns.iterator(); it.hasNext();) {
      addColumn(it.next());
    }
  }

  /**
   * Removes the given column.
   * 
   * @param column
   *          The column to remove
   */
  public void removeColumn(Column column) {
    if (column != null) {
      columns.remove(column);
    }
  }

  /**
   * Removes the indicated column.
   * 
   * @param idx
   *          The index of the column to remove
   */
  public void removeColumn(int idx) {
    columns.remove(idx);
  }

  @Override
  public Column findColumn(String columnName) {
    return columns.stream()
        .filter(c -> c.getName().equalsIgnoreCase(columnName))
        .findFirst()
        .orElseGet(() -> null);
  }

  /**
   * Returns the number of indices.
   * 
   * @return The number of indices
   */
  public int getIndexCount() {
    return indices.size();
  }

  /**
   * Returns the index at the specified position.
   * 
   * @param idx
   *          The position
   * @return The index
   */
  public Index getIndex(int idx) {
    return indices.get(idx);
  }

  /**
   * Adds the given index.
   * 
   * @param index
   *          The index
   */
  public void addIndex(Index index) {
    if (index != null) {
      indices.add(index);
    }
  }

  /**
   * Adds the given index at the specified position.
   * 
   * @param idx
   *          The position to add the index at
   * @param index
   *          The index
   */
  public void addIndex(int idx, Index index) {
    if (index != null) {
      indices.add(idx, index);
    }
  }

  /**
   * Adds the given indices.
   * 
   * @param newIndices
   *          The indices
   */
  public void addIndices(Collection<Index> newIndices) {
    for (Iterator<Index> it = newIndices.iterator(); it.hasNext();) {
      addIndex((Index) it.next());
    }
  }

  /**
   * Returns the indices of this table.
   * 
   * @return The indices
   */
  public Index[] getIndices() {
    return indices.toArray(new Index[indices.size()]);
  }

  /**
   * Removes the given index.
   * 
   * @param index
   *          The index to remove
   */
  public void removeIndex(Index index) {
    if (index != null) {
      indices.remove(index);
    }
  }

  /**
   * Removes the indicated index.
   * 
   * @param idx
   *          The position of the index to remove
   */
  public void removeIndex(int idx) {
    indices.remove(idx);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object clone() throws CloneNotSupportedException {

    MaterializedView result = (MaterializedView) super.clone();

    result.view = (View) view.clone();
    result.columns = new ArrayList<>(columns);
    result.indices = new ArrayList<>(indices);

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MaterializedView) {
      MaterializedView other = (MaterializedView) obj;
      return new EqualsBuilder().append(view.getName(), other.view.getName())
          .append(view.getStatement(), other.view.getStatement())
          .append(columns, other.columns)
          .append(indices, other.indices)
          .isEquals();
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(view).append(columns).append(indices).toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();

    result.append("MaterializedView [");
    if ((getName() != null) && (getName().length() > 0)) {
      result.append("name=");
      result.append(getName());
      result.append("; ");
    }
    result.append("]");

    return result.toString();
  }

  public String toVerboseString() {
    StringBuilder result = new StringBuilder();

    result.append("MaterializedView [");
    if ((getName() != null) && (getName().length() > 0)) {
      result.append("name=");
      result.append(getName());
      result.append("; ");
    }
    result.append("statement=");
    result.append(getStatement());
    result.append("]");

    return result.toString();
  }

  public View getView() {
    return view;
  }
}
