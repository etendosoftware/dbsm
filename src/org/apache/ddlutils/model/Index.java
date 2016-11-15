package org.apache.ddlutils.model;

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

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.ddlutils.PlatformInfo;

/**
 * Base class for indices.
 * 
 * @version $Revision: $
 */
public class Index implements ConstraintObject, Cloneable, Serializable {
  /** The name of the index. */
  protected String _name;
  /** Whether the index is unique */
  protected boolean _unique = false;
  /** The where clause expression used for partial indexing **/
  protected String _whereClause;
  /** The columns making up the index. */
  protected ArrayList _columns = new ArrayList();

  /**
   * {@inheritDoc}
   */
  public String getName() {
    return _name;
  }

  /**
   * {@inheritDoc}
   */
  public void setName(String name) {
    _name = name;
  }

  /**
   * {@inheritDoc}
   */
  public int getColumnCount() {
    return _columns.size();
  }

  /**
   * {@inheritDoc}
   */
  public IndexColumn getColumn(int idx) {
    return (IndexColumn) _columns.get(idx);
  }

  /**
   * {@inheritDoc}
   */
  public IndexColumn[] getColumns() {
    return (IndexColumn[]) _columns.toArray(new IndexColumn[_columns.size()]);
  }

  /**
   * {@inheritDoc}
   */
  public boolean hasColumn(Column column) {
    for (int idx = 0; idx < _columns.size(); idx++) {
      IndexColumn curColumn = getColumn(idx);

      if (column.equals(curColumn.getColumn())) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  public void addColumn(IndexColumn column) {
    if (column != null) {
      for (int idx = 0; idx < _columns.size(); idx++) {
        IndexColumn curColumn = getColumn(idx);

        if (curColumn.getOrdinalPosition() > column.getOrdinalPosition()) {
          _columns.add(idx, column);
          return;
        }
      }
      _columns.add(column);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void removeColumn(IndexColumn column) {
    _columns.remove(column);
  }

  /**
   * {@inheritDoc}
   */
  public void removeColumn(int idx) {
    _columns.remove(idx);
  }

  public boolean isUnique() {
    return _unique;
  }

  public void setUnique(boolean unique) {
    _unique = unique;
  }

  /**
   * {@inheritDoc}
   */
  public String getWhereClause() {
    return _whereClause;
  }

  /**
   * {@inheritDoc}
   */
  public void setWhereClause(String whereClause) {
    _whereClause = whereClause;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object obj) {
    if (obj instanceof Index) {
      Index other = (Index) obj;

      return new EqualsBuilder().append(_name, other._name).append(_unique, other._unique)
          .append(_whereClause, other._whereClause).append(_columns, other._columns).isEquals();
    } else {
      return false;
    }
  }

  /**
   * Checks if two indexes are equals, taking into account if in the current dbms empty strings are
   * treated as NULL
   * 
   * @param other
   *          the index that will be compared with the current class
   * @param platformInfo
   *          platform information of the current dbms
   * @return true if the two indexes are equal, false otherwise
   */
  @SuppressWarnings("unchecked")
  public boolean equals(Index other, PlatformInfo platformInfo) {
    if (platformInfo == null) {
      return equals(other);
    } else {
      EqualsBuilder equalsBuilder = new EqualsBuilder().append(_name, other._name).append(_unique,
          other._unique);
      if (platformInfo.isPartialIndexesSupported()) {
        equalsBuilder.append(_whereClause, other._whereClause);
      }
      return equalsBuilder.isEquals() && columnsAreEqual(_columns, other._columns, platformInfo);
    }
  }

  /**
   * Checks if two index column arrays are equal, taking into account if in the current database
   * empty strings are treated as NULL
   *
   * @param columns
   *          the first index column arrays
   * @param otherColumns
   *          the second index column array
   * @param platformInfo
   *          platform information of the current dbms
   * @return true if the two index column arrays are equal, false otherwise
   */
  private boolean columnsAreEqual(ArrayList<IndexColumn> columns,
      ArrayList<IndexColumn> otherColumns, PlatformInfo platformInfo) {
    for (int idx = 0; idx < getColumnCount(); idx++) {
      if (!columns.get(idx).equals(otherColumns.get(idx), platformInfo)) {
        return false;
      }
    }
    return true;
  }

  public boolean equalsIgnoreCase(Index other) {
    PlatformInfo platformInfo = null;
    return equals(other, platformInfo);
  }

  /**
   * Compares two indexes without taking into account the casing of their names or of their columns
   * names. It also takes into account if in the current database empty strings are treated as null
   * (relevant when comparing the functions of the index columns)
   * 
   * @param other
   *          the index being compared with the current one
   * @param platformInfo
   *          platform information of the current dbms
   * @return true if the two indexes are equal
   */
  public boolean equalsIgnoreCase(Index other, PlatformInfo platformInfo) {
    if (other instanceof Index) {
      Index otherIndex = (Index) other;

      boolean checkName = (_name != null) && (_name.length() > 0) && (otherIndex._name != null)
          && (otherIndex._name.length() > 0);

      if ((!checkName || _name.equalsIgnoreCase(otherIndex._name))
          && (getColumnCount() == otherIndex.getColumnCount())) {
        if (_unique != other._unique) {
          return false;
        }
        if (platformInfo.isPartialIndexesSupported() && !isSameWhereClause(other)) {
          return false;
        }
        for (int idx = 0; idx < getColumnCount(); idx++) {
          if (!getColumn(idx).equalsIgnoreCase(otherIndex.getColumn(idx), platformInfo)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  public boolean isSameWhereClause(Index otherIndex) {
    if (_whereClause != null) {
      return _whereClause.equalsIgnoreCase(otherIndex._whereClause);
    }
    return otherIndex._whereClause == null;
  }

  /**
   * {@inheritDoc}
   */
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(_name).append(_unique).append(_whereClause)
        .append(_columns).toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  public String toString() {
    StringBuffer result = new StringBuffer();

    result.append("Index [name=");
    result.append(getName());
    result.append("; unique =");
    result.append(isUnique());
    result.append("; where clause =");
    result.append(getWhereClause());
    result.append("; ");
    result.append(getColumnCount());
    result.append(" columns]");

    return result.toString();
  }

  /**
   * {@inheritDoc}
   */
  public String toVerboseString() {
    StringBuffer result = new StringBuffer();

    result.append("Index [name=");
    result.append(getName());
    result.append("; unique =");
    result.append(isUnique());
    result.append("; where clause =");
    result.append(getWhereClause());
    result.append("] columns:");
    for (int idx = 0; idx < getColumnCount(); idx++) {
      result.append(" ");
      result.append(getColumn(idx).toString());
    }

    return result.toString();
  }

  /**
   * {@inheritDoc}
   */
  public Object clone() throws CloneNotSupportedException {
    Index result = new Index();

    result._name = _name;
    result._unique = _unique;
    result._whereClause = _whereClause;
    result._columns = (ArrayList) _columns.clone();

    return result;
  }
}
