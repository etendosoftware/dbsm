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
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.ddlutils.platform.ExcludeFilter;

/**
 * Represents a table in the database model.
 * 
 * @version $Revision: 494338 $
 */
public class Table implements StructureObject, Serializable, Cloneable {
  /** Unique ID for serialization purposes. */
  private static final long serialVersionUID = -5541154961302342608L;

  /** The catalog of this table as read from the database. */
  private String _catalog = null;
  /** The table's schema. */
  private String _schema = null;
  /** The name. */
  private String _name = null;
  /** the name of the primary key */
  private String _primaryKey = null;
  /** A desription of the table. */
  private String _description = null;
  /** The table's type as read from the database. */
  private String _type = null;
  /** The columns in this table. */
  private ArrayList _columns = new ArrayList();
  /** The foreign keys associated to this table. */
  private ArrayList _foreignKeys = new ArrayList();
  /** The indices applied to this table. */
  private ArrayList _indices = new ArrayList();
  /** The uniques applied to this table. */
  private ArrayList _uniques = new ArrayList();
  /** The constraint checks applied to this table. */
  private ArrayList _checks = new ArrayList();

  /**
   * Returns the catalog of this table as read from the database.
   * 
   * @return The catalog
   */
  public String getCatalog() {
    return _catalog;
  }

  /**
   * Sets the catalog of this table.
   * 
   * @param catalog
   *          The catalog
   */
  public void setCatalog(String catalog) {
    _catalog = catalog;
  }

  /**
   * Returns the schema of this table as read from the database.
   * 
   * @return The schema
   */
  public String getSchema() {
    return _schema;
  }

  /**
   * Sets the schema of this table.
   * 
   * @param schema
   *          The schema
   */
  public void setSchema(String schema) {
    _schema = schema;
  }

  /**
   * Returns the type of this table as read from the database.
   * 
   * @return The type
   */
  public String getType() {
    return _type;
  }

  /**
   * Sets the type of this table.
   * 
   * @param type
   *          The type
   */
  public void setType(String type) {
    _type = type;
  }

  /**
   * Returns the name of the table.
   * 
   * @return The name
   */
  public String getName() {
    return _name;
  }

  /**
   * Sets the name of the table.
   * 
   * @param name
   *          The name
   */
  public void setName(String name) {
    _name = name;
  }

  /**
   * Returns the description of the table.
   * 
   * @return The description
   */
  public String getDescription() {
    return _description;
  }

  /**
   * Sets the description of the table.
   * 
   * @param description
   *          The description
   */
  public void setDescription(String description) {
    _description = description;
  }

  /**
   * Returns the name of the primary key of the table.
   * 
   * @return The description
   */
  public String getPrimaryKey() {
    return _primaryKey;
  }

  /**
   * Sets the name of the primary key of the table.
   * 
   * @param primaryKey
   *          The primary key
   */
  public void setPrimaryKey(String primaryKey) {
    _primaryKey = primaryKey;
  }

  /**
   * Returns the number of columns in this table.
   * 
   * @return The number of columns
   */
  public int getColumnCount() {
    return _columns.size();
  }

  /**
   * Returns the column at the specified position.
   * 
   * @param idx
   *          The column index
   * @return The column at this position
   */
  public Column getColumn(int idx) {
    return (Column) _columns.get(idx);
  }

  /**
   * Returns the columns in this table.
   * 
   * @return The columns
   */
  public Column[] getColumns() {
    return (Column[]) _columns.toArray(new Column[_columns.size()]);
  }

  /**
   * Adds the given column.
   * 
   * @param column
   *          The column
   */
  public void addColumn(Column column) {
    if (column != null) {
      _columns.add(column);
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
      _columns.add(idx, column);
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
      if (previousColumn == null) {
        _columns.add(0, column);
      } else {
        _columns.add(_columns.indexOf(previousColumn), column);
      }
    }
  }

  /**
   * Adds the given columns.
   * 
   * @param columns
   *          The columns
   */
  public void addColumns(Collection columns) {
    for (Iterator it = columns.iterator(); it.hasNext();) {
      addColumn((Column) it.next());
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
      _columns.remove(column);
    }
  }

  /**
   * Removes the indicated column.
   * 
   * @param idx
   *          The index of the column to remove
   */
  public void removeColumn(int idx) {
    _columns.remove(idx);
  }

  /**
   * Returns the number of foreign keys.
   * 
   * @return The number of foreign keys
   */
  public int getForeignKeyCount() {
    return _foreignKeys.size();
  }

  /**
   * Returns the foreign key at the given position.
   * 
   * @param idx
   *          The foreign key index
   * @return The foreign key
   */
  public ForeignKey getForeignKey(int idx) {
    return (ForeignKey) _foreignKeys.get(idx);
  }

  /**
   * Returns the foreign keys of this table.
   * 
   * @return The foreign keys
   */
  public ForeignKey[] getForeignKeys() {
    return (ForeignKey[]) _foreignKeys.toArray(new ForeignKey[_foreignKeys.size()]);
  }

  /**
   * Adds the given foreign key.
   * 
   * @param foreignKey
   *          The foreign key
   */
  public void addForeignKey(ForeignKey foreignKey) {
    if (foreignKey != null) {
      _foreignKeys.add(foreignKey);
    }
  }

  /**
   * Adds the given foreign key at the specified position.
   * 
   * @param idx
   *          The index to add the foreign key at
   * @param foreignKey
   *          The foreign key
   */
  public void addForeignKey(int idx, ForeignKey foreignKey) {
    if (foreignKey != null) {
      _foreignKeys.add(idx, foreignKey);
    }
  }

  /**
   * Adds the given foreign keys.
   * 
   * @param foreignKeys
   *          The foreign keys
   */
  public void addForeignKeys(Collection foreignKeys) {
    for (Iterator it = foreignKeys.iterator(); it.hasNext();) {
      addForeignKey((ForeignKey) it.next());
    }
  }

  /**
   * Removes the given foreign key.
   * 
   * @param foreignKey
   *          The foreign key to remove
   */
  public void removeForeignKey(ForeignKey foreignKey) {
    if (foreignKey != null) {
      _foreignKeys.remove(foreignKey);
    }
  }

  /**
   * Removes the indicated foreign key.
   * 
   * @param idx
   *          The index of the foreign key to remove
   */
  public void removeForeignKey(int idx) {
    _foreignKeys.remove(idx);
  }

  /**
   * Returns the number of indices.
   * 
   * @return The number of indices
   */
  public int getIndexCount() {
    return _indices.size();
  }

  /**
   * Returns the index at the specified position.
   * 
   * @param idx
   *          The position
   * @return The index
   */
  public Index getIndex(int idx) {
    return (Index) _indices.get(idx);
  }

  /**
   * Adds the given index.
   * 
   * @param index
   *          The index
   */
  public void addIndex(Index index) {
    if (index != null) {
      _indices.add(index);
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
      _indices.add(idx, index);
    }
  }

  /**
   * Adds the given indices.
   * 
   * @param indices
   *          The indices
   */
  public void addIndices(Collection indices) {
    for (Iterator it = indices.iterator(); it.hasNext();) {
      addIndex((Index) it.next());
    }
  }

  /**
   * Returns the indices of this table.
   * 
   * @return The indices
   */
  public Index[] getIndices() {
    return (Index[]) _indices.toArray(new Index[_indices.size()]);
  }

  /**
   * Gets a list of non-unique indices on this table.
   * 
   * @return The unique indices
   */
  public Index[] getNonUniqueIndices() {
    Collection nonUniqueIndices = CollectionUtils.select(_indices, new Predicate() {
      public boolean evaluate(Object input) {
        return !((Index) input).isUnique();
      }
    });

    return (Index[]) nonUniqueIndices.toArray(new Index[nonUniqueIndices.size()]);
  }

  /**
   * Gets a list of unique indices on this table.
   * 
   * @return The unique indices
   */
  public Index[] getUniqueIndices() {
    Collection uniqueIndices = CollectionUtils.select(_indices, new Predicate() {
      public boolean evaluate(Object input) {
        return ((Index) input).isUnique();
      }
    });

    return (Index[]) uniqueIndices.toArray(new Index[uniqueIndices.size()]);
  }

  /**
   * Removes the given index.
   * 
   * @param index
   *          The index to remove
   */
  public void removeIndex(Index index) {
    if (index != null) {
      _indices.remove(index);
    }
  }

  /**
   * Removes the indicated index.
   * 
   * @param idx
   *          The position of the index to remove
   */
  public void removeIndex(int idx) {
    _indices.remove(idx);
  }

  /**
   * Returns the number of uniques.
   * 
   * @return The number of uniques
   */
  public int getUniqueCount() {
    return _uniques.size();
  }

  /**
   * Returns the unique at the specified position.
   * 
   * @param idx
   *          The position
   * @return The unique
   */
  public Unique getUnique(int idx) {
    return (Unique) _uniques.get(idx);
  }

  /**
   * Adds the given unique.
   * 
   * @param unique
   *          The unique
   */
  public void addUnique(Unique unique) {
    if (unique != null) {
      _uniques.add(unique);
    }
  }

  /**
   * Adds the given unique at the specified position.
   * 
   * @param idx
   *          The position to add the unique at
   * @param unique
   *          The unique
   */
  public void addUnique(int idx, Unique unique) {
    if (unique != null) {
      _uniques.add(idx, unique);
    }
  }

  /**
   * Adds the given uniques.
   * 
   * @param uniques
   *          The uniques
   */
  public void adduniques(Collection uniques) {
    for (Iterator it = uniques.iterator(); it.hasNext();) {
      addUnique((Unique) it.next());
    }
  }

  /**
   * Returns the uniques of this table.
   * 
   * @return The uniques
   */
  public Unique[] getuniques() {
    return (Unique[]) _uniques.toArray(new Unique[_uniques.size()]);
  }

  /**
   * Removes the given unique.
   * 
   * @param unique
   *          The unique to remove
   */
  public void removeUnique(Unique unique) {
    if (unique != null) {
      _indices.remove(unique);
    }
  }

  /**
   * Removes the indicated unique.
   * 
   * @param idx
   *          The position of the unique to remove
   */
  public void removeUnique(int idx) {
    _indices.remove(idx);
  }

  // Helper methods
  // -------------------------------------------------------------------------

  /**
   * Determines whether there is at least one primary key column on this table.
   * 
   * @return <code>true</code> if there are one or more primary key columns
   */
  public boolean hasPrimaryKey() {
    for (Iterator it = _columns.iterator(); it.hasNext();) {
      Column column = (Column) it.next();

      if (column.isPrimaryKey()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Finds the column with the specified name, using case insensitive matching. Note that this
   * method is not called getColumn(String) to avoid introspection problems.
   * 
   * @param name
   *          The name of the column
   * @return The column or <code>null</code> if there is no such column
   */
  public Column findColumn(String name) {
    return findColumn(name, false);
  }

  /**
   * Finds the column with the specified name, using case insensitive matching. Note that this
   * method is not called getColumn(String) to avoid introspection problems.
   * 
   * @param name
   *          The name of the column
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The column or <code>null</code> if there is no such column
   */
  public Column findColumn(String name, boolean caseSensitive) {
    for (Iterator it = _columns.iterator(); it.hasNext();) {
      Column column = (Column) it.next();

      if (caseSensitive) {
        if (column.getName().equals(name)) {
          return column;
        }
      } else {
        if (column.getName().equalsIgnoreCase(name)) {
          return column;
        }
      }
    }
    return null;
  }

  /**
   * Determines the index of the given column.
   * 
   * @param column
   *          The column
   * @return The index or <code>-1</code> if it is no column of this table
   */
  public int getColumnIndex(Column column) {
    int idx = 0;

    for (Iterator it = _columns.iterator(); it.hasNext(); idx++) {
      if (column == it.next()) {
        return idx;
      }
    }
    return -1;
  }

  /**
   * Finds the index with the specified name, using case insensitive matching. Note that this method
   * is not called getIndex to avoid introspection problems.
   * 
   * @param name
   *          The name of the index
   * @return The index or <code>null</code> if there is no such index
   */
  public Index findIndex(String name) {
    return findIndex(name, false);
  }

  /**
   * Finds the index with the specified name, using case insensitive matching. Note that this method
   * is not called getIndex to avoid introspection problems.
   * 
   * @param name
   *          The name of the index
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The index or <code>null</code> if there is no such index
   */
  public Index findIndex(String name, boolean caseSensitive) {
    for (int idx = 0; idx < getIndexCount(); idx++) {
      Index index = getIndex(idx);

      if (caseSensitive) {
        if (index.getName().equals(name)) {
          return index;
        }
      } else {
        if (index.getName().equalsIgnoreCase(name)) {
          return index;
        }
      }
    }
    return null;
  }

  /**
   * Finds the foreign key in this table that is equal to the supplied foreign key.
   * 
   * @param key
   *          The foreign key to search for
   * @return The found foreign key
   */
  public ForeignKey findForeignKey(ForeignKey key) {
    for (int idx = 0; idx < getForeignKeyCount(); idx++) {
      ForeignKey fk = getForeignKey(idx);

      if (fk.equals(key)) {
        return fk;
      }
    }
    return null;
  }

  /**
   * Finds the foreign key in this table that is equal to the supplied foreign key.
   * 
   * @param key
   *          The foreign key to search for
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The found foreign key
   */
  public ForeignKey findForeignKey(ForeignKey key, boolean caseSensitive) {
    for (int idx = 0; idx < getForeignKeyCount(); idx++) {
      ForeignKey fk = getForeignKey(idx);

      if ((caseSensitive && fk.equals(key)) || (!caseSensitive && fk.equalsIgnoreCase(key))) {
        return fk;
      }
    }
    return null;
  }

  /**
   * Returns the foreign key referencing this table if it exists.
   * 
   * @return The self-referencing foreign key if any
   */
  public ForeignKey getSelfReferencingForeignKey() {
    for (int idx = 0; idx < getForeignKeyCount(); idx++) {
      ForeignKey fk = getForeignKey(idx);

      if (this.equals(fk.getForeignTable())) {
        return fk;
      }
    }
    return null;
  }

  /**
   * Returns the primary key columns of this table.
   * 
   * @return The primary key columns
   */
  public Column[] getPrimaryKeyColumns() {
    Collection pkColumns = CollectionUtils.select(_columns, new Predicate() {
      public boolean evaluate(Object input) {
        return ((Column) input).isPrimaryKey();
      }
    });

    return (Column[]) pkColumns.toArray(new Column[pkColumns.size()]);
  }

  /**
   * Returns the auto increment columns in this table. If no incrementcolumns are found, it will
   * return an empty array.
   * 
   * @return The columns
   */
  public Column[] getAutoIncrementColumns() {
    Collection autoIncrColumns = CollectionUtils.select(_columns, new Predicate() {
      public boolean evaluate(Object input) {
        return ((Column) input).isAutoIncrement();
      }
    });

    return (Column[]) autoIncrColumns.toArray(new Column[autoIncrColumns.size()]);
  }

  /**
   * Sorts the foreign keys alphabetically.
   * 
   * @param caseSensitive
   *          Whether case matters
   */
  public void sortForeignKeys(final boolean caseSensitive) {
    if (!_foreignKeys.isEmpty()) {
      final Collator collator = Collator.getInstance();

      Collections.sort(_foreignKeys, new Comparator() {
        public int compare(Object obj1, Object obj2) {
          String fk1Name = ((ForeignKey) obj1).getName();
          String fk2Name = ((ForeignKey) obj2).getName();

          if (!caseSensitive) {
            fk1Name = (fk1Name != null ? fk1Name.toLowerCase() : null);
            fk2Name = (fk2Name != null ? fk2Name.toLowerCase() : null);
          }
          return collator.compare(fk1Name, fk2Name);
        }
      });
    }
  }

  public void sortChecks(final boolean caseSensitive) {

    if (!_checks.isEmpty()) {
      final Collator collator = Collator.getInstance();

      Collections.sort(_checks, new Comparator() {
        public int compare(Object obj1, Object obj2) {
          String ch1Name = ((Check) obj1).getName();
          String ch2Name = ((Check) obj2).getName();

          if (!caseSensitive) {
            ch1Name = (ch1Name != null ? ch1Name.toLowerCase() : null);
            ch2Name = (ch2Name != null ? ch2Name.toLowerCase() : null);
          }
          return collator.compare(ch1Name, ch2Name);
        }
      });
    }
  }

  /**
   * Returns the number of checks.
   * 
   * @return The number of checks
   */
  public int getCheckCount() {
    return _checks.size();
  }

  /**
   * Returns the check at the given position.
   * 
   * @param idx
   *          The check index
   * @return The check
   */
  public Check getCheck(int idx) {
    return (Check) _checks.get(idx);
  }

  /**
   * Returns the checks of this table.
   * 
   * @return The checks
   */
  public Check[] getChecks() {
    return (Check[]) _checks.toArray(new Check[_checks.size()]);
  }

  /**
   * Adds the given check.
   * 
   * @param check
   *          The check
   */
  public void addCheck(Check check) {
    if (check != null) {
      _checks.add(check);
    }
  }

  /**
   * Adds the given check at the specified position.
   * 
   * @param idx
   *          The index to add the check at
   * @param check
   *          The check
   */
  public void addCheck(int idx, Check check) {
    if (check != null) {
      _checks.add(idx, check);
    }
  }

  /**
   * Adds the given checks.
   * 
   * @param checks
   *          The checks
   */
  public void addChecks(Collection checks) {
    for (Iterator it = checks.iterator(); it.hasNext();) {
      addCheck((Check) it.next());
    }
  }

  /**
   * Removes the given check.
   * 
   * @param check
   *          The check to remove
   */
  public void removeCheck(Check check) {
    if (check != null) {
      _checks.remove(check);
    }
  }

  /**
   * Removes the indicated check.
   * 
   * @param idx
   *          The index of the check to remove
   */
  public void removeCheck(int idx) {
    _checks.remove(idx);
  }

  /**
   * Finds the check with the specified name, using case insensitive matching.
   * 
   * @param name
   *          The name of the index
   * @return The check or <code>null</code> if there is no such check
   */
  public Check findCheck(String name) {
    return findCheck(name, false);
  }

  /**
   * Finds the check with the specified name, using case insensitive matching.
   * 
   * @param name
   *          The name of the check
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The check or <code>null</code> if there is no such check
   */
  public Check findCheck(String name, boolean caseSensitive) {
    for (int idx = 0; idx < getCheckCount(); idx++) {
      Check check = getCheck(idx);

      if (caseSensitive) {
        if (check.getName().equals(name)) {
          return check;
        }
      } else {
        if (check.getName().equalsIgnoreCase(name)) {
          return check;
        }
      }
    }
    return null;
  }

  /**
   * Finds the unique with the specified name, using case insensitive matching. Note that this
   * method is not called getUnique to avoid introspection problems.
   * 
   * @param name
   *          The name of the unique
   * @return The unique or <code>null</code> if there is no such unique
   */
  public Unique findUnique(String name) {
    return findUnique(name, false);
  }

  /**
   * Finds the unique with the specified name, using case insensitive matching. Note that this
   * method is not called getUnique to avoid introspection problems.
   * 
   * @param name
   *          The name of the unique
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The unique or <code>null</code> if there is no such unique
   */
  public Unique findUnique(String name, boolean caseSensitive) {
    for (int idx = 0; idx < getUniqueCount(); idx++) {
      Unique unique = getUnique(idx);

      if (caseSensitive) {
        if (unique.getName().equals(name)) {
          return unique;
        }
      } else {
        if (unique.getName().equalsIgnoreCase(name)) {
          return unique;
        }
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public Object clone() throws CloneNotSupportedException {
    Table result = (Table) super.clone();

    result._catalog = _catalog;
    result._schema = _schema;
    result._name = _name;
    result._primaryKey = _primaryKey;
    result._type = _type;
    result._columns = new ArrayList();
    for (int i = 0; i < _columns.size(); i++)
      result._columns.add(((Column) _columns.get(i)).clone());
    result._foreignKeys = new ArrayList();
    for (int i = 0; i < _foreignKeys.size(); i++)
      result._foreignKeys.add(((ForeignKey) _foreignKeys.get(i)).clone());
    result._indices = new ArrayList();
    for (int i = 0; i < _indices.size(); i++)
      result._indices.add(((Index) _indices.get(i)).clone());
    result._uniques = new ArrayList();
    for (int i = 0; i < _uniques.size(); i++)
      result._uniques.add(((Unique) _uniques.get(i)).clone());
    result._checks = new ArrayList();
    for (int i = 0; i < _checks.size(); i++)
      result._checks.add(((Check) _checks.get(i)).clone());

    return result;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object obj) {
    if (obj instanceof Table) {
      Table other = (Table) obj;

      // Note that this compares case sensitive
      // TODO: For now we ignore catalog and schema (type should be
      // irrelevant anyways)
      return new EqualsBuilder().append(_name, other._name).append(_primaryKey, other._primaryKey)
          .append(_name, other._name).append(_columns, other._columns).append(
              new HashSet(_foreignKeys), new HashSet(other._foreignKeys)).append(
              new HashSet(_indices), new HashSet(other._indices)).append(new HashSet(_uniques),
              new HashSet(other._uniques)).append(new HashSet(_checks), new HashSet(other._checks))
          .isEquals();
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  public int hashCode() {
    // TODO: For now we ignore catalog and schema (type should be irrelevant
    // anyways)
    return new HashCodeBuilder(17, 37).append(_name).append(_primaryKey).append(_columns).append(
        new HashSet(_foreignKeys)).append(new HashSet(_indices)).append(new HashSet(_uniques))
        .append(new HashSet(_checks)).toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  public String toString() {
    StringBuffer result = new StringBuffer();

    result.append("Table [name=");
    result.append(getName());
    result.append("; ");
    result.append(getColumnCount());
    result.append(" columns]");

    return result.toString();
  }

  /**
   * Returns a verbose string representation of this table.
   * 
   * @return The string representation
   */
  public String toVerboseString() {
    StringBuffer result = new StringBuffer();

    result.append("Table [name=");
    result.append(getName());
    result.append("; catalog=");
    result.append(getCatalog());
    result.append("; schema=");
    result.append(getCatalog());
    result.append("; type=");
    result.append(getType());
    result.append("] columns:");
    for (int idx = 0; idx < getColumnCount(); idx++) {
      result.append(" ");
      result.append(getColumn(idx).toVerboseString());
    }
    result.append("; indices:");
    for (int idx = 0; idx < getIndexCount(); idx++) {
      result.append(" ");
      result.append(getIndex(idx).toVerboseString());
    }
    result.append("; uniques:");
    for (int idx = 0; idx < getUniqueCount(); idx++) {
      result.append(" ");
      result.append(getUnique(idx).toVerboseString());
    }
    result.append("; foreign keys:");
    for (int idx = 0; idx < getForeignKeyCount(); idx++) {
      result.append(" ");
      result.append(getForeignKey(idx).toVerboseString());
    }
    result.append("; checks:");
    for (int idx = 0; idx < getCheckCount(); idx++) {
      result.append(" ");
      result.append(getCheck(idx).toVerboseString());
    }

    return result.toString();
  }

  public void mergeWith(Table table) {
    try {
      for (int i = 0; i < table._columns.size(); i++) {
        this._columns.add(((Column) table._columns.get(i)).clone());
      }
      for (int i = 0; i < table._foreignKeys.size(); i++) {
        this._foreignKeys.add(((ForeignKey) table._foreignKeys.get(i)).clone());
      }
      for (int i = 0; i < table._indices.size(); i++) {
        if (findIndex(((Index) table._indices.get(i)).getName()) != null)
          removeIndex(findIndex(((Index) table._indices.get(i)).getName()));
        this._indices.add(((Index) table._indices.get(i)).clone());
      }
      for (int i = 0; i < table._uniques.size(); i++) {
        if (findUnique(((Unique) table._uniques.get(i)).getName()) != null)
          removeUnique(findUnique(((Unique) table._uniques.get(i)).getName()));
        this._uniques.add(((Unique) table._uniques.get(i)).clone());
      }
      for (int i = 0; i < table._checks.size(); i++) {
        if (findCheck(((Check) table._checks.get(i)).getName()) != null)
          removeCheck(findCheck(((Check) table._checks.get(i)).getName()));
        this._checks.add(((Check) table._checks.get(i)).clone());
      }
    } catch (CloneNotSupportedException e) {
      // won't happen
    }
  }

  public void applyNamingConventionFilter(ExcludeFilter filter) {
    if (!filter.compliesWithNamingRuleObject(_name)) {

      // The table doesn't comply with the naming rule, and therefore
      // doesn't belong to the module.
      // Its objects will only belong to this module if they have the
      // external prefix, and the prefix of the module.
      for (int i = 0; i < _columns.size(); i++) {
        if (!filter.compliesWithExternalNamingRule(((Column) _columns.get(i)).getName(), _name)) {
          _columns.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _foreignKeys.size(); i++) {
        if (!filter.compliesWithExternalNamingRule(((ForeignKey) _foreignKeys.get(i)).getName(),
            _name)) {
          _foreignKeys.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _indices.size(); i++) {
        if (!filter.compliesWithExternalNamingRule(((Index) _indices.get(i)).getName(), _name)) {
          _indices.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _uniques.size(); i++) {
        if (!filter.compliesWithExternalNamingRule(((Unique) _uniques.get(i)).getName(), _name)) {
          _uniques.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _checks.size(); i++) {
        if (!filter.compliesWithExternalNamingRule(((Check) _checks.get(i)).getName(), _name)) {
          _checks.remove(i);
          i--;
        }
      }
    } else {
      // As the table name complies with naming conventions, and it's
      // marked as isindevelopment,
      // all its objects should be preserved except the ones that start
      // with the "external" prefix
      for (int i = 0; i < _columns.size(); i++) {
        if (filter.compliesWithExternalPrefix(((Column) _columns.get(i)).getName(), _name)) {
          _columns.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _foreignKeys.size(); i++) {
        if (filter.compliesWithExternalPrefix(((ForeignKey) _foreignKeys.get(i)).getName(), _name)) {
          _foreignKeys.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _indices.size(); i++) {
        if (filter.compliesWithExternalPrefix(((Index) _indices.get(i)).getName(), _name)) {
          _indices.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _uniques.size(); i++) {
        if (filter.compliesWithExternalPrefix(((Unique) _uniques.get(i)).getName(), _name)) {
          _uniques.remove(i);
          i--;
        }
      }
      for (int i = 0; i < _checks.size(); i++) {
        if (filter.compliesWithExternalPrefix(((Check) _checks.get(i)).getName(), _name)) {
          _checks.remove(i);
          i--;
        }
      }
    }
  }

  public boolean isEmpty() {
    return _columns.size() == 0 && _foreignKeys.size() == 0 && _indices.size() == 0
        && _uniques.size() == 0 && _checks.size() == 0;
  }

}
