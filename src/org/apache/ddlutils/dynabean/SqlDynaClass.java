package org.apache.ddlutils.dynabean;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BasicDynaClass;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.ddlutils.model.Table;

/**
 * SqlDynaClass is a DynaClass which is associated with a persistent Table in a Database.
 * 
 * @version $Revision: 463757 $
 */
public class SqlDynaClass extends BasicDynaClass {
  /** Unique ID for serializaion purposes. */
  private static final long serialVersionUID = -5768155698352911245L;

  /** The table for which this dyna class is defined. */
  private Table _table;
  /** The primary key dyna properties. */
  private SqlDynaProperty[] _primaryKeyProperties;
  /** The non-primary key dyna properties. */
  private SqlDynaProperty[] _nonPrimaryKeyProperties;

  /**
   * Factory method for creating and initializing a new dyna class instance for the given table.
   * 
   * @param table
   *          The table
   * @return The dyna class for the table
   */
  public static SqlDynaClass newInstance(Table table) {
    List properties = new ArrayList();

    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      properties.add(new SqlDynaProperty(table.getColumn(idx)));
    }

    SqlDynaProperty[] array = new SqlDynaProperty[properties.size()];

    properties.toArray(array);
    return new SqlDynaClass(table, array);
  }

  /**
   * Creates a new dyna class instance for the given table that has the given properties.
   * 
   * @param table
   *          The table
   * @param properties
   *          The dyna properties
   */
  public SqlDynaClass(Table table, SqlDynaProperty[] properties) {
    super(table.getName(), SqlDynaBean.class, properties);
    _table = table;
  }

  /**
   * Returns the table for which this dyna class is defined.
   * 
   * @return The table
   */
  public Table getTable() {
    return _table;
  }

  // Helper methods
  // -------------------------------------------------------------------------

  /**
   * Returns the table name for which this dyna class is defined.
   * 
   * @return The table name
   */
  public String getTableName() {
    return getTable().getName();
  }

  /**
   * Returns the properties of this dyna class.
   * 
   * @return The properties
   */
  public SqlDynaProperty[] getSqlDynaProperties() {
    return (SqlDynaProperty[]) getDynaProperties();
  }

  /**
   * Returns the properties for the primary keys of the corresponding table.
   * 
   * @return The properties
   */
  public SqlDynaProperty[] getPrimaryKeyProperties() {
    if (_primaryKeyProperties == null) {
      initPrimaryKeys();
    }
    return _primaryKeyProperties;
  }

  /**
   * Returns the properties for the non-primary keys of the corresponding table.
   * 
   * @return The properties
   */
  public SqlDynaProperty[] getNonPrimaryKeyProperties() {
    if (_nonPrimaryKeyProperties == null) {
      initPrimaryKeys();
    }
    return _nonPrimaryKeyProperties;
  }

  // Implementation methods
  // -------------------------------------------------------------------------

  /**
   * Initializes the primary key and non primary key property arrays.
   */
  protected void initPrimaryKeys() {
    List pkProps = new ArrayList();
    List nonPkProps = new ArrayList();
    DynaProperty[] properties = getDynaProperties();

    for (int idx = 0; idx < properties.length; idx++) {
      if (properties[idx] instanceof SqlDynaProperty) {
        SqlDynaProperty sqlProperty = (SqlDynaProperty) properties[idx];

        if (sqlProperty.isPrimaryKey()) {
          pkProps.add(sqlProperty);
        } else {
          nonPkProps.add(sqlProperty);
        }
      }
    }
    _primaryKeyProperties = (SqlDynaProperty[]) pkProps
        .toArray(new SqlDynaProperty[pkProps.size()]);
    _nonPrimaryKeyProperties = (SqlDynaProperty[]) nonPkProps
        .toArray(new SqlDynaProperty[nonPkProps.size()]);
  }

  /**
   * This method resets the properties of a dynaclass, adding all the properties of the provided
   * table
   * 
   * @param table
   *          The properties of this table will be added to the dynaclass (the old ones will be
   *          removed)
   */
  public void resetDynaClass(Table table) {
    _table = table;

    List properties = new ArrayList();

    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      properties.add(new SqlDynaProperty(table.getColumn(idx)));
    }

    SqlDynaProperty[] array = new SqlDynaProperty[properties.size()];

    properties.toArray(array);
    setProperties(array);
    initPrimaryKeys();
  }

  /**
   * This method resets the properties of a dynaclass, but adding only the properties of the new
   * table which were present in the old one, and ignoring the ones which do not exist in the old
   * table
   * 
   * @param table
   *          The properties of this table which also exist on the old table will be added to the
   *          dynaclass
   */
  public void resetDynaClassWithoutMissingProperties(Table table) {

    List properties = new ArrayList();

    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      if (_table.findColumn(table.getColumn(idx).getName()) != null
          || table.getColumn(idx).isRequired()) {
        properties.add(new SqlDynaProperty(table.getColumn(idx)));
      }
    }
    _table = table;

    SqlDynaProperty[] array = new SqlDynaProperty[properties.size()];

    properties.toArray(array);
    setProperties(array);
    initPrimaryKeys();
  }
}
