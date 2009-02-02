/*
 * UniqueDatabaseFilter.java
 *
 * Created on 17 de octubre de 2007, 18:21
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.apache.ddlutils.io;

/**
 * 
 * The class UniqueDatabaseFilter implements DatabaseFilter
 * 
 * Defines a filter for database with a unique table
 * 
 * @author adrian
 */
public class UniqueDatabaseFilter implements DatabaseFilter {

  private DatabaseFilter _databasefilter;
  private String _tablename;

  /**
   * 
   * Creates a new instance of UniqueDatabaseFilter
   * 
   * @param databasefilter
   * @param tablename
   */
  public UniqueDatabaseFilter(DatabaseFilter databasefilter, String tablename) {
    _databasefilter = databasefilter;
    _tablename = tablename;
  }

  /**
   * Returns a String array with only one table
   * 
   * @return A String array wiht only one String
   */
  public String[] getTableNames() {
    return new String[] { _tablename };
  }

  /**
   * Returns the WHERE clause that filters the table
   * 
   * @return The WHERE clause or null if has no filter the table
   */
  public String getTableFilter(String tablename) {
    if (_tablename.equals(tablename)) {
      return _databasefilter.getTableFilter(_tablename);
    } else {
      return FILTER_NODATA;
    }
  }
}
