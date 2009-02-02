/*
 * NoneDatabaseFilter.java
 *
 * Created on 2 de octubre de 2007, 9:57
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.apache.ddlutils.io;

import org.apache.ddlutils.model.Database;

/**
 * 
 * The class NoneDatabaseFilter implements a DynamicDatabaseFilter
 * 
 * This class defines a empty filter for select no table from database
 * 
 * @author adrian
 */
public final class NoneDatabaseFilter implements DynamicDatabaseFilter {

  /** Creates a new instance of NoneDatabaseFilter */
  public NoneDatabaseFilter() {
  }

  /**
   * The init method
   * 
   * @param database
   *          The database
   */
  public void init(Database database) {
  }

  /**
   * Return an empty String array
   * 
   * @return Empty String array
   */
  public String[] getTableNames() {
    return new String[0];
  }

  /**
   * Return the constant FILTER_NODATA
   * 
   * @param tablename
   *          The table
   * @return The constant FILTER_NODATA
   */
  public String getTableFilter(String tablename) {
    return FILTER_NODATA;
  }
}
