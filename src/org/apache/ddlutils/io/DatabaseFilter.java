/*
 * DatabaseFilter.java
 *
 * Created on 19 de septiembre de 2007, 10:03
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.apache.ddlutils.io;

/**
 * 
 * Interface that especifies the filter of tables from database
 * 
 * 
 * @author adrian
 */
public interface DatabaseFilter {
    /** WHERE clause that obtains all the data from table */
    public final static String FILTER_ALLDATA = "";
    /** WHERE clause that obtains no data from table */
    public final static String FILTER_NODATA = null;

    /**
     * Returns the WHERE clause that filters the table
     * 
     * @param tablename
     *            The table
     * @return A String with the WHERE clause
     */
    public String getTableFilter(String tablename);

    /**
     * Return all the tables specified in the filter
     * 
     * @return An array with the names of the tables
     */
    public String[] getTableNames();
}
