/*
 * AbstractDatabaseFilter.java
 *
 * Created on 19 de septiembre de 2007, 10:12
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.apache.ddlutils.io;

import java.util.HashMap;
import java.util.Map;

import org.apache.ddlutils.model.Database;

/**
 * 
 * Abstract class that implements DynamicDatabaseFilter
 * 
 * Defines the methods for add and removes tables from the list of filters
 * 
 * @author adrian
 */
public abstract class AbstractDatabaseFilter implements DynamicDatabaseFilter {

    // private boolean includealltables = false;
    private Map<String, String> m_tablefilters = new HashMap<String, String>();

    /** Creates a new instance of AbstractDatabaseFilter */
    public AbstractDatabaseFilter() {
    }

    /**
     * 
     * Removes the table from the filter
     * 
     * @param table
     *            Table to be removed from the filter
     */
    protected void removeTable(String table) {
        m_tablefilters.remove(table);
    }

    /**
     * Adds a table to the filter
     * 
     * @param table
     *            The table to be added to the filter
     */
    protected void addTable(String table) {
        m_tablefilters.put(table, FILTER_ALLDATA);
    }

    /**
     * Add a table to the filter For filter records especified a where clause in
     * param filter
     * 
     * @param table
     *            The name of the table
     * @param filter
     *            The WHERE clause of SQL statement that filter the table
     */
    protected void addTable(String table, String filter) {
        if (filter == FILTER_NODATA) {
            m_tablefilters.remove(table);
        } else {
            m_tablefilters.put(table, filter);
        }
    }

    /**
     * Adds all the tables from database to the filter
     * 
     * @param database
     *            The name of database
     */
    protected void addAllTables(Database database) {
        for (int i = 0; i < database.getTableCount(); i++) {
            addTable(database.getTable(i).getName());
        }
    }

    /**
     * Returns the names of all tables of the filter
     * 
     * @return An array with a String for each table name
     */
    public final String[] getTableNames() {
        return m_tablefilters.keySet().toArray(new String[0]);
    }

    /**
     * Returns the SQL statements that filter the contain of a table in the
     * filter list
     * 
     * @param tablename
     *            The name of the table
     * @return An String with the SQL statement
     */
    public final String getTableFilter(String tablename) {
        return m_tablefilters.get(tablename);
    }
}
