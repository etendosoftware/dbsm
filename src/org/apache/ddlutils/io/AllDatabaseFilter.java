/*
 * AllDatabaseFilter.java
 *
 * Created on 19 de septiembre de 2007, 10:44
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.apache.ddlutils.io;

import org.apache.ddlutils.model.Database;

/**
 * This class selects for the filter all the tables of database
 * 
 * @author adrian
 */
public final class AllDatabaseFilter extends AbstractDatabaseFilter {

    /** Creates a new instance of AllDatabaseFilter */
    public AllDatabaseFilter() {
    }

    /**
     * Add to the filter all of the tables from database
     * 
     * @param database
     *            The name of database
     */
    public void init(Database database) {
        addAllTables(database);
    }
}
