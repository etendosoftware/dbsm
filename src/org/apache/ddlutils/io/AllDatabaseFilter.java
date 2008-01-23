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
 *
 * @author adrian
 */
public final class AllDatabaseFilter extends AbstractDatabaseFilter {
    
    /** Creates a new instance of AllDatabaseFilter */
    public AllDatabaseFilter() {
    }
    
    public void init(Database database) {
        addAllTables(database);
    }    
}
