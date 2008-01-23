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
 * @author adrian
 */
public final class NoneDatabaseFilter implements DynamicDatabaseFilter {
    
    /** Creates a new instance of NoneDatabaseFilter */
    public NoneDatabaseFilter() {
    }
    
    public void init(Database database) {
    }
    
    public String[] getTableNames() {
        return new String[0];
    }    
    
    public String getTableFilter(String tablename) {
        return FILTER_NODATA;
    }    
}
