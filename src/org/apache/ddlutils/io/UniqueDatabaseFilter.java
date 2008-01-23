/*
 * UniqueDatabaseFilter.java
 *
 * Created on 17 de octubre de 2007, 18:21
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
public class UniqueDatabaseFilter implements DatabaseFilter {
    
    private DatabaseFilter _databasefilter;
    private String _tablename;
    
    /** Creates a new instance of UniqueDatabaseFilter */
    public UniqueDatabaseFilter(DatabaseFilter databasefilter, String tablename) {
        _databasefilter = databasefilter;
        _tablename = tablename;
    }
    
    public String[] getTableNames() {
        return new String[] {_tablename};
    }    
    
    public String getTableFilter(String tablename) {
        if (_tablename.equals(tablename)) {
            return _databasefilter.getTableFilter(_tablename);
        } else {
            return FILTER_NODATA;
        }
    }        
}
