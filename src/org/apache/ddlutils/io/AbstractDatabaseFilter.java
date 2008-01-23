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
 * @author adrian
 */
public abstract class AbstractDatabaseFilter implements DynamicDatabaseFilter {
    
    // private boolean includealltables = false;
    private Map<String, String> m_tablefilters = new HashMap<String, String>();
            
    /** Creates a new instance of AbstractDatabaseFilter */
    public AbstractDatabaseFilter() {
    }
    
    protected void removeTable(String table) {
        m_tablefilters.remove(table);
    }
    
    protected void addTable(String table) {
        m_tablefilters.put(table, FILTER_ALLDATA);
    }
    
    protected void addTable(String table, String filter) {
        if (filter == FILTER_NODATA) {
            m_tablefilters.remove(table);
        } else {
            m_tablefilters.put(table, filter);
        }
    }   
    
    protected void addAllTables(Database database) {
        for (int i = 0; i < database.getTableCount(); i++) {
            addTable(database.getTable(i).getName());
        }
    }
    
    public final String[] getTableNames() {
        return m_tablefilters.keySet().toArray(new String[0]);
    }
    
    public final String getTableFilter(String tablename) {
        return m_tablefilters.get(tablename);        
    }
}
