/*
 * DynamicDatabaseFilter.java
 *
 * Created on 18 de octubre de 2007, 8:10
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
public interface DynamicDatabaseFilter extends DatabaseFilter {
    
    public void init(Database database);    
}
