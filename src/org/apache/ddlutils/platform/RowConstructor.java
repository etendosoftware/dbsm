/*
 * RowConstructor.java
 *
 * Created on 13 de noviembre de 2007, 16:30
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.apache.ddlutils.platform;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author adrian
 */
public interface RowConstructor {
    
    public Object getRow(ResultSet r) throws SQLException;
}
