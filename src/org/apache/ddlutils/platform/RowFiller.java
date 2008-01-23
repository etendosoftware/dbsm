/*
 * RowPopulate.java
 *
 * Created on 13 de noviembre de 2007, 16:50
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
public interface RowFiller {
    
    public void fillRow(ResultSet r) throws SQLException;    
}
