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
 * @author adrian
 */
public interface DatabaseFilter {
    
    public final static String FILTER_ALLDATA = "";
    public final static String FILTER_NODATA = null;
    
    public String getTableFilter(String tablename);    
    public String[] getTableNames();
}
