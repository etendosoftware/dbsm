/*
************************************************************************************
* Copyright (C) 2001-2006 Openbravo S.L.
* Licensed under the Apache Software License version 2.0
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
* Unless required by applicable law or agreed to  in writing,  software  distributed
* under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
* CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
* specific language governing permissions and limitations under the License.
************************************************************************************
*/

package org.apache.ddlutils.platform.oracle;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ModelLoaderBase;
import org.apache.ddlutils.platform.RowFiller;
import org.apache.ddlutils.util.ExtTypes;

/**
 *
 * @author adrian
 */
public class OracleModelLoader extends ModelLoaderBase {

    protected PreparedStatement _stmt_comments_tables;
    /** Creates a new instance of BasicModelLoader */
    public OracleModelLoader() {
    }  
    
    protected String readName() {
        return "Oracle server";
    }
    
    protected void initMetadataSentences() throws SQLException {

        if (_filter.getExcludedTables().length == 0) {
            _stmt_listtables = _connection.prepareStatement("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME");
        } else {
            _stmt_listtables = _connection.prepareStatement("SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME NOT IN (" +  getListObjects(_filter.getExcludedTables()) + ") ORDER BY TABLE_NAME");
        }
        _stmt_pkname = _connection.prepareStatement("SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND TABLE_NAME = ?");
        _stmt_listcolumns = _connection.prepareStatement("SELECT COLUMN_NAME, DATA_TYPE, CHAR_COL_DECL_LENGTH, DATA_LENGTH ,DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? ORDER BY COLUMN_ID");
        _stmt_pkcolumns = _connection.prepareStatement("SELECT COLUMN_NAME FROM USER_CONS_COLUMNS WHERE CONSTRAINT_NAME = ? ORDER BY POSITION");
        _stmt_listchecks = _connection.prepareStatement("SELECT CONSTRAINT_NAME, SEARCH_CONDITION FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'C' AND GENERATED = 'USER NAME' AND TABLE_NAME = ? ORDER BY CONSTRAINT_NAME");
        _stmt_listfks = _connection.prepareStatement("SELECT C.CONSTRAINT_NAME, C2.TABLE_NAME, C.DELETE_RULE, 'NO ACTION' FROM USER_CONSTRAINTS C, USER_CONSTRAINTS C2 WHERE C.R_CONSTRAINT_NAME = C2.CONSTRAINT_NAME AND C.CONSTRAINT_TYPE = 'R' AND C.TABLE_NAME = ? ORDER BY C.CONSTRAINT_NAME");
        _stmt_fkcolumns = _connection.prepareStatement("SELECT C.COLUMN_NAME, C2.COLUMN_NAME FROM USER_CONS_COLUMNS C, USER_CONSTRAINTS K, USER_CONS_COLUMNS C2, USER_CONSTRAINTS K2 WHERE C.CONSTRAINT_NAME = K.CONSTRAINT_NAME AND C2.CONSTRAINT_NAME = K2.CONSTRAINT_NAME AND K.R_CONSTRAINT_NAME = K2.CONSTRAINT_NAME AND C.CONSTRAINT_NAME = ? ORDER BY C.POSITION");

        _stmt_listindexes = _connection.prepareStatement("SELECT INDEX_NAME, UNIQUENESS FROM USER_INDEXES U WHERE TABLE_NAME = ? AND INDEX_TYPE = 'NORMAL' AND NOT EXISTS (SELECT 1 FROM USER_CONSTRAINTS WHERE TABLE_NAME = U.TABLE_NAME AND INDEX_NAME = U.INDEX_NAME AND CONSTRAINT_TYPE IN ('U', 'P')) ORDER BY INDEX_NAME");        
        _stmt_indexcolumns = _connection.prepareStatement("SELECT COLUMN_NAME FROM USER_IND_COLUMNS WHERE INDEX_NAME = ? ORDER BY COLUMN_POSITION");

        _stmt_listuniques = _connection.prepareStatement("SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'U' AND TABLE_NAME = ? ORDER BY CONSTRAINT_NAME");
        _stmt_uniquecolumns = _connection.prepareStatement("SELECT COLUMN_NAME FROM USER_CONS_COLUMNS WHERE CONSTRAINT_NAME = ? ORDER BY POSITION");

        if (_filter.getExcludedViews().length == 0) {
            _stmt_listviews = _connection.prepareStatement("SELECT VIEW_NAME, TEXT FROM USER_VIEWS");
        } else {
             _stmt_listviews = _connection.prepareStatement("SELECT VIEW_NAME, TEXT FROM USER_VIEWS WHERE VIEW_NAME NOT IN (" + getListObjects(_filter.getExcludedViews()) + ")");
        }

        if (_filter.getExcludedSequences().length == 0) {
            _stmt_listsequences = _connection.prepareStatement("SELECT SEQUENCE_NAME, LAST_NUMBER, INCREMENT_BY FROM USER_SEQUENCES");
        } else {
           _stmt_listsequences = _connection.prepareStatement("SELECT SEQUENCE_NAME, LAST_NUMBER, INCREMENT_BY FROM USER_SEQUENCES WHERE SEQUENCE_NAME NOT IN (" +  getListObjects(_filter.getExcludedSequences()) + ")");
        }
        
        if (_filter.getExcludedTriggers().length == 0) {
            _stmt_listtriggers = _connection.prepareStatement("SELECT TRIGGER_NAME, TABLE_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TRIGGER_BODY FROM USER_TRIGGERS");
        } else {
            _stmt_listtriggers = _connection.prepareStatement("SELECT TRIGGER_NAME, TABLE_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TRIGGER_BODY FROM USER_TRIGGERS WHERE TRIGGER_NAME NOT IN (" + getListObjects(_filter.getExcludedTriggers()) + ")");
        }

        if (_filter.getExcludedFunctions().length == 0) {
            _stmt_listfunctions = _connection.prepareStatement("SELECT DISTINCT NAME FROM USER_SOURCE WHERE TYPE = 'PROCEDURE' OR TYPE = 'FUNCTION'");
        } else {
            _stmt_listfunctions = _connection.prepareStatement("SELECT DISTINCT NAME FROM USER_SOURCE WHERE (TYPE = 'PROCEDURE' OR TYPE = 'FUNCTION') AND NAME NOT IN (" + getListObjects(_filter.getExcludedFunctions()) + ")");
        }
        
        _stmt_functioncode = _connection.prepareStatement("SELECT TEXT FROM USER_SOURCE WHERE NAME = ? ORDER BY LINE");
        _stmt_comments_tables= _connection.prepareStatement("SELECT COMMENTS FROM USER_COL_COMMENTS WHERE TABLE_NAME= ? AND COLUMN_NAME= ?");
    }
    
    protected boolean translateRequired(String required) {
        return "N".equals(required);
    }
    
    protected String translateDefault(String value, int type) {
        
        switch (type) {
            case Types.CHAR:
            case Types.VARCHAR:
            case ExtTypes.NCHAR:
            case ExtTypes.NVARCHAR:
            case Types.LONGVARCHAR:
                if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
                    value =  value.substring(1, value.length() - 1);
                    int i = 0;
                    StringBuffer sunescaped = new StringBuffer();
                    while (i < value.length()) {
                        char c = value.charAt(i);
                        if (c == '\'') {
                            i++;
                            if (i < value.length()) {
                                sunescaped.append(c);
                                i++;                                    
                            }                                
                        } else {
                            sunescaped.append(c);
                            i++;
                        }
                    }
                    if(sunescaped.length() == 0) return null;
                    else return sunescaped.toString();
                } else {
                    return value;
                }
            default: return value;
        }
    }
    
    protected int translateColumnType(String nativeType) {
        
        if (nativeType == null) {
            return Types.NULL;
        } else if ("CHAR".equalsIgnoreCase(nativeType)) {
            return Types.CHAR;
        } else if ("VARCHAR2".equalsIgnoreCase(nativeType)) {
            return Types.VARCHAR;
        } else if ("NCHAR".equalsIgnoreCase(nativeType)) {
            return ExtTypes.NCHAR;
        } else if ("NVARCHAR2".equalsIgnoreCase(nativeType)) {
            return ExtTypes.NVARCHAR;
        } else if ("NUMBER".equalsIgnoreCase(nativeType)) {
            return Types.DECIMAL;
        } else if ("DATE".equalsIgnoreCase(nativeType)) {
            return Types.TIMESTAMP;
        } else if ("CLOB".equalsIgnoreCase(nativeType)) {
            return Types.CLOB;
        } else if ("BLOB".equalsIgnoreCase(nativeType)) {
            return Types.BLOB;
        } else {
            return Types.VARCHAR;
        }
    }
    
    protected int translateParamType(String nativeType) {
        
        if (nativeType == null) {
            return Types.NULL;
        } else if ("CHAR".equalsIgnoreCase(nativeType)) {
            return Types.CHAR;
        } else if ("VARCHAR2".equalsIgnoreCase(nativeType)) {
            return Types.VARCHAR;
        } else if ("NCHAR".equalsIgnoreCase(nativeType)) {
            return ExtTypes.NCHAR;
        } else if ("NVARCHAR2".equalsIgnoreCase(nativeType)) {
            return ExtTypes.NVARCHAR;
        } else if ("NUMBER".equalsIgnoreCase(nativeType)) {
            return Types.NUMERIC;
        } else if ("DATE".equalsIgnoreCase(nativeType)) {
            return Types.TIMESTAMP;
        } else if ("CLOB".equalsIgnoreCase(nativeType)) {
            return Types.CLOB;
        } else if ("BLOB".equalsIgnoreCase(nativeType)) {
            return Types.BLOB;
        } else {
            return Types.VARCHAR;
        }
    }    
    
    protected int translateFKEvent(String fkevent) {
        if ("CASCADE".equalsIgnoreCase(fkevent)) {
            return DatabaseMetaData.importedKeyCascade;
        } else if ("SET NULL".equalsIgnoreCase(fkevent)) {
            return DatabaseMetaData.importedKeySetNull;
        } else if ("RESTRICT".equalsIgnoreCase(fkevent)) {
            return DatabaseMetaData.importedKeyRestrict;
        } else {
            return DatabaseMetaData.importedKeyNoAction;
        }
    }      

    String commentCol;
    protected Table readTable(String tablename) throws SQLException
    {

    	Table t=super.readTable(tablename);

    	for(int i=0;i<t.getColumnCount();i++)
    	{
            _stmt_comments_tables.setString(1, tablename);
            _stmt_comments_tables.setString(2, t.getColumn(i).getName());
            fillList(_stmt_comments_tables,new RowFiller() { public void fillRow(ResultSet r) throws SQLException {
            		commentCol=r.getString(1);
            }});
            if(commentCol!=null && !commentCol.equals(""))
            {
            	Pattern pat3=Pattern.compile("--OBTG:ONCREATEDEFAULT:(.*?)--");
            	Matcher match3=pat3.matcher(commentCol);
            	if(match3.matches())
            	{
            		t.getColumn(i).setOnCreateDefault(match3.group(1));
            	}
            }
    	}
    	
    	return t;
    }
}
