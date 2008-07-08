package org.apache.ddlutils.platform.oracle;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.ddlutils.DatabaseOperationException;

import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.platform.PlatformImplBase;
import org.apache.ddlutils.util.ExtTypes;

/**
 * The platform for Oracle 8.
 * 
 * TODO: We might support the {@link org.apache.ddlutils.Platform#createDatabase(String, String, String, String, Map)}
 *       functionality via "CREATE SCHEMA"/"CREATE USER" or "CREATE TABLESPACE" ?
 *
 * @version $Revision: 231306 $
 */
public class Oracle8Platform extends PlatformImplBase
{
    /** Database name of this platform. */
    public static final String DATABASENAME              = "Oracle";
    /** The standard Oracle jdbc driver. */
    public static final String JDBC_DRIVER               = "oracle.jdbc.driver.OracleDriver";
    /** The old Oracle jdbc driver. */
    public static final String JDBC_DRIVER_OLD           = "oracle.jdbc.dnlddriver.OracleDriver";
    /** The thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_THIN     = "oracle:thin";
    /** The thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_OCI8     = "oracle:oci8";
    /** The old thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_THIN_OLD = "oracle:dnldthin";

    /**
     * Creates a new platform instance.
     */
    public Oracle8Platform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(30);
        info.setIdentityStatusReadingSupported(false);

        // Note that the back-mappings are partially done by the model reader, not the driver
        info.addNativeTypeMapping(Types.CHAR,          "CHAR");
        info.addNativeTypeMapping(ExtTypes.NCHAR,         "NCHAR");
        info.addNativeTypeMapping(Types.ARRAY,         "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.BIGINT,        "NUMBER(38)");
        info.addNativeTypeMapping(Types.BINARY,        "RAW",              Types.VARBINARY);
        info.addNativeTypeMapping(Types.BIT,           "NUMBER(1)");
        info.addNativeTypeMapping(Types.DATE,          "DATE",             Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.DECIMAL,       "NUMBER");
        info.addNativeTypeMapping(Types.DISTINCT,      "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.DOUBLE,        "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT,         "FLOAT",            Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT,   "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARCHAR,   "CLOB",             Types.CLOB);
        info.addNativeTypeMapping(Types.NULL,          "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.NUMERIC,       "NUMBER",           Types.DECIMAL);
        info.addNativeTypeMapping(Types.OTHER,         "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.REF,           "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.SMALLINT,      "NUMBER(5)");
        info.addNativeTypeMapping(Types.STRUCT,        "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.TIME,          "DATE",             Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIMESTAMP,     "DATE");
        info.addNativeTypeMapping(Types.TINYINT,       "NUMBER(3)");
        info.addNativeTypeMapping(Types.VARBINARY,     "RAW");
        info.addNativeTypeMapping(Types.VARCHAR,       "VARCHAR2");
        info.addNativeTypeMapping(ExtTypes.NVARCHAR,      "NVARCHAR2");

        info.addNativeTypeMapping("BOOLEAN",  "NUMBER(1)", "BIT");
        info.addNativeTypeMapping("DATALINK", "BLOB",      "BLOB");

        info.setDefaultSize(Types.CHAR,       254);
        info.setDefaultSize(Types.VARCHAR,    254);
        info.setDefaultSize(Types.BINARY,     254);
        info.setDefaultSize(Types.VARBINARY,  254);

        setSqlBuilder(new Oracle8Builder(this));
        setModelReader(new Oracle8ModelReader(this));
        setModelLoader(new OracleModelLoader());
    }


    /**
     * {@inheritDoc}
     */
    public String getCreateTablesSqlScript(Database model, boolean dropTablesFirst, boolean continueOnError)
    {
        return "SET DEFINE OFF\n/\n"+getCreateTablesSql(model, dropTablesFirst, continueOnError);
    }
    
    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }
    
    /**
     * {@inheritDoc}
     */
    public void disableAllFK(Connection connection, Database model,boolean continueOnError) throws DatabaseOperationException {
    	
        String current = null;
    	try {
    		connection.prepareCall("PURGE RECYCLEBIN").execute();
            current = "SELECT 'ALTER TABLE'|| ' ' || TABLE_NAME || ' ' || 'DISABLE CONSTRAINT' || ' ' || CONSTRAINT_NAME  SQL_STR FROM USER_CONSTRAINTS WHERE  CONSTRAINT_TYPE='R' ";
            PreparedStatement pstmt  = connection.prepareStatement(current);
            ResultSet rs = pstmt.executeQuery();    	       
            while (rs.next ()) {
                current = rs.getString("SQL_STR");
                PreparedStatement pstmtd=connection.prepareStatement(current);
                pstmtd.executeUpdate();
                pstmtd.close();
            }
            rs.close();
            pstmt.close();    	       
        } catch (SQLException e) {
            System.out.println("SQL command failed with " + e.getMessage());
            System.out.println(current);
            throw new DatabaseOperationException("Error while disabling foreign key ", e);
        }               
    }
    
    /**
     * {@inheritDoc}
     */
    public void enableAllFK(Connection connection, Database model,boolean continueOnError) throws DatabaseOperationException {
    	
        String current = null;
    	try {
            current = "SELECT 'ALTER TABLE'|| ' ' || TABLE_NAME || ' ' || 'ENABLE CONSTRAINT' || ' ' || CONSTRAINT_NAME  SQL_STR FROM USER_CONSTRAINTS WHERE  CONSTRAINT_TYPE='R' ";
            PreparedStatement pstmt  = connection.prepareStatement(current);
            ResultSet rs = pstmt.executeQuery();    	       
            while (rs.next ()) {
                current = rs.getString("SQL_STR");
                PreparedStatement pstmtd=connection.prepareStatement(current);
                pstmtd.executeUpdate();
                pstmtd.close();
            }
            rs.close();
            pstmt.close();    	       
        } catch (SQLException e) {
            System.out.println("SQL command failed with " + e.getMessage());
            System.out.println(current);
            throw new DatabaseOperationException("Error while enabling foreign key ", e);
        }        
    }
    
    /**
     * {@inheritDoc}
     */
    public void disableAllTriggers(Connection connection, Database model, boolean continueOnError) throws DatabaseOperationException {
      
        String current = null;
    	try {
            current = "SELECT 'ALTER TRIGGER'|| ' ' || TRIGGER_NAME || ' ' || 'DISABLE' SQL_STR FROM USER_TRIGGERS";
            PreparedStatement pstmt = connection.prepareStatement(current);
            ResultSet rs = pstmt.executeQuery();    	       
            while (rs.next ()) {
                current = rs.getString("SQL_STR");
                PreparedStatement pstmtd=connection.prepareStatement(current);
                pstmtd.executeUpdate();
                pstmtd.close();
            }
            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            System.out.println("SQL command failed with " + e.getMessage());
            System.out.println(current);
            throw new DatabaseOperationException("Error while disabling triggers ", e);
        }         	    
    }
    
    /**
     * {@inheritDoc}
     */
    public void enableAllTriggers(Connection connection, Database model,boolean continueOnError) throws DatabaseOperationException {    	
        
        String current = null;
    	try {
            current = "SELECT 'ALTER TRIGGER'|| ' ' || TRIGGER_NAME || ' ' || 'ENABLE' SQL_STR FROM USER_TRIGGERS";
            PreparedStatement pstmt = connection.prepareStatement(current);
            ResultSet rs = pstmt.executeQuery();    	       
            while (rs.next ()) {
                current = rs.getString("SQL_STR");
                PreparedStatement pstmtd=connection.prepareStatement(current);
                pstmtd.executeUpdate();
                pstmtd.close();
            }
            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            System.out.println("SQL command failed with " + e.getMessage());
            System.out.println(current);
            throw new DatabaseOperationException("Error while enabling triggers ", e);
        }      
    }
    
//    /**
//     * {@inheritDoc}
//     */    
//    public void deleteAllDataFromTables(Connection connection, Database model, boolean continueOnError) throws DatabaseOperationException {
//        
//    	try {         
//            PreparedStatement pstmt = connection.prepareStatement(" SELECT 'TRUNCATE TABLE '|| TABLE_NAME || ' REUSE STORAGE'   SQL_STR  FROM USER_TABLES");
//            ResultSet rs = pstmt.executeQuery();    	       
//            while (rs.next ()) {
//                // System.out.println (rs.getString("SQL_STR"));
//                PreparedStatement pstmtd = connection.prepareStatement(rs.getString("SQL_STR"));
//                pstmtd.executeQuery();
//                pstmtd.close();
//            }
//            rs.close();
//            pstmt.close();    	       
//        } catch (SQLException e) {
//            e.printStackTrace();
//            throw new DatabaseOperationException("Error while truncating tables ", e);
//        }       
//    }    
}
