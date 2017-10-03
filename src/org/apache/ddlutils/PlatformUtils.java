package org.apache.ddlutils;

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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.ddlutils.platform.oracle.OraclePlatform;
import org.apache.ddlutils.platform.postgresql.PostgreSqlPlatform;

/**
 * Utility functions for dealing with database platforms.
 */
public class PlatformUtils {

  /** Maps the sub-protocol part of a jdbc connection url to a OJB platform name. */
  private Map<String, String> jdbcSubProtocolToPlatform = new HashMap<>();
  /** Maps the jdbc driver name to a OJB platform name. */
  private Map<String, String> jdbcDriverToPlatform = new HashMap<>();

  public PlatformUtils() {
    jdbcSubProtocolToPlatform
        .put(OraclePlatform.JDBC_SUBPROTOCOL_THIN, OraclePlatform.DATABASENAME);
    jdbcSubProtocolToPlatform
        .put(OraclePlatform.JDBC_SUBPROTOCOL_OCI8, OraclePlatform.DATABASENAME);
    jdbcSubProtocolToPlatform.put(OraclePlatform.JDBC_SUBPROTOCOL_THIN_OLD,
        OraclePlatform.DATABASENAME);
    jdbcSubProtocolToPlatform.put(OraclePlatform.JDBC_SUBPROTOCOL_DATADIRECT_ORACLE,
        OraclePlatform.DATABASENAME);
    jdbcSubProtocolToPlatform.put(OraclePlatform.JDBC_SUBPROTOCOL_INET_ORACLE,
        OraclePlatform.DATABASENAME);
    jdbcSubProtocolToPlatform.put(PostgreSqlPlatform.JDBC_SUBPROTOCOL,
        PostgreSqlPlatform.DATABASENAME);

    jdbcDriverToPlatform.put(OraclePlatform.JDBC_DRIVER, OraclePlatform.DATABASENAME);
    jdbcDriverToPlatform.put(OraclePlatform.JDBC_DRIVER_OLD, OraclePlatform.DATABASENAME);
    jdbcDriverToPlatform.put(OraclePlatform.JDBC_DRIVER_DATADIRECT_ORACLE,
        OraclePlatform.DATABASENAME);
    jdbcDriverToPlatform.put(OraclePlatform.JDBC_DRIVER_INET_ORACLE, OraclePlatform.DATABASENAME);
    jdbcDriverToPlatform.put(PostgreSqlPlatform.JDBC_DRIVER, PostgreSqlPlatform.DATABASENAME);
  }

  /**
   * Tries to determine the database type for the given data source. Note that this will establish a
   * connection to the database.
   * 
   * @param dataSource
   *          The data source
   * @return The database type or <code>null</code> if the database type couldn't be determined
   */
  public String determineDatabaseType(DataSource dataSource) throws DatabaseOperationException {
    return determineDatabaseType(dataSource, null, null);
  }

  /**
   * Tries to determine the database type for the given data source. Note that this will establish a
   * connection to the database.
   * 
   * @param dataSource
   *          The data source
   * @param username
   *          The user name to use for connecting to the database
   * @param password
   *          The password to use for connecting to the database
   * @return The database type or <code>null</code> if the database type couldn't be determined
   */
  public String determineDatabaseType(DataSource dataSource, String username, String password)
      throws DatabaseOperationException {
    Connection connection = null;

    try {
      if (username != null) {
        connection = dataSource.getConnection(username, password);
      } else {
        connection = dataSource.getConnection();
      }

      DatabaseMetaData metaData = connection.getMetaData();

      return determineDatabaseType(metaData.getDriverName(), metaData.getURL());
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while reading the database metadata: "
          + ex.getMessage(), ex);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException ex) {
          // we ignore this one
        }
      }
    }
  }

  /**
   * Tries to determine the database type for the given jdbc driver and connection url.
   * 
   * @param driverName
   *          The fully qualified name of the JDBC driver
   * @param jdbcConnectionUrl
   *          The connection url
   * @return The database type or <code>null</code> if the database type couldn't be determined
   */
  public String determineDatabaseType(String driverName, String jdbcConnectionUrl) {
    if (jdbcDriverToPlatform.containsKey(driverName)) {
      return (String) jdbcDriverToPlatform.get(driverName);
    }
    if (jdbcConnectionUrl == null) {
      return null;
    }

    for (Entry<String, String> entry : jdbcSubProtocolToPlatform.entrySet()) {
      String curSubProtocol = "jdbc:" + entry.getKey() + ":";

      if (jdbcConnectionUrl.startsWith(curSubProtocol)) {
        return entry.getValue();
      }
    }
    return null;
  }
}
