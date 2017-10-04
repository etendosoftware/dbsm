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

import javax.sql.DataSource;

import org.apache.ddlutils.platform.oracle.OraclePlatform;
import org.apache.ddlutils.platform.postgresql.PostgreSql10Platform;
import org.apache.ddlutils.platform.postgresql.PostgreSqlPlatform;

/**
 * A factory of {@link org.apache.ddlutils.Platform} instances. Note that this is a convenience
 * class as the platforms can also simply be created via their constructors.
 * 
 * @version $Revision: 209952 $
 */
public class PlatformFactory {

  /**
   * Creates a new platform for the database whose information is retrieved using the data source
   * passed as parameter. Note that this method sets the data source at the returned platform
   * instance (method {@link Platform#setDataSource(DataSource)}).
   * 
   * @param dataSource
   *          The data source for the database
   * @return The platform or <code>null</code> if the database is not supported
   */
  public static synchronized Platform createNewPlatformInstance(DataSource dataSource)
      throws DdlUtilsException {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      DatabaseMetaData metaData = connection.getMetaData();
      Platform platform = selectPlatformInstance(metaData.getDatabaseProductName(),
          metaData.getDatabaseMajorVersion());
      if (platform != null) {
        platform.setDataSource(dataSource);
      }
      return platform;
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

  private static Platform selectPlatformInstance(String databaseProductName, int majorVersion) {
    try {
      Class<? extends Platform> selectedPlatform;
      switch (databaseProductName) {
      case "Oracle":
        selectedPlatform = OraclePlatform.class;
        break;
      case "PostgreSQL":
        if (majorVersion == 9) {
          selectedPlatform = PostgreSqlPlatform.class;
        } else {
          selectedPlatform = PostgreSql10Platform.class;
        }
        break;
      default:
        return null;
      }
      return selectedPlatform.newInstance();
    } catch (Exception ex) {
      throw new DdlUtilsException("Could not create platform for database " + databaseProductName
          + " version " + majorVersion, ex);
    }
  }

  /**
   * Determines whether the indicated platform is supported.
   * 
   * @param platformName
   *          The name of the platform
   * @return <code>true</code> if the platform is supported
   */
  public static boolean isPlatformSupported(String platformName) {
    return OraclePlatform.DATABASENAME.equalsIgnoreCase(platformName)
        || PostgreSqlPlatform.DATABASENAME.equalsIgnoreCase(platformName);
  }
}
