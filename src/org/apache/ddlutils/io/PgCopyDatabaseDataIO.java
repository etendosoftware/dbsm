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

package org.apache.ddlutils.io;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.ddlutils.util.OBDatasetTable;
import org.openbravo.retail.storeserver.synchronization.task.PostgreSqlCopyDatabaseDataIO;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * Export data from the database to CSV using PostgreSQL's COPY functionality. The first line of the
 * exported file includes the names of the exported columns
 *
 */
public class PgCopyDatabaseDataIO implements DataSetTableExporter {

  private final Log log = LogFactory.getLog(PostgreSqlCopyDatabaseDataIO.class);

  private static List<String> auditColumnNames = Arrays.asList("CREATED", "UPDATED", "CREATEDBY",
      "UPDATEDBY");

  @Override
  public boolean exportDataSet(Database model, OBDatasetTable dsTable, OutputStream output,
      String moduleId, Map<String, Object> customParams) {
    long count = 0;
    Table table = model.findTable(dsTable.getName());
    String fullwhereclause = dsTable.getWhereclause(moduleId);
    if (dsTable.getSecondarywhereclause() != null) {
      fullwhereclause += " AND " + dsTable.getSecondarywhereclause() + " ";
    }
    BaseConnection connection = null;
    try {
      connection = getPgBaseConnection();
      CopyManager copyManager = new CopyManager(connection);
      StringBuilder copyCommand = new StringBuilder();
      List<String> columns = getNotExcludedColumns(dsTable);
      String query = getQueryToExportData(table, columns, fullwhereclause);
      copyCommand.append("COPY (" + query + ")");
      copyCommand.append(" TO STDOUT WITH (FORMAT CSV, HEADER true)");
      count = copyManager.copyOut(copyCommand.toString(), output);
      if (count > 0) {
        log.info(count + " records exported");
      }
    } catch (Exception e) {
      log.error("Error while exporting table", e);
      if (connection != null) {
        try {
          connection.rollback();
        } catch (SQLException ignore) {
        }
      }
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException ignore) {
        }
      }
    }
    return (count > 0);
  }

  private List<String> getNotExcludedColumns(OBDatasetTable dsTable) {
    List<String> includedColumns = dsTable.getIncludedColumns();
    if (dsTable.isExcludeAuditInfo()) {
      removeAuditInfoColumns(includedColumns);
    }
    return includedColumns;
  }

  private void removeAuditInfoColumns(List<String> includedColumns) {
    List<String> columnToDelete = new ArrayList<String>();
    for (String columnName : includedColumns) {
      if (auditColumnNames.contains(columnName.toUpperCase())) {
        columnToDelete.add(columnName);
      }
    }
    includedColumns.removeAll(columnToDelete);
  }

  private String getQueryToExportData(Table table, List<String> columns, String fullwhereclause) {
    StringBuilder query = new StringBuilder();
    query.append("SELECT " + toCommaSeparatedString(columns) + " FROM " + table.getName());
    if (!StringUtils.isBlank(fullwhereclause)) {
      query.append(" WHERE " + fullwhereclause);
    }
    return query.toString();
  }

  private String toCommaSeparatedString(List<String> columns) {
    StringBuilder listStringified = new StringBuilder();
    Iterator<String> iterator = columns.iterator();
    while (iterator.hasNext()) {
      listStringified.append(iterator.next());
      if (iterator.hasNext()) {
        listStringified.append(",");
      }
    }
    return listStringified.toString();
  }

  private BaseConnection getPgBaseConnection() throws SQLException {
    Properties obProperties = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    String user = obProperties.getProperty("bbdd.user");
    String password = obProperties.getProperty("bbdd.password");
    String url = obProperties.getProperty("bbdd.url");
    String sid = obProperties.getProperty("bbdd.sid");
    Connection connection = DriverManager.getConnection(url + "/" + sid, user, password);
    connection.setAutoCommit(false);
    return (BaseConnection) connection;
  }
}
