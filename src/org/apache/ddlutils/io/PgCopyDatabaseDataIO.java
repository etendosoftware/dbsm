/*
 ************************************************************************************
 * Copyright (C) 2016 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.io;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.ddlutils.util.OBDatasetTable;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * Export data from the database to CSV using PostgreSQL's COPY functionality. The first line of the
 * exported file includes the names of the exported columns
 *
 */
public class PgCopyDatabaseDataIO implements DataSetTableExporter {

  private final Log log = LogFactory.getLog(PgCopyDatabaseDataIO.class);

  private final static List<String> AUDIT_COLUMN_NAMES = Arrays.asList("CREATED", "UPDATED",
      "CREATEDBY", "UPDATEDBY");

  private DataSetTableQueryGenerator queryGenerator;

  public PgCopyDatabaseDataIO() {
    this.queryGenerator = new DataSetTableQueryGenerator();
  }

  public PgCopyDatabaseDataIO(DataSetTableQueryGenerator queryGenerator) {
    this.queryGenerator = queryGenerator;
  }

  @Override
  public boolean exportDataSet(Database model, OBDatasetTable dsTable, OutputStream output,
      String moduleId, Map<String, Object> customParams, boolean orderByTableId) {
    long count = 0;
    try (BaseConnection connection = getPgBaseConnection()) {
      CopyManager copyManager = new CopyManager(connection);
      StringBuilder copyCommand = new StringBuilder();
      List<String> columns = getNotExcludedColumns(dsTable);

      DataSetTableQueryGeneratorExtraProperties extraProperties = new DataSetTableQueryGeneratorExtraProperties();
      if (orderByTableId) {
        Table table = model.findTable(dsTable.getName());
        extraProperties.setOrderByClause(queryGenerator.buildOrderByClauseUsingKeyColumns(table));
      }
      String query = queryGenerator.generateQuery(dsTable, columns, extraProperties);
      copyCommand.append("COPY (" + query + ")");
      copyCommand.append(" TO STDOUT WITH (FORMAT CSV, HEADER true)");
      count = copyManager.copyOut(copyCommand.toString(), output);
      if (count > 0) {
        log.info(count + " records exported");
      }
    } catch (Exception e) {
      log.error("Error while exporting table", e);
    }
    return count > 0;
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
      if (AUDIT_COLUMN_NAMES.contains(columnName.toUpperCase())) {
        columnToDelete.add(columnName);
      }
    }
    includedColumns.removeAll(columnToDelete);
  }

  /**
   * Imports into the database a CSV file that has been exported using PostgreSQL's COPY
   * 
   * @param file
   *          The file being imported
   * @throws DdlUtilsException
   *           if the file cannot be imported using PostgreSQL's COPY
   */
  public void importCopyFile(File file) throws DdlUtilsException {
    String tableName = getTableName(file);
    try (BaseConnection connection = getPgBaseConnection();
        InputStream inputStream = new FileInputStream(file);) {
      CopyManager copyManager = new CopyManager(connection);
      InputStream bufferedInStream = new BufferedInputStream(inputStream, 65536);
      StringBuilder copyCommand = new StringBuilder();
      copyCommand.append("COPY " + tableName + " ");
      copyCommand.append("(" + getColumnNames(file) + " ) ");
      copyCommand.append("FROM STDIN WITH (FORMAT CSV, HEADER TRUE) ");
      copyManager.copyIn(copyCommand.toString(), bufferedInStream);
      connection.commit();
    } catch (Exception e) {
      log.error("Error while importing file " + file.getName(), e);
    }
  }

  // the names of the exported columns are placed in the first line of the file
  private String getColumnNames(File file) {
    String columnNames = null;
    try (InputStream inputStream = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream,
            StandardCharsets.UTF_8));) {
      columnNames = br.readLine();
    } catch (Exception e) {
      log.error("Error while reading the column names of a .copy file", e);
    }
    return columnNames;
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

  private String getTableName(File file) {
    String fileName = file.getName();
    String tableName = fileName.substring(0, fileName.lastIndexOf("."));
    if (Character.isDigit(tableName.charAt(tableName.length() - 1))) {
      tableName = tableName.substring(0, tableName.lastIndexOf("_"));
    }
    return tableName;
  }

}
