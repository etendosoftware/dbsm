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

package org.apache.ddlutils.platform.postgresql;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DataSetTableExporter;
import org.apache.ddlutils.io.DataSetTableQueryGenerator;
import org.apache.ddlutils.io.DataSetTableQueryGeneratorExtraProperties;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.openbravo.ddlutils.util.OBDatasetTable;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * Export data from the database to CSV using PostgreSQL's COPY functionality. The first line of the
 * exported file includes the names of the exported columns
 *
 */
public class PostgreSqlDatabaseDataIO implements DataSetTableExporter {

  private final Log log = LogFactory.getLog(PostgreSqlDatabaseDataIO.class);

  private final static List<String> AUDIT_COLUMN_NAMES = Arrays.asList("CREATED", "UPDATED",
      "CREATEDBY", "UPDATEDBY");

  private DataSetTableQueryGenerator queryGenerator;

  public PostgreSqlDatabaseDataIO() {
    this.queryGenerator = new DataSetTableQueryGenerator();
  }

  public PostgreSqlDatabaseDataIO(DataSetTableQueryGenerator queryGenerator) {
    this.queryGenerator = queryGenerator;
  }

  @Override
  public boolean exportDataSet(Database model, OBDatasetTable dsTable, OutputStream output,
      String moduleId, Map<String, Object> customParams, boolean orderByTableId) {
    long count = 0;

    Platform platform = (Platform) customParams.get("platform");
    Connection connection = platform.borrowConnection();
    try {
      BaseConnection baseConnection = connection.unwrap(BaseConnection.class);
      CopyManager copyManager = new CopyManager(baseConnection);
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
    } finally {
      platform.returnConnection(connection);
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
   * @param platform
   *          Platform instance that will be used to retrieve the database connection
   * @throws DdlUtilsException
   *           if the file cannot be imported using PostgreSQL's COPY
   */
  public void importCopyFile(File file, Platform platform) throws DdlUtilsException {
    String tableName = getTableName(file);
    Connection connection = platform.borrowConnection();
    try (InputStream inputStream = new FileInputStream(file);) {
      BaseConnection baseConnection = connection.unwrap(BaseConnection.class);
      CopyManager copyManager = new CopyManager(baseConnection);
      InputStream bufferedInStream = new BufferedInputStream(inputStream, 65536);
      StringBuilder copyCommand = new StringBuilder();
      copyCommand.append("COPY " + tableName + " ");
      copyCommand.append("(" + getColumnNames(file) + " ) ");
      copyCommand.append("FROM STDIN WITH (FORMAT CSV, HEADER TRUE) ");
      copyManager.copyIn(copyCommand.toString(), bufferedInStream);
    } catch (Exception e) {
      log.error("Error while importing file " + file.getName(), e);
    } finally {
      platform.returnConnection(connection);
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

  private String getTableName(File file) {
    String fileName = file.getName();
    String tableName = fileName.substring(0, fileName.lastIndexOf("."));
    if (Character.isDigit(tableName.charAt(tableName.length() - 1))) {
      tableName = tableName.substring(0, tableName.lastIndexOf("_"));
    }
    return tableName;
  }

}
