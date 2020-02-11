/*
 ************************************************************************************
 * Copyright (C) 2010-2020 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OBDataset {
  private static final Logger logger = LogManager.getLogger();

  Vector<OBDatasetTable> tables = new Vector<OBDatasetTable>();
  DatabaseData databaseData;
  Database database;
  Platform platform;

  public OBDataset(Platform platform, Database database, String name) {
    this.platform = platform;
    this.database = database;
    DynaBean dataset = searchDynaBeans("AD_DATASET", true, name, "NAME").get(0);
    createDataset(dataset, true);
  }

  public OBDataset(DatabaseData databaseData, String name) {
    this.database = databaseData.getDatabase();
    this.databaseData = databaseData;
    if (name != null) {
      DynaBean dataset = searchDynaBeans("AD_DATASET", false, name, "NAME").get(0);
      createDataset(dataset, false);
    }
  }

  public OBDataset(DatabaseData databaseData) {
    this.database = databaseData.getDatabase();
    this.databaseData = databaseData;
  }

  private void createDataset(DynaBean dataset, boolean readFromDb) {
    // Vector<DynaBean> alldsTables = getDynaBeans("AD_DATASET_TABLE", readFromDb);
    List<DynaBean> dsTables = searchDynaBeans("AD_DATASET_TABLE", readFromDb,
        dataset.get("AD_DATASET_ID").toString(), "AD_DATASET_ID");
    Vector<DynaBean> adTables = getDynaBeans("AD_TABLE", readFromDb);
    for (DynaBean dsTable : dsTables) {
      OBDatasetTable table = new OBDatasetTable();
      tables.add(table);
      table.setWhereclause(
          dsTable.get("WHERECLAUSE") == null ? null : dsTable.get("WHERECLAUSE").toString());
      table.setSecondarywhereclause(dsTable.get("SECONDARYWHERECLAUSE") == null ? null
          : dsTable.get("SECONDARYWHERECLAUSE").toString());
      table.setIncludeAllColumns(dsTable.get("INCLUDEALLCOLUMNS").toString().equals("Y"));
      table.setExcludeAuditInfo(dsTable.get("EXCLUDEAUDITINFO").toString().equals("Y"));
      DynaBean adTable = DatabaseData.searchDynaBean(adTables,
          dsTable.get("AD_TABLE_ID").toString(), "AD_TABLE_ID");
      table.setName(adTable.get("TABLENAME").toString());
      table.setDataSetTableId(dsTable.get("AD_DATASET_TABLE_ID").toString());
      if (table.isIncludeAllColumns()) {
        List<DynaBean> dsCols = searchDynaBeans("AD_DATASET_COLUMN", readFromDb,
            dsTable.get("AD_DATASET_TABLE_ID").toString(), "AD_DATASET_TABLE_ID");
        List<DynaBean> excludedCols = DatabaseData.searchDynaBeans(dsCols, "Y", "ISEXCLUDED");
        for (DynaBean dsCol : dsCols) {
          DynaBean adCol = searchDynaBeans("AD_COLUMN", readFromDb,
              dsCol.get("AD_COLUMN_ID").toString(), "AD_COLUMN_ID").get(0);
          if (adCol.get("ISTRANSIENT").toString().equals("Y")) {
            excludedCols.add(adCol);
          }
        }
        Vector<String> excludedColNames = new Vector<String>();
        for (DynaBean db : excludedCols) {
          String colName = searchDynaBeans("AD_COLUMN", readFromDb,
              db.get("AD_COLUMN_ID").toString(), "AD_COLUMN_ID").get(0)
                  .get("COLUMNNAME")
                  .toString();
          excludedColNames.add(colName.toUpperCase());
        }
        Table mTable = database.findTable(table.getName());
        if (mTable == null) {
          throw new DdlUtilsException("Table " + table.getName() + " not found in the database");
        }
        for (int i = 0; i < mTable.getColumnCount(); i++) {
          String col = mTable.getColumn(i).getName();
          if (!excludedColNames.contains(col)) {
            table.getIncludedColumns().add(col);
          }
        }
      }
    }
  }

  private List<DynaBean> searchDynaBeans(String tablename, boolean readFromDb, String colValue,
      String colName) {
    if (readFromDb) {
      Connection connection = platform.borrowConnection();
      Table table = database.findTable(tablename);
      DatabaseDataIO dbIO = new DatabaseDataIO();
      OBDatasetTable dsTable = new OBDatasetTable();
      dsTable.setWhereclause("1=1");
      dsTable.setSecondarywhereclause(colName + "=" + "'" + colValue + "'");
      Vector<DynaBean> rowsNewData = dbIO.readRowsFromTableList(connection, platform, database,
          table, dsTable, null);
      platform.returnConnection(connection);
      return rowsNewData;
    } else {
      return DatabaseData.searchDynaBeans(getDynaBeans(tablename, readFromDb), colValue, colName);
    }

  }

  private Vector<DynaBean> getDynaBeans(String tablename, boolean readFromDb) {
    if (readFromDb) {
      Connection connection = platform.borrowConnection();
      Table table = database.findTable(tablename);
      DatabaseDataIO dbIO = new DatabaseDataIO();
      Vector<DynaBean> rowsNewData = dbIO.readRowsFromTableList(connection, platform, database,
          table, new OBDatasetTable(), null);
      platform.returnConnection(connection);
      return rowsNewData;
    } else {
      return databaseData.getRowsFromTable(tablename);
    }
  }

  public OBDatasetTable getTable(String tablename) {
    for (OBDatasetTable table : tables) {
      if (table.getName().equalsIgnoreCase(tablename)) {
        return table;
      }
    }
    return null;
  }

  public Vector<OBDatasetTable> getTableList() {
    return tables;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (OBDatasetTable table : tables) {
      sb.append(table.toString() + "\n");
    }
    return sb.toString();
  }

  public boolean hasChanged(Connection connection, Logger log) {
    return hasChanged(connection, log, null);
  }

  public boolean hasChanged(Connection connection, Logger log, List<String> modifiedTables) {
    boolean hasChanges = false;
    for (OBDatasetTable table : tables) {
      try {
        String sql = "SELECT count(*) FROM " + table.getName() + " WHERE 1=1 ";
        if (table.getSecondarywhereclause() != null) {
          sql += " AND " + table.getSecondarywhereclause() + " ";
        }
        sql += " AND UPDATED>(SELECT LAST_DBUPDATE FROM AD_SYSTEM_INFO)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
          ps.execute();
          ResultSet rs = ps.getResultSet();
          rs.next();
          if (rs.getInt(1) > 0) {
            logger.warn("Change detected in table: {}", table.getName());
            if (modifiedTables != null) {
              modifiedTables.add(table.getName());
            }
            hasChanges = true;
          }
        }
      } catch (Exception e) {
        // We do nothing if the select fails in one table. This can happen if a new table has been
        // added to the dataset AD, but it still doesn't exist in the model. In any case, if
        // something fails here, there shouldn't be a warning
      }
    }
    return hasChanges;
  }

  /**
   * @deprecated Use with log4j2 logger: hasChanged(Connection, org.apache.logging.log4j.Logger)
   */
  @Deprecated
  public boolean hasChanged(Connection connection, org.apache.log4j.Logger log) {
    return hasChanged(connection, (Logger) null, null);
  }

  /**
   * @deprecated Use with log4j2 logger: hasChanged(Connection, org.apache.logging.log4j.Logger,
   *             List<String>)
   */
  @Deprecated
  public boolean hasChanged(Connection connection, org.apache.log4j.Logger log,
      List<String> modifiedTables) {
    return hasChanged(connection, (Logger) null, modifiedTables);
  }

  public void setTables(Vector<OBDatasetTable> tables2) {
    this.tables = tables2;

  }
}
