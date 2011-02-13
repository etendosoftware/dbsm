package org.openbravo.ddlutils.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.log4j.Logger;

public class OBDataset {
  Vector<OBDatasetTable> tables = new Vector<OBDatasetTable>();

  public OBDataset(DatabaseData databaseData, String name) {
    Database database = databaseData.getDatabase();
    DynaBean dataset = DatabaseData.searchDynaBean(databaseData.getRowsFromTable("AD_DATASET"),
        name, "NAME");
    Vector<DynaBean> alldsTables = databaseData.getRowsFromTable("AD_DATASET_TABLE");
    List<DynaBean> dsTables = DatabaseData.searchDynaBeans(alldsTables, dataset
        .get("AD_DATASET_ID").toString(), "AD_DATASET_ID");
    Vector<DynaBean> adTables = databaseData.getRowsFromTable("AD_TABLE");
    for (DynaBean dsTable : dsTables) {
      OBDatasetTable table = new OBDatasetTable();
      tables.add(table);
      table.setWhereclause(dsTable.get("WHERECLAUSE") == null ? null : dsTable.get("WHERECLAUSE")
          .toString());
      table.setSecondarywhereclause(dsTable.get("SECONDARYWHERECLAUSE") == null ? null : dsTable
          .get("SECONDARYWHERECLAUSE").toString());
      table.setIncludeAllColumns(dsTable.get("INCLUDEALLCOLUMNS").toString().equals("Y"));
      table.setExcludeAuditInfo(dsTable.get("EXCLUDEAUDITINFO").toString().equals("Y"));
      DynaBean adTable = DatabaseData.searchDynaBean(adTables, dsTable.get("AD_TABLE_ID")
          .toString(), "AD_TABLE_ID");
      table.setName(adTable.get("TABLENAME").toString());
      if (table.isIncludeAllColumns()) {
        List<DynaBean> dsCols = DatabaseData.searchDynaBeans(databaseData
            .getRowsFromTable("AD_DATASET_COLUMN"), dsTable.get("AD_DATASET_TABLE_ID").toString(),
            "AD_DATASET_TABLE_ID");
        List<DynaBean> excludedCols = DatabaseData.searchDynaBeans(dsCols, "Y", "ISEXCLUDED");
        for (DynaBean dsCol : dsCols) {
          DynaBean adCol = DatabaseData.searchDynaBean(databaseData.getRowsFromTable("AD_COLUMN"),
              dsCol.get("AD_COLUMN_ID").toString(), "AD_COLUMN_ID");
          if (adCol.get("ISTRANSIENT").toString().equals("Y")) {
            excludedCols.add(adCol);
          }
        }
        Vector<String> excludedColNames = new Vector<String>();
        for (DynaBean db : excludedCols) {
          String colName = DatabaseData.searchDynaBean(databaseData.getRowsFromTable("AD_COLUMN"),
              db.get("AD_COLUMN_ID").toString(), "AD_COLUMN_ID").get("COLUMNNAME").toString();
          excludedColNames.add(colName.toUpperCase());
        }
        Table mTable = database.findTable(table.getName());
        if (mTable == null) {
          throw new DdlUtilsException("Table " + table.getName() + " not found in the database");
        }
        for (int i = 0; i < mTable.getColumnCount(); i++) {
          String col = mTable.getColumn(i).getName();
          if (!excludedColNames.contains(col))
            table.getIncludedColumns().add(col);
        }
      }
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

  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (OBDatasetTable table : tables) {
      sb.append(table.toString() + "\n");
    }
    return sb.toString();
  }

  public boolean hasChanged(Connection connection, Logger log) {
    for (OBDatasetTable table : tables) {
      try {
        String sql = "SELECT count(*) FROM " + table.getName() + " WHERE 1=1 ";
        if (table.getSecondarywhereclause() != null) {
          sql += " AND " + table.getSecondarywhereclause() + " ";
        }
        sql += " AND UPDATED>(SELECT LAST_DBUPDATE FROM AD_SYSTEM_INFO)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.execute();
        ResultSet rs = ps.getResultSet();
        rs.next();
        if (rs.getInt(1) > 0) {
          log.info("Change detected in table: " + table.getName());
          return true;
        }
      } catch (Exception e) {
        // We do nothing if the select fails in one table. This can happen if a new table has been
        // added to the dataset AD, but it still doesn't exist in the model. In any case, if
        // something fails here, there shouldn't be a warning
      }
    }
    return false;
  }

}
