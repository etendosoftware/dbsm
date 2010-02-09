package org.openbravo.ddlutils.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.log4j.Logger;

public class OBDataset {
  Vector<OBDatasetTable> tables = new Vector<OBDatasetTable>();

  public OBDataset(DatabaseData databaseData, String name) {
    String allModuleIds = "SELECT AD_MODULE_ID FROM AD_MODULE";
    Database database = databaseData.getDatabase();
    DynaBean dataset = searchDynaBean(databaseData.getRowsFromTable("AD_DATASET"), name, "NAME");
    Vector<DynaBean> alldsTables = databaseData.getRowsFromTable("AD_DATASET_TABLE");
    Vector<DynaBean> dsTables = searchDynaBeans(alldsTables, dataset.get("AD_DATASET_ID")
        .toString(), "AD_DATASET_ID");
    Vector<DynaBean> adTables = databaseData.getRowsFromTable("AD_TABLE");
    for (DynaBean dsTable : dsTables) {
      OBDatasetTable table = new OBDatasetTable();
      tables.add(table);
      table.setWhereclause(dsTable.get("WHERECLAUSE")==null?null:dsTable.get("WHERECLAUSE").toString());
      table.setIncludeAllColumns(dsTable.get("INCLUDEALLCOLUMNS").toString().equals("Y"));
      table.setExcludeAuditInfo(dsTable.get("EXCLUDEAUDITINFO").toString().equals("Y"));
      table.setAllModuleIds(allModuleIds);
      DynaBean adTable = searchDynaBean(adTables, dsTable.get("AD_TABLE_ID").toString(),
          "AD_TABLE_ID");
      table.setName(adTable.get("TABLENAME").toString());
      if (table.isIncludeAllColumns()) {
        Vector<DynaBean> dsCols = searchDynaBeans(databaseData
            .getRowsFromTable("AD_DATASET_COLUMN"), dsTable.get("AD_DATASET_TABLE_ID").toString(),
            "AD_DATASET_TABLE_ID");
        Vector<DynaBean> excludedCols = searchDynaBeans(dsCols, "Y", "ISEXCLUDED");
        for (DynaBean dsCol : dsCols) {
          DynaBean adCol = searchDynaBean(databaseData.getRowsFromTable("AD_COLUMN"), dsCol.get(
              "AD_COLUMN_ID").toString(), "AD_COLUMN_ID");
          if (adCol.get("ISTRANSIENT").toString().equals("Y")) {
            excludedCols.add(adCol);
          }
        }
        Vector<String> excludedColNames = new Vector<String>();
        for (DynaBean db : excludedCols) {
          String colName = searchDynaBean(databaseData.getRowsFromTable("AD_COLUMN"),
              db.get("AD_COLUMN_ID").toString(), "AD_COLUMN_ID").get("COLUMNNAME").toString();
          excludedColNames.add(colName.toUpperCase());
        }
        Table mTable = database.findTable(table.getName());
        for (int i = 0; i < mTable.getColumnCount(); i++) {
          String col = mTable.getColumn(i).getName();
          if (!excludedColNames.contains(col))
            table.getIncludedColumns().add(col);
        }
      }
    }

  }

  private DynaBean searchDynaBean(Vector<DynaBean> vector, String name, String property) {
    for (DynaBean bean : vector) {
      if (bean.get(property).toString().equalsIgnoreCase(name))
        return bean;
    }
    return null;
  }

  private Vector<DynaBean> searchDynaBeans(Vector<DynaBean> vector, String name, String property) {
    Vector<DynaBean> dbs = new Vector<DynaBean>();
    for (DynaBean bean : vector) {
      if (bean.get(property).toString().equalsIgnoreCase(name))
        dbs.add(bean);
    }
    return dbs;
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
    try {
      for (OBDatasetTable table : tables) {
        PreparedStatement ps = connection.prepareStatement("SELECT count(*) FROM "
            + table.getName() + " WHERE UPDATED>(SELECT LAST_DBUPDATE FROM AD_SYSTEM_INFO)");
        ps.execute();
        ResultSet rs = ps.getResultSet();
        rs.next();
        if (rs.getInt(1) > 0) {
          log.warn("Change detected in table: " + table.getName());
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      e.printStackTrace();
      log.warn("Error while checking changes in the application dictionary.");
      return false;
    }
  }
}
