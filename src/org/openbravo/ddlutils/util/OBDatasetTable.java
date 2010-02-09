package org.openbravo.ddlutils.util;

import java.util.Vector;

public class OBDatasetTable {

  String name;
  boolean excludeAuditInfo;
  String whereclause;
  boolean includeAllColumns;
  Vector<String> includedColumns = new Vector<String>();
  String allModuleIds;

  public String toString() {
    String cols = "";
    for (String col : includedColumns)
      cols += "           - " + col + "\n";
    return "   - Table " + name + ". Whereclause=" + whereclause + "|| Included cols: " + cols;
  }

  public boolean includesColumn(String column) {
    if (excludeAuditInfo) {
      if (column.equalsIgnoreCase("CREATED") || column.equalsIgnoreCase("UPDATED")
          || column.equalsIgnoreCase("UPDATEDBY") || column.equalsIgnoreCase("CREATEDBY"))
        return false;
    }
    return includedColumns.contains(column);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isExcludeAuditInfo() {
    return excludeAuditInfo;
  }

  public void setExcludeAuditInfo(boolean excludeAuditInfo) {
    this.excludeAuditInfo = excludeAuditInfo;
  }

  public String getWhereclause(String moduleId) {
    if (whereclause == null)
      return null;
    if (moduleId != null) {
      return whereclause.replace("'AD_MODULE_ID'", "'0'").replace("':moduleid'", ":moduleid")
          .replace(":moduleid", "'" + moduleId + "'");
    } else {
      return whereclause.replace(":moduleid", allModuleIds);
    }
  }

  public void setWhereclause(String whereclause) {
    this.whereclause = whereclause;
  }

  public boolean isIncludeAllColumns() {
    return includeAllColumns;
  }

  public void setIncludeAllColumns(boolean includeAllColumns) {
    this.includeAllColumns = includeAllColumns;
  }

  public Vector<String> getIncludedColumns() {
    return includedColumns;
  }

  public void setIncludedColumns(Vector<String> includedColumns) {
    this.includedColumns = includedColumns;
  }

  public String getAllModuleIds() {
    return allModuleIds;
  }

  public void setAllModuleIds(String allModuleIds) {
    this.allModuleIds = allModuleIds;
  }

}
