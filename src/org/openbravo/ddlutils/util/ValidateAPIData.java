/*
 *************************************************************************
 * The contents of this file are subject to the Openbravo  Public  License
 * Version  1.0  (the  "License"),  being   the  Mozilla   Public  License
 * Version 1.1  with a permitted attribution clause; you may not  use this
 * file except in compliance with the License. You  may  obtain  a copy of
 * the License at http://www.openbravo.com/legal/license.html 
 * Software distributed under the License  is  distributed  on  an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific  language  governing  rights  and  limitations
 * under the License. 
 * The Original Code is Openbravo ERP. 
 * The Initial Developer of the Original Code is Openbravo SL 
 * All portions are Copyright (C) 2009-2010 Openbravo SL 
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */

package org.openbravo.ddlutils.util;

import java.util.List;

import org.apache.ddlutils.alteration.AddRowChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.RemoveRowChange;
import org.apache.ddlutils.dynabean.SqlDynaClass;

public class ValidateAPIData extends ValidateAPI {
  List<Change> changes;

  public ValidateAPIData(List<Change> changes) {
    super();
    valType = ValidationType.DATA;
    this.changes = changes;
  }

  public void execute() {
    for (Change change : changes)
      checkChange(change);
  }

  private void checkChange(Change change) {

    if (change instanceof RemoveRowChange) {
      RemoveRowChange c = (RemoveRowChange) change;
      if (!c.getTable().getName().equalsIgnoreCase("AD_TextInterfaces")) {
        errors.add("Removed row from table "
            + c.getTable().getName()
            + " - ID: "
            + c.getRow().get(
                ((SqlDynaClass) c.getRow().getDynaClass()).getPrimaryKeyProperties()[0].getName()));
      }
    } else if (change instanceof ColumnDataChange) {
      ColumnDataChange c = (ColumnDataChange) change;
      String tableName = c.getTable().getName();
      String columnName = c.getColumnname();
      boolean error = (tableName.equals("AD_TABLE") && (columnName.equals("CLASSNAME")
          || columnName.equals("TABLENAME") || columnName.equals("AD_PACKAGE_ID")));
      error = error
          || (tableName.equals("AD_COLUMN") && (columnName.equals("NAME") || columnName
              .equals("COLUMNNAME")));
      error = error || (tableName.equals("AD_ELEMENT") && columnName.equals("COLUMNNAME"));
      error = error || (tableName.equals("AD_MESSAGE") && columnName.equals("VALUE"));
      error = error
          || (tableName.equals("AD_MODULE") && (columnName.equals("NAME")
              || columnName.equals("JAVAPACKAGE") || columnName.equals("TYPE")));
      error = error || (tableName.equals("AD_MODULE_DBPREFIX"));
      error = error || (tableName.equals("AD_MODULE_DEPENDENCY"));
      error = error || (tableName.equals("AD_MODULE_DBPREFIX"));
      error = error || (tableName.equals("AD_PACKAGE") && columnName.equals("JAVAPACKAGE"));
      error = error
          || (tableName.equals("AD_PROCESS_PARA") && (columnName.equals("COLUMNNAME") || columnName
              .equals("ISRANGE")));
      error = error || (tableName.equals("AD_REF_LIST") && columnName.equals("VALUE"));
      error = error
          || (tableName.equals("AD_REF_SEARCH") && (columnName.equals("AD_TABLE_ID") || columnName
              .equals("AD_COLUMN_ID")));
      error = error
          || (tableName.equals("AD_REF_TABLE") && (columnName.equals("AD_TABLE_ID") || columnName
              .equals("AD_KEY")));
      error = error || (tableName.equals("AD_REFERENCE") && columnName.equals("VALIDATIONTYPE"));
      error = error
          || (tableName.equals("AD_TAB") && (columnName.equals("AD_TABLE_ID") || columnName
              .equals("AD_WINDOW_ID")));
      error = error
          || (tableName.equals("AD_COLUMN") && columnName.equals("ISMANDATORY")
              && c.getOldValue().equals("N") && c.getNewValue().equals("Y"));
      if (error) {
        errors.add("Changed column value " + tableName + "." + columnName + " -ID:" + c.getPkRow()
            + " from [" + c.getOldValue() + "] to [" + c.getNewValue() + "]");
      }
    } else if (change instanceof AddRowChange) {
      AddRowChange c = (AddRowChange) change;
      if (c.getTable().getName().equalsIgnoreCase("AD_PROCESS_PARA")) {
        errors.add("Not Allowed insertions in "
            + c.getTable().getName()
            + " table. ID: "
            + c.getRow().get(
                ((SqlDynaClass) c.getRow().getDynaClass()).getPrimaryKeyProperties()[0].getName()));
      }
    }

  }
}
