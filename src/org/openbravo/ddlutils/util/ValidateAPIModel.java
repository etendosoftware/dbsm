/*
 *************************************************************************
 * The contents of this file are subject to the Openbravo  Public  License
 * Version  1.1  (the  "License"),  being   the  Mozilla   Public  License
 * Version 1.1  with a permitted attribution clause; you may not  use this
 * file except in compliance with the License. You  may  obtain  a copy of
 * the License at http://www.openbravo.com/legal/license.html 
 * Software distributed under the License  is  distributed  on  an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific  language  governing  rights  and  limitations
 * under the License. 
 * The Original Code is Openbravo ERP. 
 * The Initial Developer of the Original Code is Openbravo SLU 
 * All portions are Copyright (C) 2009-2010 Openbravo SLU 
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */

package org.openbravo.ddlutils.util;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.AddCheckChange;
import org.apache.ddlutils.alteration.AddColumnChange;
import org.apache.ddlutils.alteration.AddForeignKeyChange;
import org.apache.ddlutils.alteration.AddFunctionChange;
import org.apache.ddlutils.alteration.AddIndexChange;
import org.apache.ddlutils.alteration.AddPrimaryKeyChange;
import org.apache.ddlutils.alteration.AddSequenceChange;
import org.apache.ddlutils.alteration.AddUniqueChange;
import org.apache.ddlutils.alteration.AddViewChange;
import org.apache.ddlutils.alteration.ColumnAutoIncrementChange;
import org.apache.ddlutils.alteration.ColumnChange;
import org.apache.ddlutils.alteration.ColumnDataTypeChange;
import org.apache.ddlutils.alteration.ColumnRequiredChange;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.ModelComparator;
import org.apache.ddlutils.alteration.PrimaryKeyChange;
import org.apache.ddlutils.alteration.RemoveColumnChange;
import org.apache.ddlutils.alteration.RemoveForeignKeyChange;
import org.apache.ddlutils.alteration.RemoveFunctionChange;
import org.apache.ddlutils.alteration.RemovePrimaryKeyChange;
import org.apache.ddlutils.alteration.RemoveTableChange;
import org.apache.ddlutils.alteration.RemoveViewChange;
import org.apache.ddlutils.alteration.TableChange;
import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Unique;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.util.ExtTypes;

public class ValidateAPIModel extends ValidateAPI {
  Database validDB;
  Database testDB;
  Platform platform;
  OBDataset obDataset;

  /**
   * Creates a {@link ValidateAPIModel} object with the two {@link Database} objects to be compared
   * 
   * @param platform
   *          {@link Platform} to obtain information from
   * @param valid
   *          Database that is the base for the comparation
   * @param test
   *          Database to be tested
   */
  public ValidateAPIModel(Platform platform, Database valid, Database test, OBDataset obDataset) {
    super();
    validDB = valid;
    testDB = test;
    this.platform = platform;
    valType = ValidationType.MODEL;
    this.obDataset = obDataset;
  }

  /**
   * Executes the comparation
   */
  @Override
  @SuppressWarnings("unchecked")
  public void execute() {
    ModelComparator modelComparator = new ModelComparator(platform.getPlatformInfo(),
        platform.isDelimitedIdentifierModeOn());
    List<ModelChange> modelChanges = modelComparator.compare(validDB, testDB);
    List<ModelChange> removeViewChanges = getRemoveViewChanges(validDB, testDB);
    modelChanges.addAll(removeViewChanges);
    checkAllChanges(modelChanges);
  }

  private List<ModelChange> getRemoveViewChanges(Database sourceModel, Database targetModel) {
    ArrayList<ModelChange> changes = new ArrayList<ModelChange>();
    for (int viIdx = 0; viIdx < sourceModel.getViewCount(); viIdx++) {
      View sourceView = sourceModel.getView(viIdx);
      View targetView = findCorrespondingView(targetModel, sourceView);

      if (targetView == null) {
        boolean foundViewWithSameName = false;
        int i = 0;
        while (i < targetModel.getViewCount() && !foundViewWithSameName) {
          if (targetModel.getView(i).getName().equalsIgnoreCase(sourceView.getName())) {
            foundViewWithSameName = true;
          }
          i++;
        }
        changes.add(new RemoveViewChange(sourceView));
        // }
      }
    }
    return changes;
  }

  /**
   * Searches in the given database for a corresponding view. If the given view has no name, then a
   * view (but not necessarily in the same order) is searched. If the given view has a name, then
   * the corresponding view also needs to have the same name, or no name at all, but not a different
   * one.
   * 
   * @param database
   *          The database to search in
   * @param view
   *          The original view
   * @return The corresponding view if found
   */
  private View findCorrespondingView(Database database, View view) {
    for (int viIdx = 0; viIdx < database.getViewCount(); viIdx++) {
      View curView = database.getView(viIdx);

      if (view.equalsIgnoreCase(curView)) {
        return curView;
      }
    }
    return null;
  }

  private void checkAllChanges(List<ModelChange> changes) {
    if (changes != null && changes.size() > 0) {
      for (ModelChange change : changes) {
        checkChange(change);
      }
    }
  }

  private void checkChange(ModelChange change) {
    if (change instanceof TableChange) {
      String tablename = ((TableChange) change).getChangedTable().getName();

      if (change instanceof AddColumnChange) {
        AddColumnChange c = (AddColumnChange) change;
        Column oldCol = validDB.findTable(c.getChangedTable().getName())
            .findColumn(c.getNewColumn().getName());
        if (oldCol == null && c.getNewColumn().isRequired()
            && c.getNewColumn().getOnCreateDefault() == null
            && c.getNewColumn().getDefaultValue() == null) {
          // it is a real creation, not a re-creation
          errors.add("Added mandatory column without default or onCreateDefault: " + tablename + "."
              + c.getNewColumn().getName());
        }
        if (oldCol == null && c.getNewColumn().isRequired()
            && c.getNewColumn().getOnCreateDefault() == null && isADTable(tablename)) {
          errors.add(
              "Added mandatory column without onCreateDefault in a table contained in the AD dataset: "
                  + tablename + "." + c.getNewColumn().getName());
        }
        if ("ad_module".equalsIgnoreCase(tablename) && c.getNewColumn().isRequired()) {
          // Check if the column has been added to ad_module_install
          Column adModuleInstallColumn = testDB.findTable("ad_module_install", false)
              .findColumn(c.getNewColumn().getName(), false);
          if (adModuleInstallColumn == null) {
            errors.add("A mandatory column (" + c.getNewColumn().getName()
                + ") has been added to ad_module but not to ad_module_install");
          }
        }
      } else if (change instanceof AddForeignKeyChange) {
        AddForeignKeyChange c = (AddForeignKeyChange) change;
        if (validDB.findTable(tablename) != null) { // no new table
          ForeignKey fk = validDB.findTable(tablename).findForeignKey(c.getNewForeignKey());
          if (fk != null) {
            errors.add("Added foreign key: table: " + tablename + " - FK name: "
                + c.getNewForeignKey().getName());
          }
        }
      } else if (change instanceof RemoveForeignKeyChange) {
        RemoveForeignKeyChange c = (RemoveForeignKeyChange) change;
        // let's check whether the fk is in new model (if it is there it is not a removal but a
        // modification)
        if (testDB.findTable(tablename) != null) {
          ForeignKey fk = testDB.findTable(tablename).findForeignKey(c.getForeignKey());

          if (fk == null) {
            ForeignKey[] fks = testDB.findTable(tablename).getForeignKeys();
            boolean found = false;
            for (int i = 0; i < fks.length && !found; i++) {
              found = fks[i].getName().equals(c.getForeignKey().getName());
            }
            if (found) {
              warnings.add("Changed Foreign Key: table: " + tablename + " - FK: "
                  + c.getForeignKey().getName());
            } else {
              warnings.add("Removed Foreign Key: table: " + tablename + " - FK: "
                  + c.getForeignKey().getName());
            }
          }
        }
      } else if (change instanceof RemovePrimaryKeyChange) {
        RemovePrimaryKeyChange c = (RemovePrimaryKeyChange) change;
        warnings.add(
            "Removed Primary Key: table: " + tablename + " - Columns: " + c.getPrimaryKeyColumns());
      } else if (change instanceof AddCheckChange) {
        // A change in a check is a removal and an addition, it must be checked
        AddCheckChange c = (AddCheckChange) change;
        Check validCheck = validDB.findTable(tablename).findCheck(c.getNewCheck().getName());
        if (validCheck != null) { // the constraint was previously in the table
          warnings.add("Check Constraint changed: table:" + tablename + " - Constraint: "
              + validCheck.getName() + " from condition: [" + validCheck.getCondition() + "] to ["
              + c.getNewCheck().getCondition() + "]");
        } else {
          /*
           * check if that new check-constraint is the usual boolean constraint. and if it is also
           * based on a newly added column. Together when adding new column + new boolean constraint
           * in same run of api-check -> no complaint should be generated
           */
          boolean ignoreStdBooleanConstraintNewColumn = false;

          String constraintDef = c.getNewCheck().getCondition();
          Pattern p = Pattern.compile("^(.+) IN \\('Y', 'N'\\)$");
          Matcher m = p.matcher(constraintDef);
          // pattern matches usual boolean constraint?
          if (m.find()) {
            String columnName = m.group(1);
            String tableName = c.getChangedTable().getName();

            Table oldTable = validDB.findTable(tableName);
            Table newTable = testDB.findTable(tableName);
            Column oldColumn = oldTable.findColumn(columnName);
            Column newColumn = newTable.findColumn(columnName);

            // ignore, if column 'appeared' in same api-checks run
            if (oldColumn == null && newColumn != null) {
              ignoreStdBooleanConstraintNewColumn = true;
            }
          }
          if (ignoreStdBooleanConstraintNewColumn) {
            // ignore as that is the standard boolean constraint added for a column also newly added
          } else {
            errors.add("Check Constraint addition: table: " + tablename + " - Constraint: "
                + c.getNewCheck().getName() + " defined as: " + c.getNewCheck().getCondition());
          }

        }
      } else if (change instanceof AddIndexChange) {
        AddIndexChange c = (AddIndexChange) change;
        if (c.getNewIndex().isUnique()) {
          errors.add("Unique index addition: table: " + tablename + " - Index: "
              + c.getNewIndex().getName());
        }
      } else if (change instanceof AddPrimaryKeyChange) {
        AddPrimaryKeyChange c = (AddPrimaryKeyChange) change;
        warnings.add("Added PK: table: " + tablename + " - PK: " + c.getprimaryKeyName());
      } else if (change instanceof AddUniqueChange) {
        /*
         * Disalllowed changes in Unique constraints:
         * 
         * -Delete a column from a existent contraint
         * 
         * -Create a new constraint with already existent columns
         * 
         * Allowed changes in Unique constraints:
         * 
         * -Create a new constraint composed just by new columns in the model
         * 
         * -Add column to an existent constraint
         */
        AddUniqueChange c = (AddUniqueChange) change;
        Unique originalUnique = validDB.findTable(tablename).findUnique(c.getNewUnique().getName());
        Unique newUnique = c.getNewUnique();

        if (originalUnique == null) {
          // New constraint, it only can have new columns in the model
          for (IndexColumn cols : newUnique.getColumns()) {
            if (!(validDB.findTable(tablename) == null
                || validDB.findTable(tablename).findColumn(cols.getName()) == null)) {
              // error only in case the constraint is for an already existent table/column
              errors.add("Unique constraint added: table: " + tablename + " - Unique constraint: "
                  + newUnique.getName());
              break;
            }
          }
        } else {
          // Constraint modification
          // check no columns where removed from original constraint
          for (IndexColumn colOriginal : originalUnique.getColumns()) {
            boolean found = false;
            for (IndexColumn colNew : newUnique.getColumns()) {
              if (colNew.getName().equals(colOriginal.getName())) {
                found = true;
                break;
              }
            }
            if (!found) {
              errors.add("Unique constraint modified, column removed: table: " + tablename
                  + " - Unique constraint: " + newUnique.getName() + " - removed column "
                  + colOriginal.getName());
            }
          }

        }
      } else if (change instanceof PrimaryKeyChange) {
        PrimaryKeyChange c = (PrimaryKeyChange) change;

        // check changes in the cols in the PK
        Column[] originalPKCols = c.getOldPrimaryKeyColumns();
        Column[] testPKCols = c.getNewPrimaryKeyColumns();
        boolean fail = (originalPKCols.length != testPKCols.length);
        if (!fail) {
          for (Column col : originalPKCols) {
            boolean found = false;
            for (int i = 0; i < testPKCols.length
                && !(found = (col.getName().equals(testPKCols[i].getName()))); i++) {
              ;
            }
            fail = !found;
          }
        }
        if (fail) {
          errors.add("Changed cols in Primary Key for table:" + tablename + " - from "
              + getColsNames(originalPKCols) + " to " + getColsNames(testPKCols));
        }

      } else if (change instanceof ColumnChange) {
        String columnname = ((ColumnChange) change).getChangedColumn().getName();
        String tableColumn = tablename + "." + columnname;
        if (change instanceof ColumnDataTypeChange) {
          ColumnDataTypeChange c = (ColumnDataTypeChange) change;
          Column originalColumn = validDB.findTable(tablename).findColumn(columnname);
          int originalType = originalColumn.getTypeCode();
          int testType = c.getNewTypeCode();
          if (originalType == ExtTypes.NVARCHAR && testType == Types.VARCHAR) {
            errors.add("Column type change from NVARCHAR to VARCHAR: column:" + tableColumn);
          } else if (originalColumn.isOfTextType() && c.getChangedColumn().isOfNumericType()) {
            errors.add("Column type change from text to numeric: column:" + tableColumn);
          } else if (originalType != Types.TIMESTAMP && testType == Types.TIMESTAMP) {
            errors.add("Column type change from " + c.getChangedColumn().getType() + " to DATE");
          } else if (originalType != testType) {
            warnings.add("Column type change from " + originalColumn.getType() + " to "
                + testDB.findTable(c.getChangedTable().getName())
                    .findColumn(c.getChangedColumn().getName())
                    .getType()
                + ": column:" + tableColumn);
          }
        } else if (change instanceof ColumnAutoIncrementChange) {
          errors.add("Column changed to auto increment: column" + tableColumn);
        } else if (change instanceof ColumnRequiredChange) {
          ColumnRequiredChange c = (ColumnRequiredChange) change;
          boolean oldRequired = c.getChangedColumn().isRequired();
          boolean newRequired = testDB.findTable(tablename).findColumn(columnname).isRequired();
          if (!oldRequired && newRequired) {
            errors.add("Column change from not required to required: column: " + tableColumn);
          }
          if (oldRequired && !newRequired) {
            errors.add("Column change from required to not required: column: " + tableColumn);
          }
        } else if (change instanceof ColumnSizeChange) {
          int testSize = testDB.findTable(tablename).findColumn(columnname).getSizeAsInt();
          int originalSize = validDB.findTable(tablename).findColumn(columnname).getSizeAsInt();
          if (testSize < originalSize) {
            errors.add("Column size decreased from " + originalSize + " to " + testSize
                + ": column: " + tableColumn);
          } else {
            warnings.add("Column size changed from " + originalSize + " to " + testSize
                + ": column: " + tableColumn);
          }
        }
      } else if (change instanceof RemoveColumnChange) {
        RemoveColumnChange c = (RemoveColumnChange) change;
        errors.add(
            "Removed column: " + c.getChangedTable().getName() + "." + c.getColumn().getName());
      } else if (change instanceof RemoveTableChange) {
        RemoveTableChange c = (RemoveTableChange) change;
        errors.add("Removed table: " + c.getChangedTable().getName());
      }
    } else if (change instanceof AddSequenceChange) {
      AddSequenceChange c = (AddSequenceChange) change;
      errors.add("Added sequence :" + c.getNewSequence().getName());
    } else if (change instanceof RemoveViewChange) {
      // any modification in a view is a view remove and add, must check different changes
      RemoveViewChange c = (RemoveViewChange) change;
      View validView = testDB.findView(c.getView().getName());
      if (validView == null) {
        errors.add("Removed view :" + c.getView().getName());
      }
    } else if (change instanceof AddViewChange) {
      AddViewChange c = (AddViewChange) change;
      View validView = validDB.findView(c.getNewView().getName());
      if (validView != null) {
        warnings.add("Modified view " + validView.getName());
      }
    } else if (change instanceof RemoveFunctionChange) {
      // Any modification in functions consists in a remove and add, must check different changes
      RemoveFunctionChange c = (RemoveFunctionChange) change;
      Function validFunction = testDB.findFunction(c.getFunction().getName());
      if (validFunction == null) {
        errors.add("Removed function " + c.getFunction().getName());
      }
    } else if (change instanceof AddFunctionChange) {
      AddFunctionChange c = (AddFunctionChange) change;
      Function validFunction = validDB.findFunction(c.getNewFunction().getName());
      if (validFunction != null
          && !validFunction.getNotation().equals(c.getNewFunction().getNotation())) {
        // Modifying an existent function, let's check notation. New functions are allowed
        errors.add("Changed parameters for function: " + c.getNewFunction().getName() + " from "
            + validFunction.getNotation() + " to " + c.getNewFunction().getNotation());
      }
    }

  }

  private boolean isADTable(String tablename) {
    return obDataset.getTable(tablename) != null ? true : false;
  }

  private String getColsNames(Column[] c) {
    if (c == null) {
      return "";
    }
    String rt = "";

    for (Column col : c) {
      rt += col.getName() + ", ";
    }
    return rt.substring(0, rt.length() - 2);
  }

}
