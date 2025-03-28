package org.apache.ddlutils.model;

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

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.function.Predicate;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.ddlutils.alteration.AddColumnChange;
import org.apache.ddlutils.alteration.ColumnChange;
import org.apache.ddlutils.dynabean.DynaClassCache;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.dynabean.SqlDynaException;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.ddlutils.util.ExtTypes;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
 * Represents the database model, ie. the tables in the database. It also contains the corresponding
 * dyna classes for creating dyna beans for the objects stored in the tables.
 * 
 * @version $Revision: 504014 $
 */
public class Database implements Serializable, Cloneable {
  /** Unique ID for serialization purposes. */
  private static final long serialVersionUID = -3160443396757573868L;

  /** The name of the database model. */
  private String _name;
  /** The method for generating primary keys (currently ignored). */
  private String _idMethod;
  /** The version of the model. */
  private String _version;
  /** The tables. */
  private ArrayList _tables = new ArrayList();
  /** The sequences. */
  private ArrayList _sequences = new ArrayList();
  /** The views. */
  private ArrayList _views = new ArrayList();

  private List<MaterializedView> materializedViews = new ArrayList<>();
  /** The functions. */
  private ArrayList _functions = new ArrayList();
  /** The functions. */
  private ArrayList _triggers = new ArrayList();
  /** The dyna class cache for this model. */
  private transient DynaClassCache _dynaClassCache = null;

  private ArrayList _modifiedTables = new ArrayList();

  private List<ColumnChange> deferredNotNulls = new ArrayList<ColumnChange>();
  private List<AddColumnChange> addedColumns = new ArrayList<AddColumnChange>();
  private List<AddColumnChange> deferredDefaults = new ArrayList<AddColumnChange>();

  /**
   * Adds all tables from the other database to this database. Note that the other database is not
   * changed.
   * 
   * @param otherDb
   *          The other database model
   */
  public void mergeWith(Database otherDb) throws ModelException {
    for (Iterator it = otherDb._tables.iterator(); it.hasNext();) {
      Table table = (Table) it.next();

      if (findTable(table.getName()) != null) {
        // We will merge the table with the corresponding one in the
        // target model
        findTable(table.getName()).mergeWith(table);
      } else {
        try {
          addTable((Table) table.clone());
        } catch (CloneNotSupportedException ex) {
          // won't happen
        }
      }
    }

    for (Iterator it = otherDb._sequences.iterator(); it.hasNext();) {
      Sequence sequence = (Sequence) it.next();

      if (findSequence(sequence.getName()) != null) {
        // TODO: It might make more sense to log a warning and overwrite
        // the sequence (or merge them) ?
        throw new ModelException("Cannot merge the models because sequence " + sequence.getName()
            + " already defined in this model");
      }
      try {
        addSequence((Sequence) sequence.clone());
      } catch (CloneNotSupportedException ex) {
        // won't happen
      }
    }

    for (Iterator it = otherDb._views.iterator(); it.hasNext();) {
      View view = (View) it.next();

      if (findView(view.getName()) != null) {
        this.removeView(findView(view.getName()));
      }
      try {
        addView((View) view.clone());
      } catch (CloneNotSupportedException ex) {
        // won't happen
      }
    }

    // using Iterator because records may be deleted from the collection
    for (Iterator<MaterializedView> it = otherDb.materializedViews.iterator(); it.hasNext();) {
      MaterializedView materializedView = it.next();

      String materializedViewName = materializedView.getName();
      if (findMaterializedView(materializedViewName) != null) {
        removeMaterializedView(findMaterializedView(materializedViewName));
      }
      try {
        addMaterializedView((MaterializedView) materializedView.clone());
      } catch (CloneNotSupportedException ex) {
        // won't happen
      }
    }

    for (Iterator it = otherDb._functions.iterator(); it.hasNext();) {
      Function function = (Function) it.next();
      Function oldFunction = findFunction(function.getName());
      if (oldFunction != null) {
        this.removeFunction(oldFunction);
      }
      try {
        addFunction((Function) function.clone());
      } catch (CloneNotSupportedException ex) {
        // won't happen
      }
    }

    for (Iterator it = otherDb._triggers.iterator(); it.hasNext();) {
      Trigger trigger = (Trigger) it.next();
      Trigger oldTrigger = findTrigger(trigger.getName());
      if (oldTrigger != null) {
        this.removeTrigger(oldTrigger);
      }
      try {
        addTrigger((Trigger) trigger.clone());
      } catch (CloneNotSupportedException ex) {
        // won't happen
      }
    }

    for (Iterator it = otherDb._modifiedTables.iterator(); it.hasNext();) {
      Table mtable = (Table) it.next();
      this._modifiedTables.add(mtable);
    }

    Vector<Table> toRemove = new Vector<Table>();
    for (Iterator it = this._modifiedTables.iterator(); it.hasNext();) {
      Table mtable = (Table) it.next();
      Table table = findTable(mtable.getName());
      if (table != null) {
        toRemove.add(mtable);
        table.mergeWith(mtable);
      }
    }

    for (Table table : toRemove) {
      _modifiedTables.remove(table);
    }
  }

  /**
   * Returns the name of this database model.
   * 
   * @return The name
   */
  public String getName() {
    return _name;
  }

  /**
   * Sets the name of this database model.
   * 
   * @param name
   *          The name
   */
  public void setName(String name) {
    _name = name;
  }

  /**
   * Returns the version of this database model.
   * 
   * @return The version
   */
  public String getVersion() {
    return _version;
  }

  /**
   * Sets the version of this database model.
   * 
   * @param version
   *          The version
   */
  public void setVersion(String version) {
    _version = version;
  }

  /**
   * Returns the method for generating primary key values.
   * 
   * @return The method
   */
  public String getIdMethod() {
    return _idMethod;
  }

  /**
   * Sets the method for generating primary key values. Note that this value is ignored by DdlUtils
   * and only for compatibility with Torque.
   * 
   * @param idMethod
   *          The method
   */
  public void setIdMethod(String idMethod) {
    _idMethod = idMethod;
  }

  /**
   * Returns the number of tables in this model.
   * 
   * @return The number of tables
   */
  public int getTableCount() {
    return _tables.size();
  }

  public int getModifiedTableCount() {
    return _modifiedTables.size();
  }

  /**
   * Returns the tables in this model.
   * 
   * @return The tables
   */
  public Table[] getTables() {
    return (Table[]) _tables.toArray(new Table[_tables.size()]);
  }

  public Table[] getModifiedTables() {
    return (Table[]) _modifiedTables.toArray(new Table[_modifiedTables.size()]);
  }

  /**
   * Returns the table at the specified position.
   * 
   * @param idx
   *          The index of the table
   * @return The table
   */
  public Table getTable(int idx) {
    return (Table) _tables.get(idx);
  }

  public Table getModifiedTable(int idx) {
    return (Table) _modifiedTables.get(idx);
  }

  /**
   * Adds a table.
   * 
   * @param table
   *          The table to add
   */
  public void addTable(Table table) {
    if (table != null) {
      _tables.add(table);
    }
  }

  public void addModifiedTable(Table table) {
    if (table != null) {
      _modifiedTables.add(table);
    }
  }

  /**
   * Adds a table at the specified position.
   * 
   * @param idx
   *          The index where to insert the table
   * @param table
   *          The table to add
   */
  public void addTable(int idx, Table table) {
    if (table != null) {
      _tables.add(idx, table);
    }
  }

  public void addModifiedTable(int idx, Table table) {
    if (table != null) {
      _modifiedTables.add(idx, table);
    }
  }

  /**
   * Adds the given tables.
   * 
   * @param tables
   *          The tables to add
   */
  public void addTables(Collection tables) {
    for (Iterator it = tables.iterator(); it.hasNext();) {
      addTable((Table) it.next());
    }
  }

  public void addModifiedTables(Collection tables) {
    for (Iterator it = tables.iterator(); it.hasNext();) {
      addModifiedTable((Table) it.next());
    }
  }

  /**
   * Removes the given table.
   * 
   * @param table
   *          The table to remove
   */
  public void removeTable(Table table) {
    if (table != null) {
      _tables.remove(table);
    }
  }

  public void removeModifiedTable(Table table) {
    if (table != null) {
      _modifiedTables.remove(table);
    }
  }

  /**
   * Removes the indicated table.
   * 
   * @param idx
   *          The index of the table to remove
   */
  public void removeTable(int idx) {
    _tables.remove(idx);
  }

  public void removeModifiedTable(int idx) {
    _modifiedTables.remove(idx);
  }

  /**
   * Returns the number of sequences in this model.
   * 
   * @return The number of sequences
   */
  public int getSequenceCount() {
    return _sequences.size();
  }

  /**
   * Returns the sequences in this model.
   * 
   * @return The sequences
   */
  public Sequence[] getSequences() {
    return (Sequence[]) _sequences.toArray(new Sequence[_sequences.size()]);
  }

  /**
   * Returns the sequence at the specified position.
   * 
   * @param idx
   *          The index of the sequence
   * @return The sequence
   */
  public Sequence getSequence(int idx) {
    return (Sequence) _sequences.get(idx);
  }

  /**
   * Adds a sequence.
   * 
   * @param sequence
   *          The sequence to add
   */
  public void addSequence(Sequence sequence) {
    if (sequence != null) {
      _sequences.add(sequence);
    }
  }

  /**
   * Adds a sequence at the specified position.
   * 
   * @param idx
   *          The index where to insert the sequence
   * @param sequence
   *          The sequence to add
   */
  public void addSequence(int idx, Sequence sequence) {
    if (sequence != null) {
      _sequences.add(idx, sequence);
    }
  }

  /**
   * Adds the given sequences.
   * 
   * @param sequences
   *          The sequences to add
   */
  public void addSequences(Collection sequences) {
    for (Iterator it = sequences.iterator(); it.hasNext();) {
      addSequence((Sequence) it.next());
    }
  }

  /**
   * Removes the given sequence.
   * 
   * @param sequence
   *          The sequence to remove
   */
  public void removeSequence(Sequence sequence) {
    if (sequence != null) {
      _sequences.remove(sequence);
    }
  }

  /**
   * Removes the indicated sequence.
   * 
   * @param idx
   *          The index of the sequence to remove
   */
  public void removeSequence(int idx) {
    _sequences.remove(idx);
  }

  /**
   * Returns the number of views in this model.
   * 
   * @return The number of views
   */
  public int getViewCount() {
    return _views.size();
  }

  /**
   * Returns the number of materialized views in this model.
   * 
   * @return The number of materialized views
   */
  public int getMaterializedViewCount() {
    return materializedViews.size();
  }

  /**
   * Returns the views in this model.
   * 
   * @return The views
   */
  public View[] getViews() {
    return (View[]) _views.toArray(new View[_views.size()]);
  }

  /**
   * Returns the materialized views in this model.
   * 
   * @return The materialized views
   */
  public List<MaterializedView> getMaterializedViews() {
    return new ArrayList<>(materializedViews);
  }

  /**
   * Returns the view at the specified position.
   * 
   * @param idx
   *          The index of the view
   * @return The view
   */
  public View getView(int idx) {
    return (View) _views.get(idx);
  }

  /**
   * Returns the materialized view at the specified position.
   * 
   * @param idx
   *          The index of the materialized view
   * @return The materialized view
   */
  public MaterializedView getMaterializedView(int idx) {
    return (MaterializedView) materializedViews.get(idx);
  }

  /**
   * Adds a view.
   * 
   * @param view
   *          The view to add
   */
  public void addView(View view) {
    if (view != null) {
      _views.add(view);
    }
  }

  /**
   * Adds a materialized view.
   * 
   * @param materializedView
   *          The materialized view to add
   */
  public void addMaterializedView(MaterializedView materializedView) {
    if (materializedView != null) {
      materializedViews.add(materializedView);
    }
  }

  /**
   * Adds a view at the specified position.
   * 
   * @param idx
   *          The index where to insert the view
   * @param view
   *          The view to add
   */
  public void addView(int idx, View view) {
    if (view != null) {
      _views.add(idx, view);
    }
  }

  /**
   * Adds the given views.
   * 
   * @param views
   *          The views to add
   */
  public void addViews(Collection views) {
    for (Iterator it = views.iterator(); it.hasNext();) {
      addView((View) it.next());
    }
  }

  /**
   * Adds the given materialized views.
   * 
   * @param matViews
   *          The materialized views to add
   */
  public void addMaterializedViews(Collection<MaterializedView> matViews) {
    for (Iterator<MaterializedView> it = matViews.iterator(); it.hasNext();) {
      addMaterializedView(it.next());
    }
  }

  /**
   * Removes the given view.
   * 
   * @param view
   *          The view to remove
   */
  public void removeView(View view) {
    if (view != null) {
      _views.remove(view);
    }
  }

  /**
   * Removes the given view.
   * 
   * @param view
   *          The view to remove
   */
  public void removeMaterializedView(MaterializedView materializedView) {
    if (materializedView != null) {
      materializedViews.remove(materializedView);
    }
  }

  /**
   * Removes the indicated view.
   * 
   * @param idx
   *          The index of the view to remove
   */
  public void removeView(int idx) {
    _views.remove(idx);
  }

  /**
   * Returns the number of functions in this model.
   * 
   * @return The number of functions
   */
  public int getFunctionCount() {
    return _functions.size();
  }

  /**
   * Returns the functions in this model.
   * 
   * @return The functions
   */
  public Function[] getFunctions() {
    return (Function[]) _functions.toArray(new Function[_functions.size()]);
  }

  /**
   * Returns the function at the specified position.
   * 
   * @param idx
   *          The index of the function
   * @return The function
   */
  public Function getFunction(int idx) {
    return (Function) _functions.get(idx);
  }

  /**
   * Adds a function.
   * 
   * @param function
   *          The function to add
   */
  public void addFunction(Function function) {
    if (function != null) {
      _functions.add(function);
    }
  }

  /**
   * Adds a function at the specified position.
   * 
   * @param idx
   *          The index where to insert the function
   * @param function
   *          The function to add
   */
  public void addFunction(int idx, Function function) {
    if (function != null) {
      _functions.add(idx, function);
    }
  }

  /**
   * Adds the given functions.
   * 
   * @param functions
   *          The functions to add
   */
  public void addFunctions(Collection functions) {
    for (Iterator it = functions.iterator(); it.hasNext();) {
      addFunction((Function) it.next());
    }
  }

  /**
   * Removes the given function.
   * 
   * @param function
   *          The function to remove
   */
  public void removeFunction(Function function) {
    if (function != null) {
      _functions.remove(function);
    }
  }

  /**
   * Removes the indicated function.
   * 
   * @param idx
   *          The index of the function to remove
   */
  public void removeFunction(int idx) {
    _functions.remove(idx);
  }

  /**
   * Returns the number of triggers in this model.
   * 
   * @return The number of triggers
   */
  public int getTriggerCount() {
    return _triggers.size();
  }

  /**
   * Returns the triggers in this model.
   * 
   * @return The triggers
   */
  public Trigger[] getTriggers() {
    return (Trigger[]) _triggers.toArray(new Trigger[_triggers.size()]);
  }

  /**
   * Returns the trigger at the specified position.
   * 
   * @param idx
   *          The index of the trigger
   * @return The trigger
   */
  public Trigger getTrigger(int idx) {
    return (Trigger) _triggers.get(idx);
  }

  /**
   * Adds a trigger.
   * 
   * @param trigger
   *          The trigger to add
   */
  public void addTrigger(Trigger trigger) {
    if (trigger != null) {
      _triggers.add(trigger);
    }
  }

  /**
   * Adds a trigger at the specified position.
   * 
   * @param idx
   *          The index where to insert the trigger
   * @param trigger
   *          The trigger to add
   */
  public void addTrigger(int idx, Trigger trigger) {
    if (trigger != null) {
      _triggers.add(idx, trigger);
    }
  }

  /**
   * Adds the given triggers.
   * 
   * @param triggers
   *          The triggers to add
   */
  public void addTriggers(Collection triggers) {
    for (Iterator it = triggers.iterator(); it.hasNext();) {
      addTrigger((Trigger) it.next());
    }
  }

  /**
   * Removes the given trigger.
   * 
   * @param trigger
   *          The trigger to remove
   */
  public void removeTrigger(Trigger trigger) {
    if (trigger != null) {
      _triggers.remove(trigger);
    }
  }

  /**
   * Removes the indicated trigger.
   * 
   * @param idx
   *          The index of the trigger to remove
   */
  public void removeTrigger(int idx) {
    _triggers.remove(idx);
  }

  // Helper methods

  /**
   * Initializes the model by establishing the relationships between elements in this model encoded
   * eg. in foreign keys etc. Also checks that the model elements are valid (table and columns have
   * a name, foreign keys rference existing tables etc.)
   */
  public void initialize() throws ModelException {
    // we have to setup
    // * target tables in foreign keys
    // * columns in foreign key references
    // * columns in indices
    // * columns in uniques
    HashSet namesOfProcessedTables = new HashSet();
    HashSet namesOfProcessedColumns = new HashSet();
    HashSet namesOfProcessedFks = new HashSet();
    HashSet namesOfProcessedIndices = new HashSet();
    HashSet namesOfProcessedChecks = new HashSet();
    int tableIdx = 0;

    // if ((getName() == null) || (getName().length() == 0))
    // {
    // throw new ModelException("The database model has no name");
    // }

    for (Iterator tableIt = _tables.iterator(); tableIt.hasNext(); tableIdx++) {
      Table curTable = (Table) tableIt.next();

      if ((curTable.getName() == null) || (curTable.getName().length() == 0)) {
        throw new ModelException("The table nr. " + tableIdx + " has no name");
      }
      if (namesOfProcessedTables.contains(curTable.getName())) {
        throw new ModelException("There are multiple tables with the name " + curTable.getName());
      }
      namesOfProcessedTables.add(curTable.getName());

      namesOfProcessedColumns.clear();
      namesOfProcessedFks.clear();
      namesOfProcessedIndices.clear();
      namesOfProcessedChecks.clear();

      for (int idx = 0; idx < curTable.getColumnCount(); idx++) {
        Column column = curTable.getColumn(idx);

        if ((column.getName() == null) || (column.getName().length() == 0)) {
          throw new ModelException(
              "The column nr. " + idx + " in table " + curTable.getName() + " has no name");
        }
        if (namesOfProcessedColumns.contains(column.getName())) {
          throw new ModelException("There are multiple column with the name " + column.getName()
              + " in the table " + curTable.getName());
        }
        namesOfProcessedColumns.add(column.getName());

        if ((column.getType() == null) || (column.getType().length() == 0)) {
          throw new ModelException(
              "The column nr. " + idx + " in table " + curTable.getName() + " has no type");
        }
        if ((column.getTypeCode() == Types.OTHER) && !"OTHER".equalsIgnoreCase(column.getType())) {
          throw new ModelException("The column nr. " + idx + " in table " + curTable.getName()
              + " has an unknown type " + column.getType());
        }
        namesOfProcessedColumns.add(column.getName());
      }

      for (int idx = 0; idx < curTable.getForeignKeyCount(); idx++) {
        ForeignKey fk = curTable.getForeignKey(idx);
        String fkName = (fk.getName() == null ? "" : fk.getName());
        String fkDesc = (fkName.length() == 0 ? "nr. " + idx : fkName);

        if (fkName.length() > 0) {
          if (namesOfProcessedFks.contains(fkName)) {
            for (int i = 0; i < curTable.getForeignKeyCount(); i++) {
              if (curTable.getForeignKey(i).getName().equals(fkName)) {
                curTable.removeForeignKey(curTable.getForeignKey(i));
                break;
              }
            }
          }
          namesOfProcessedFks.add(fkName);
        }

        if (fk.getForeignTable() == null) {
          Table targetTable = findTable(fk.getForeignTableName(), true);

          if (targetTable == null) {
            throw new ModelException("The foreignkey " + fkDesc + " in table " + curTable.getName()
                + " references the undefined table " + fk.getForeignTableName());
          } else {
            fk.setForeignTable(targetTable);
          }
        }
        for (int refIdx = 0; refIdx < fk.getReferenceCount(); refIdx++) {
          Reference ref = fk.getReference(refIdx);

          if (ref.getLocalColumn() == null) {
            Column localColumn = curTable.findColumn(ref.getLocalColumnName(), true);

            if (localColumn == null) {
              throw new ModelException(
                  "The foreignkey " + fkDesc + " in table " + curTable.getName()
                      + " references the undefined local column " + ref.getLocalColumnName());
            } else {
              ref.setLocalColumn(localColumn);
            }
          }
          if (ref.getForeignColumn() == null) {
            Column foreignColumn = fk.getForeignTable()
                .findColumn(ref.getForeignColumnName(), true);

            if (foreignColumn == null) {
              throw new ModelException("The foreignkey " + fkDesc + " in table "
                  + curTable.getName() + " references the undefined local column "
                  + ref.getForeignColumnName() + " in table " + fk.getForeignTable().getName());
            } else {
              ref.setForeignColumn(foreignColumn);
            }
          }
        }
      }

      for (int idx = 0; idx < curTable.getIndexCount(); idx++) {
        Index index = curTable.getIndex(idx);
        String indexName = (index.getName() == null ? "" : index.getName());
        String indexDesc = (indexName.length() == 0 ? "nr. " + idx : indexName);

        if (indexName.length() > 0) {
          if (namesOfProcessedIndices.contains(indexName)) {
            throw new ModelException("There are multiple indices in table " + curTable.getName()
                + " with the name " + indexName);
          }
          namesOfProcessedIndices.add(indexName);
        }

        for (int indexColumnIdx = 0; indexColumnIdx < index.getColumnCount(); indexColumnIdx++) {
          IndexColumn indexColumn = index.getColumn(indexColumnIdx);
          Column column = curTable.findColumn(indexColumn.getName(), true);

          if (column == null) {
            // if the index column is based on a function there is no need to check that there is a
            // column in the database with same name
            // the index will be exported properly even if a column is not assigned to the
            // indexColumn
            if (!"functionBasedColumn".equals(indexColumn.getName())) {
              throw new ModelException("The index " + indexDesc + " in table " + curTable.getName()
                  + " references the undefined column " + indexColumn.getName());
            }
          } else {
            indexColumn.setColumn(column);
          }
        }
      }

      for (int idx = 0; idx < curTable.getUniqueCount(); idx++) {
        Unique unique = curTable.getUnique(idx);
        String uniqueName = (unique.getName() == null ? "" : unique.getName());
        String uniqueDesc = (uniqueName.length() == 0 ? "nr. " + idx : uniqueName);

        if (uniqueName.length() > 0) {
          if (namesOfProcessedIndices.contains(uniqueName)) {
            throw new ModelException("There are multiple uniques or indices in table "
                + curTable.getName() + " with the name " + uniqueName);
          }
          namesOfProcessedIndices.add(uniqueName);
        }

        for (int uniqueColumnIdx = 0; uniqueColumnIdx < unique
            .getColumnCount(); uniqueColumnIdx++) {
          IndexColumn uniqueColumn = unique.getColumn(uniqueColumnIdx);
          Column column = curTable.findColumn(uniqueColumn.getName(), true);

          if (column == null) {
            throw new ModelException("The unique " + uniqueDesc + " in table " + curTable.getName()
                + " references the undefined column " + uniqueColumn.getName());
          } else {
            uniqueColumn.setColumn(column);
          }
        }
      }

      for (int idx = 0; idx < curTable.getCheckCount(); idx++) {

        Check check = curTable.getCheck(idx);

        if ((check.getName() == null) || (check.getName().length() == 0)) {
          throw new ModelException(
              "The check nr. " + idx + " in table " + curTable.getName() + " has no name");
        }
        if (namesOfProcessedChecks.contains(check.getName())) {
          throw new ModelException("There are multiple checks with the name " + check.getName()
              + " in the table " + curTable.getName());
        }
        namesOfProcessedChecks.add(check.getName());

        if ((check.getCondition() == null) || (check.getCondition().length() == 0)) {
          throw new ModelException("The check " + check.getName() + " in table "
              + curTable.getName() + " has no condition defined");
        }
      }
    }

    HashSet namesOfProcessedSequences = new HashSet();
    int sequenceIdx = 0;

    for (Iterator sequenceIt = _sequences.iterator(); sequenceIt.hasNext();) {
      Sequence curSequence = (Sequence) sequenceIt.next();

      if ((curSequence.getName() == null) || (curSequence.getName().length() == 0)) {
        throw new ModelException("The sequence nr. " + sequenceIdx + "has no name");
      }
      if (namesOfProcessedSequences.contains(curSequence.getName())) {
        throw new ModelException(
            "There are multiple sequences with the name " + curSequence.getName());
      }
      namesOfProcessedSequences.add(curSequence.getName());
    }

    HashSet namesOfProcessedViews = new HashSet();
    int viewIdx = 0;

    for (Iterator viewIt = _views.iterator(); viewIt.hasNext();) {
      View curView = (View) viewIt.next();

      if ((curView.getName() == null) || (curView.getName().length() == 0)) {
        throw new ModelException("The view nr. " + viewIdx + "has no name");
      }
      if (namesOfProcessedViews.contains(curView.getName())) {
        throw new ModelException("There are multiple views with the name " + curView.getName());
      }
      namesOfProcessedViews.add(curView.getName());

      if ((curView.getStatement() == null) || (curView.getStatement().length() == 0)) {
        throw new ModelException("The view " + curView.getName() + " has no statement defined");
      }
    }

    HashSet namesOfProcessedFunctions = new HashSet();
    HashSet namesOfProcessedParameters = new HashSet();
    int functionIdx = 0;

    for (Iterator functionIt = _functions.iterator(); functionIt.hasNext();) {
      Function curFunction = (Function) functionIt.next();

      if ((curFunction.getName() == null) || (curFunction.getName().length() == 0)) {
        throw new ModelException("The function nr. " + functionIdx + "has no name");
      }
      if (namesOfProcessedFunctions.contains(curFunction.getNotation())) {
        throw new ModelException(
            "There are multiple functions with the same notation " + curFunction.getNotation());
      }
      namesOfProcessedFunctions.add(curFunction.getNotation());

      if ((curFunction.getBody() == null) || (curFunction.getBody().length() == 0)) {
        throw new ModelException("The function " + curFunction.getName() + " has no body defined");
      }

      namesOfProcessedParameters.clear();
      for (int idx = 0; idx < curFunction.getParameterCount(); idx++) {
        Parameter curParameter = curFunction.getParameter(idx);

        try {
          if ((curParameter.getName() != null) && (curParameter.getName().length() == 0)) {
            if (namesOfProcessedParameters.contains(curParameter.getName())) {
              throw new ModelException("There are multiple parameters with the name "
                  + curParameter.getName() + " in the function " + curFunction.getName());
            }
          }
        } catch (Exception e) {
          throw new ModelException(
              "There was a problem reading the parameters of function " + curFunction.getName()
                  + ". Check that all the parameters have a valid name and data type.");
        }
      }
    }

    HashSet namesOfProcessedTriggers = new HashSet();
    int triggerIdx = 0;

    for (Iterator triggerIt = _triggers.iterator(); triggerIt.hasNext();) {
      Trigger curTrigger = (Trigger) triggerIt.next();

      if ((curTrigger.getName() == null) || (curTrigger.getName().length() == 0)) {
        throw new ModelException("The trigger nr. " + triggerIdx + "has no name");
      }
      if (namesOfProcessedTriggers.contains(curTrigger.getName())) {
        throw new ModelException(
            "There are multiple triggers with the name " + curTrigger.getName());
      }
      namesOfProcessedTriggers.add(curTrigger.getName());

      if (!namesOfProcessedTables.contains(curTrigger.getTable())) {
        throw new ModelException("The trigger " + curTrigger.getName()
            + " references the undefined table " + curTrigger.getTable());
      }

      if ((curTrigger.getBody() == null) || (curTrigger.getBody().length() == 0)) {
        throw new ModelException("The trigger " + curTrigger.getName() + " has no body defined");
      }
    }
  }

  /**
   * Finds the table with the specified name, using case insensitive matching. Note that this method
   * is not called getTable to avoid introspection problems.
   * 
   * @param name
   *          The name of the table to find
   * @return The table or <code>null</code> if there is no such table
   */
  public Table findTable(String name) {
    return findTable(name, false);
  }

  /**
   * Finds the table with the specified name, using case insensitive matching. Note that this method
   * is not called getTable) to avoid introspection problems.
   * 
   * @param name
   *          The name of the table to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The table or <code>null</code> if there is no such table
   */
  public Table findTable(String name, boolean caseSensitive) {
    for (Iterator iter = _tables.iterator(); iter.hasNext();) {
      Table table = (Table) iter.next();

      if (caseSensitive) {
        if (table.getName().equals(name)) {
          return table;
        }
      } else {
        if (table.getName().equalsIgnoreCase(name)) {
          return table;
        }
      }
    }
    return null;
  }

  /**
   * Finds the sequence with the specified name, using case insensitive matching. Note that this
   * method is not called getSequence to avoid introspection problems.
   * 
   * @param name
   *          The name of the sequence to find
   * @return The sequence or <code>null</code> if there is no such sequence
   */
  public Sequence findSequence(String name) {
    return findSequence(name, false);
  }

  /**
   * Finds the sequence with the specified name, using case insensitive matching. Note that this
   * method is not called getSequence) to avoid introspection problems.
   * 
   * @param name
   *          The name of the sequence to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The sequence or <code>null</code> if there is no such sequence
   */
  public Sequence findSequence(String name, boolean caseSensitive) {
    for (Iterator iter = _sequences.iterator(); iter.hasNext();) {
      Sequence sequence = (Sequence) iter.next();

      if (caseSensitive) {
        if (sequence.getName().equals(name)) {
          return sequence;
        }
      } else {
        if (sequence.getName().equalsIgnoreCase(name)) {
          return sequence;
        }
      }
    }
    return null;
  }

  /**
   * Finds the view with the specified name, using case insensitive matching. Note that this method
   * is not called getView to avoid introspection problems.
   * 
   * @param name
   *          The name of the view to find
   * @return The view or <code>null</code> if there is no such view
   */
  public View findView(String name) {
    return findView(name, false);
  }

  /**
   * Finds the materialized view with the specified name, using case insensitive matching. Note that
   * this method is not called getMaterializedView to avoid introspection problems.
   * 
   * @param name
   *          The name of the materialized view to find
   * @return The materialized view or <code>null</code> if there is no such materialized view
   */
  public MaterializedView findMaterializedView(String name) {
    return findMaterializedView(name, false);
  }

  /**
   * Finds the view with the specified name, using case insensitive matching. Note that this method
   * is not called getView) to avoid introspection problems.
   * 
   * @param name
   *          The name of the view to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The view or <code>null</code> if there is no such view
   */
  public View findView(String name, boolean caseSensitive) {
    for (Iterator iter = _views.iterator(); iter.hasNext();) {
      View view = (View) iter.next();

      if (caseSensitive) {
        if (view.getName().equals(name)) {
          return view;
        }
      } else {
        if (view.getName().equalsIgnoreCase(name)) {
          return view;
        }
      }
    }
    return null;
  }

  /**
   * Finds the materialized view with the specified name, using case insensitive matching. Note that
   * this method is not called getMaterializedView to avoid introspection problems.
   * 
   * @param name
   *          The name of the materialized view to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The materialized view or <code>null</code> if there is no such materialized view
   */
  public MaterializedView findMaterializedView(String name, boolean caseSensitive) {
    Predicate<String> filterFunction = s -> caseSensitive ? s.equals(name)
        : s.equalsIgnoreCase(name);
    return materializedViews.stream()
        .filter(v -> filterFunction.test(v.getName()))
        .findAny()
        .orElse(null);
  }

  /**
   * Finds the function with the specified name, using case insensitive matching. Note that this
   * method is not called getFunction to avoid introspection problems.
   * 
   * @param name
   *          The name of the function to find
   * @return The function or <code>null</code> if there is no such function
   */
  public Function findFunction(String name) {
    return findFunction(name, false);
  }

  /**
   * Finds the function with the specified name and parameters, using case sensitive matching.
   * 
   * @param name
   *          The name of the function to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The function or <code>null</code> if there is no such function
   */
  public Function findFunctionWithParams(String name, Parameter[] params) {
    return findFunctionWithParams(name, params, false);
  }

  /**
   * Finds the function with the specified name, using case sensitive matching. Note that this
   * method is not called getFunction) to avoid introspection problems.
   * 
   * @param name
   *          The name of the function to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The function or <code>null</code> if there is no such function
   */
  public Function findFunction(String name, boolean caseSensitive) {
    for (Iterator iter = _functions.iterator(); iter.hasNext();) {
      Function function = (Function) iter.next();

      if (caseSensitive) {
        if (function.getName().equals(name)) {
          return function;
        }
      } else {
        if (function.getName().equalsIgnoreCase(name)) {
          return function;
        }
      }
    }
    return null;
  }

  /**
   * Finds the function with the specified name and parameters, using case insensitive matching.
   * 
   * @param name
   *          The name of the function to find
   * @param params
   *          The parameters of the function to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The function or <code>null</code> if there is no such function
   */
  public Function findFunctionWithParams(String name, Parameter[] params, boolean caseSensitive) {
    for (Iterator iter = _functions.iterator(); iter.hasNext();) {
      Function function = (Function) iter.next();

      if (caseSensitive) {
        if (function.getName().equals(name)) {
          if (function.getParameterCount() == params.length) {
            boolean eqPara = true;
            int i = 0;
            Parameter[] params2 = function.getParameters();
            while (eqPara && i < params.length) {
              if (!params[i].equals(params2[i])) {
                eqPara = false;
              }
              i++;
            }
            if (eqPara) {
              return function;
            }
          }
        }
      } else {
        if (function.getName().equalsIgnoreCase(name)) {
          Parameter[] params2 = function.getParameters();
          if (params2.length == params.length) {
            boolean eqPara = true;
            int i = 0;
            while (eqPara && i < params.length) {
              int type1 = params[i].getTypeCode();
              int type2 = params2[i].getTypeCode();

              int typeCode2 = type1 == ExtTypes.NVARCHAR ? Types.VARCHAR : type1;
              typeCode2 = typeCode2 == ExtTypes.NCHAR ? Types.CHAR : typeCode2;
              int othertypeCode2 = type2 == ExtTypes.NVARCHAR ? Types.VARCHAR : type2;
              othertypeCode2 = othertypeCode2 == ExtTypes.NCHAR ? Types.CHAR : othertypeCode2;
              if (typeCode2 != othertypeCode2) {
                eqPara = false;
              }
              i++;
            }
            if (eqPara) {
              return function;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Finds the trigger with the specified name, using case insensitive matching. Note that this
   * method is not called getTrigger to avoid introspection problems.
   * 
   * @param name
   *          The name of the trigger to find
   * @return The trigger or <code>null</code> if there is no such trigger
   */
  public Trigger findTrigger(String name) {
    return findTrigger(name, false);
  }

  /**
   * Finds the trigger with the specified name, using case insensitive matching. Note that this
   * method is not called getTrigger) to avoid introspection problems.
   * 
   * @param name
   *          The name of the trigger to find
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The trigger or <code>null</code> if there is no such trigger
   */
  public Trigger findTrigger(String name, boolean caseSensitive) {
    for (Iterator iter = _triggers.iterator(); iter.hasNext();) {
      Trigger trigger = (Trigger) iter.next();

      if (caseSensitive) {
        if (trigger.getName().equals(name)) {
          return trigger;
        }
      } else {
        if (trigger.getName().equalsIgnoreCase(name)) {
          return trigger;
        }
      }
    }
    return null;
  }

  /**
   * Returns the dyna class cache. If none is available yet, a new one will be created.
   * 
   * @return The dyna class cache
   */
  private DynaClassCache getDynaClassCache() {
    if (_dynaClassCache == null) {
      _dynaClassCache = new DynaClassCache();
    }
    return _dynaClassCache;
  }

  /**
   * Resets the dyna class cache. This should be done for instance when a column has been added or
   * removed to a table.
   */
  public void resetDynaClassCache() {
    _dynaClassCache = null;
  }

  /**
   * Returns the {@link org.apache.ddlutils.dynabean.SqlDynaClass} for the given table name. If the
   * it does not exist yet, a new one will be created based on the Table definition.
   * 
   * @param tableName
   *          The name of the table to create the bean for
   * @return The <code>SqlDynaClass</code> for the indicated table or <code>null</code> if the model
   *         contains no such table
   */
  public SqlDynaClass getDynaClassFor(String tableName) {
    Table table = findTable(tableName);

    return table != null ? getDynaClassCache().getDynaClass(table) : null;
  }

  /**
   * Returns the {@link org.apache.ddlutils.dynabean.SqlDynaClass} for the given dyna bean.
   * 
   * @param bean
   *          The dyna bean
   * @return The <code>SqlDynaClass</code> for the given bean
   */
  public SqlDynaClass getDynaClassFor(DynaBean bean) {
    return getDynaClassCache().getDynaClass(bean);
  }

  /**
   * Creates a new dyna bean for the given table.
   * 
   * @param table
   *          The table to create the bean for
   * @return The new dyna bean
   */
  public DynaBean createDynaBeanFor(Table table) throws SqlDynaException {
    return getDynaClassCache().createNewInstance(table);
  }

  /**
   * Convenience method that combines {@link #createDynaBeanFor(Table)} and
   * {@link #findTable(String, boolean)}.
   * 
   * @param tableName
   *          The name of the table to create the bean for
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The new dyna bean
   */
  public DynaBean createDynaBeanFor(String tableName, boolean caseSensitive)
      throws SqlDynaException {
    return getDynaClassCache().createNewInstance(findTable(tableName, caseSensitive));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    Database result = (Database) super.clone();

    result._name = _name;
    result._idMethod = _idMethod;
    result._version = _version;
    result._tables = new ArrayList();
    result._views = new ArrayList();
    result.materializedViews = new ArrayList();
    result._functions = new ArrayList();
    result._triggers = new ArrayList();
    result._sequences = new ArrayList();
    result._modifiedTables = new ArrayList();
    Iterator it = _tables.iterator();
    while (it.hasNext()) {
      result._tables.add(((Table) it.next()).clone());
    }
    it = _views.iterator();
    while (it.hasNext()) {
      result._views.add(((View) it.next()).clone());
    }
    for (Iterator<MaterializedView> matViewIt = materializedViews.iterator(); matViewIt
        .hasNext();) {
      result.materializedViews.add((MaterializedView) (matViewIt.next()).clone());
    }

    it = _functions.iterator();
    while (it.hasNext()) {
      result._functions.add(((Function) it.next()).clone());
    }
    it = _triggers.iterator();
    while (it.hasNext()) {
      result._triggers.add(((Trigger) it.next()).clone());
    }
    it = _sequences.iterator();
    while (it.hasNext()) {
      result._sequences.add(((Sequence) it.next()).clone());
    }
    it = _modifiedTables.iterator();
    while (it.hasNext()) {
      result._modifiedTables.add(((Table) it.next()).clone());
    }

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Database) {
      Database other = (Database) obj;

      // Note that this compares case sensitive
      return new EqualsBuilder().append(_name, other._name)
          .append(_tables, other._tables)
          .append(_views, other._views)
          .append(_functions, other._functions)
          .append(_triggers, other._triggers)
          .isEquals();
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(_name)
        .append(_tables)
        .append(_views)
        .append(_functions)
        .append(_triggers)
        .toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();

    result.append("Database [name=");
    result.append(getName());
    result.append("; ");
    result.append(getTableCount());
    result.append(" tables");
    result.append("; ");
    result.append(getSequenceCount());
    result.append(" sequences");
    result.append("; ");
    result.append(getViewCount());
    result.append(" views");
    result.append("; ");
    result.append(getMaterializedViewCount());
    result.append(" materialized views");
    result.append("; ");
    result.append(getFunctionCount());
    result.append(" functions");
    result.append("; ");
    result.append(getTriggerCount());
    result.append(" triggers]");

    return result.toString();
  }

  /**
   * Returns a verbose string representation of this database.
   * 
   * @return The string representation
   */
  public String toVerboseString() {
    StringBuffer result = new StringBuffer();

    result.append("Database [");
    result.append(getName());
    result.append("] tables:");
    for (int idx = 0; idx < getTableCount(); idx++) {
      result.append(" ");
      result.append(getTable(idx).toVerboseString());
    }
    result.append(" sequences:");
    for (int idx = 0; idx < getSequenceCount(); idx++) {
      result.append(" ");
      result.append(getSequence(idx).toString());
    }
    result.append(" views:");
    for (int idx = 0; idx < getViewCount(); idx++) {
      result.append(" ");
      result.append(getView(idx).toVerboseString());
    }
    result.append(" materialized views:");
    for (int idx = 0; idx < getMaterializedViewCount(); idx++) {
      result.append(" ");
      result.append(getMaterializedView(idx).toVerboseString());
    }
    result.append(" functions:");
    for (int idx = 0; idx < getFunctionCount(); idx++) {
      result.append(" ");
      result.append(getFunction(idx).toString());
    }
    result.append(" triggers:");
    for (int idx = 0; idx < getTriggerCount(); idx++) {
      result.append(" ");
      result.append(getTrigger(idx).toString());
    }
    return result.toString();
  }

  public void applyNamingConventionFilter(ExcludeFilter filter) {
    for (int i = 0; i < _tables.size(); i++) {
      Table t = (Table) _tables.get(i);
      t.applyNamingConventionFilter(filter);
      if (t.isEmpty()) {
        _tables.remove(t);
        i--;
      }
    }

    // We will now move incomplete tables to their specific arraylist
    for (int i = 0; i < _tables.size(); i++) {
      Table t = (Table) _tables.get(i);
      if (!filter.compliesWithNamingRuleObject(t.getName())) {
        _tables.remove(t);
        _modifiedTables.add(t);
        i--;
      }
    }

    applyNamingConventionToRest(filter);
  }

  public void applyNamingConventionToUpdate(ExcludeFilter filter) {
    try {
      for (int i = 0; i < _tables.size(); i++) {
        Table t = (Table) _tables.get(i);
        Table clonedT = (Table) t.clone();
        clonedT.applyNamingConventionFilter(filter);
        if (clonedT.isEmpty()) {
          _tables.remove(t);
          i--;
        }
      }
      applyNamingConventionToRest(filter);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void applyNamingConventionToRest(ExcludeFilter filter) {

    for (int i = 0; i < _sequences.size(); i++) {
      Sequence v = (Sequence) _sequences.get(i);
      if (!filter.compliesWithNamingRuleObject(v.getName())) {
        _sequences.remove(v);
        i--;
      }
    }
    for (int i = 0; i < _views.size(); i++) {
      View v = (View) _views.get(i);
      if (!filter.compliesWithNamingRuleObject(v.getName())) {
        _views.remove(v);
        i--;
      }
    }

    // using Iterator because records may be deleted from the collection
    for (Iterator<MaterializedView> it = materializedViews.iterator(); it.hasNext();) {
      MaterializedView mv = it.next();
      if (!filter.compliesWithNamingRuleObject(mv.getName())) {
        it.remove();
      }
    }
    for (int i = 0; i < _functions.size(); i++) {
      Function v = (Function) _functions.get(i);
      if (!filter.compliesWithNamingRuleObject(v.getName())) {
        _functions.remove(v);
        i--;
      }
    }
    for (int i = 0; i < _triggers.size(); i++) {
      Trigger v = (Trigger) _triggers.get(i);
      if (!filter.compliesWithNamingRuleObject(v.getName())) {
        _triggers.remove(v);
        i--;
      }
    }
  }

  public void moveTablesToModified() {
    _modifiedTables.addAll(_tables);
    _tables = new ArrayList();
  }

  public void moveModifiedToTables() {
    _tables.addAll(_modifiedTables);
    _modifiedTables = new ArrayList();
  }

  public void removeAllTables() {
    _tables = new ArrayList();
  }

  public void filterByDataset(OBDataset dataset) {
    ArrayList oldTables = _tables;
    _tables = new ArrayList();
    Vector<OBDatasetTable> tables = dataset.getTableList();
    for (OBDatasetTable table : tables) {
      for (int i = 0; i < oldTables.size(); i++) {
        Table t = (Table) oldTables.get(i);
        if (t.getName().equalsIgnoreCase(table.getName())) {
          _tables.add(t);
        }
      }
    }
    _views = new ArrayList();
    ArrayList oldTriggers = _triggers;
    _triggers = new ArrayList();
    for (int i = 0; i < oldTriggers.size(); i++) {
      Trigger trigger = (Trigger) oldTriggers.get(i);
      if (findTable(trigger.getTable()) != null) {
        _triggers.add(trigger);
      }
    }
    // _functions=new ArrayList();
    _sequences = new ArrayList();
    _modifiedTables = new ArrayList();
  }

  public void checkDataTypes() throws SQLException {
    for (int i = 0; i < this.getTableCount(); i++) {
      Table t = this.getTable(i);
      for (int j = 0; j < t.getColumnCount(); j++) {
        Column c = t.getColumn(j);
        if (c.getTypeCode() == Types.OTHER || c.getSizeAsInt() < 0
            || (c.getSizeAsInt() == 0 && c.getTypeCode() == Types.VARCHAR)) {
          throw new SQLException(
              "Unsupported column data type for column " + c.getName() + " in table " + t.getName()
                  + ". You need to change the type of the column to a supported one to continue.");
        }
      }
    }
  }

  public void addDeferredNotNull(ColumnChange change) {
    deferredNotNulls.add(change);
  }

  public void addDeferredDefault(AddColumnChange change) {
    deferredDefaults.add(change);
  }

  public List<ColumnChange> getDeferedNotNulls() {
    return deferredNotNulls;
  }

  /** Returns whether a not null constraint is deferred for a column */
  public boolean isDeferredNotNull(Table table, Column column) {
    final String tableName = table.getName();
    final String columnName = column.getName();
    for (ColumnChange deferred : getDeferedNotNulls()) {
      if (tableName.equals(deferred.getChangedTable().getName())
          && columnName.equals(deferred.getChangedColumn().getName())) {
        return true;
      }
    }
    return false;
  }

  public List<AddColumnChange> getDeferredDefaults() {
    return deferredDefaults;
  }

  public void addNewColumnChange(AddColumnChange change) {
    addedColumns.add(change);
  }

  public boolean isNewColumn(Table table, Column column) {
    for (AddColumnChange newCol : addedColumns) {
      if (newCol.getChangedTable().getName().equalsIgnoreCase(table.getName())
          && newCol.getNewColumn().getName().equalsIgnoreCase(column.getName())) {
        return true;
      }
    }
    return false;
  }

  public boolean isDeferredDefault(Table table, Column column) {
    for (AddColumnChange newCol : deferredDefaults) {
      if (newCol.getChangedTable().getName().equalsIgnoreCase(table.getName())
          && newCol.getNewColumn().getName().equalsIgnoreCase(column.getName())) {
        return true;
      }
    }
    return false;
  }
}
