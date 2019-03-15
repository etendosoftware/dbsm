package org.apache.ddlutils.platform;

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

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.rmi.server.UID;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.collections.Closure;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.alteration.AddCheckChange;
import org.apache.ddlutils.alteration.AddColumnChange;
import org.apache.ddlutils.alteration.AddForeignKeyChange;
import org.apache.ddlutils.alteration.AddFunctionChange;
import org.apache.ddlutils.alteration.AddIndexChange;
import org.apache.ddlutils.alteration.AddPrimaryKeyChange;
import org.apache.ddlutils.alteration.AddRowChange;
import org.apache.ddlutils.alteration.AddSequenceChange;
import org.apache.ddlutils.alteration.AddTableChange;
import org.apache.ddlutils.alteration.AddTriggerChange;
import org.apache.ddlutils.alteration.AddUniqueChange;
import org.apache.ddlutils.alteration.AddViewChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnAutoIncrementChange;
import org.apache.ddlutils.alteration.ColumnChange;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.ColumnDataTypeChange;
import org.apache.ddlutils.alteration.ColumnDefaultValueChange;
import org.apache.ddlutils.alteration.ColumnOnCreateDefaultValueChange;
import org.apache.ddlutils.alteration.ColumnOrderChange;
import org.apache.ddlutils.alteration.ColumnRequiredChange;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.alteration.ContainsSearchIndexInformationChange;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.ModelComparator;
import org.apache.ddlutils.alteration.PartialIndexInformationChange;
import org.apache.ddlutils.alteration.PrimaryKeyChange;
import org.apache.ddlutils.alteration.RemoveCheckChange;
import org.apache.ddlutils.alteration.RemoveColumnChange;
import org.apache.ddlutils.alteration.RemoveForeignKeyChange;
import org.apache.ddlutils.alteration.RemoveFunctionChange;
import org.apache.ddlutils.alteration.RemoveIndexChange;
import org.apache.ddlutils.alteration.RemovePrimaryKeyChange;
import org.apache.ddlutils.alteration.RemoveRowChange;
import org.apache.ddlutils.alteration.RemoveSequenceChange;
import org.apache.ddlutils.alteration.RemoveTableChange;
import org.apache.ddlutils.alteration.RemoveTriggerChange;
import org.apache.ddlutils.alteration.RemoveUniqueChange;
import org.apache.ddlutils.alteration.RemoveViewChange;
import org.apache.ddlutils.alteration.SequenceDefinitionChange;
import org.apache.ddlutils.alteration.TableChange;
import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.ConstraintObject;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.ModelException;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.model.Reference;
import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.model.Unique;
import org.apache.ddlutils.model.ValueObject;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.translation.CommentFilter;
import org.apache.ddlutils.translation.LiteralFilter;
import org.apache.ddlutils.translation.NullTranslation;
import org.apache.ddlutils.translation.Translation;
import org.apache.ddlutils.util.CallbackClosure;
import org.apache.ddlutils.util.MultiInstanceofPredicate;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
 * This class is a collection of Strategy methods for creating the DDL required to create and drop
 * databases and tables.
 * 
 * It is hoped that just a single implementation of this class, for each database should make
 * creating DDL for each physical database fairly straightforward.
 * 
 * An implementation of this class can always delegate down to some templating technology such as
 * Velocity if it requires. Though often that can be quite complex when attempting to reuse code
 * across many databases. Hopefully only a small amount code needs to be changed on a per database
 * basis.
 * 
 * @version $Revision: 518498 $
 */
public abstract class SqlBuilder {
  /** The line separator for in between sql commands. */
  private static final String LINE_SEPARATOR = "\n";
  /** The placeholder for the size value in the native type spec. */
  protected static final String SIZE_PLACEHOLDER = "{0}";

  /** The Log to which logging calls will be made. */
  protected final Log _log = LogFactory.getLog(SqlBuilder.class);

  /** The platform that this builder belongs to. */
  private Platform _platform;
  /** The current Writer used to output the SQL to. */
  private Writer _writer;
  /** The indentation used to indent commands. */
  private String _indent = "    ";
  /** An optional locale specification for number and date formatting. */
  private String _valueLocale;
  /** The date formatter. */
  private DateFormat _valueDateFormat;
  /** The date time formatter. */
  private DateFormat _valueTimeFormat;
  /** The number formatter. */
  private NumberFormat _valueNumberFormat;
  /** Helper object for dealing with default values. */
  private DefaultValueHelper _defaultValueHelper = new DefaultValueHelper();
  /** The character sequences that need escaping. */
  private Map _charSequencesToEscape = new ListOrderedMap();
  /** The map of the tables with its list of indexes that have index columns with operator class */
  private Map<String, List<Index>> _removedIndexesWithOperatorClassMap;
  /** The map of the tables with its list of partial indexes */
  private Map<String, List<Index>> _removedIndexesWithWhereClause;

  private Translation _PLSQLFunctionTranslation = new NullTranslation();
  public Translation _PLSQLTriggerTranslation = new NullTranslation();
  private Translation _SQLTranslation = new NullTranslation();
  private boolean script = false;
  protected ArrayList<String> recreatedTables = new ArrayList<String>();
  protected ArrayList<String> createdTables = new ArrayList<String>();
  protected ArrayList<String> recreatedTablesTwice = new ArrayList<String>();
  protected ArrayList<String> recreatedFKs = new ArrayList<String>();
  protected ArrayList<String> recreatedPKs = new ArrayList<String>();
  private List<String> droppedFKs = new ArrayList<String>();

  private String forcedRecreation = "";

  //
  // Configuration
  //

  /**
   * Creates a new sql builder.
   * 
   * @param platform
   *          The plaftform this builder belongs to
   */
  public SqlBuilder(Platform platform) {
    _platform = platform;
  }

  /**
   * Returns the platform object.
   * 
   * @return The platform
   */
  public Platform getPlatform() {
    return _platform;
  }

  /**
   * Returns the platform info object.
   * 
   * @return The info object
   */
  public PlatformInfo getPlatformInfo() {
    return _platform.getPlatformInfo();
  }

  /**
   * Returns the writer that the DDL is printed to.
   * 
   * @return The writer
   */
  public Writer getWriter() {
    return _writer;
  }

  /**
   * Sets the writer for printing the DDL to.
   * 
   * @param writer
   *          The writer
   */
  public void setWriter(Writer writer) {
    _writer = writer;
  }

  public void setScript(boolean script) {
    this.script = script;
  }

  /**
   * Returns the default value helper.
   * 
   * @return The default value helper
   */
  public DefaultValueHelper getDefaultValueHelper() {
    return _defaultValueHelper;
  }

  /**
   * Returns the string used to indent the SQL.
   * 
   * @return The indentation string
   */
  public String getIndent() {
    return _indent;
  }

  /**
   * Sets the string used to indent the SQL.
   * 
   * @param indent
   *          The indentation string
   */
  public void setIndent(String indent) {
    _indent = indent;
  }

  /**
   * Returns the locale that is used for number and date formatting (when printing default values
   * and in generates insert/update/delete statements).
   * 
   * @return The locale or <code>null</code> if default formatting is used
   */
  public String getValueLocale() {
    return _valueLocale;
  }

  /**
   * Sets the locale that is used for number and date formatting (when printing default values and
   * in generates insert/update/delete statements).
   * 
   * @param localeStr
   *          The new locale or <code>null</code> if default formatting should be used; Format is
   *          "language[_country[_variant]]"
   */
  public void setValueLocale(String localeStr) {
    if (localeStr != null) {
      int sepPos = localeStr.indexOf('_');
      String language = null;
      String country = null;
      String variant = null;

      if (sepPos > 0) {
        language = localeStr.substring(0, sepPos);
        country = localeStr.substring(sepPos + 1);
        sepPos = country.indexOf('_');
        if (sepPos > 0) {
          variant = country.substring(sepPos + 1);
          country = country.substring(0, sepPos);
        }
      } else {
        language = localeStr;
      }
      if (language != null) {
        Locale locale = null;

        if (variant != null) {
          locale = new Locale(language, country, variant);
        } else if (country != null) {
          locale = new Locale(language, country);
        } else {
          locale = new Locale(language);
        }

        _valueLocale = localeStr;
        setValueDateFormat(DateFormat.getDateInstance(DateFormat.SHORT, locale));
        setValueTimeFormat(DateFormat.getTimeInstance(DateFormat.SHORT, locale));
        setValueNumberFormat(NumberFormat.getNumberInstance(locale));
        return;
      }
    }
    _valueLocale = null;
    setValueDateFormat(null);
    setValueTimeFormat(null);
    setValueNumberFormat(null);
  }

  /**
   * Returns the format object for formatting dates in the specified locale.
   * 
   * @return The date format object or null if no locale is set
   */
  protected DateFormat getValueDateFormat() {
    return _valueDateFormat;
  }

  /**
   * Sets the format object for formatting dates in the specified locale.
   * 
   * @param format
   *          The date format object
   */
  protected void setValueDateFormat(DateFormat format) {
    _valueDateFormat = format;
  }

  /**
   * Returns the format object for formatting times in the specified locale.
   * 
   * @return The time format object or null if no locale is set
   */
  protected DateFormat getValueTimeFormat() {
    return _valueTimeFormat;
  }

  /**
   * Sets the date format object for formatting times in the specified locale.
   * 
   * @param format
   *          The time format object
   */
  protected void setValueTimeFormat(DateFormat format) {
    _valueTimeFormat = format;
  }

  /**
   * Returns the format object for formatting numbers in the specified locale.
   * 
   * @return The number format object or null if no locale is set
   */
  protected NumberFormat getValueNumberFormat() {
    return _valueNumberFormat;
  }

  /**
   * Returns a new date format object for formatting numbers in the specified locale. Platforms can
   * override this if necessary.
   * 
   * @param format
   *          The number format object
   */
  protected void setValueNumberFormat(NumberFormat format) {
    _valueNumberFormat = format;
  }

  /**
   * Adds a char sequence that needs escaping, and its escaped version.
   * 
   * @param charSequence
   *          The char sequence
   * @param escapedVersion
   *          The escaped version
   */
  protected void addEscapedCharSequence(String charSequence, String escapedVersion) {
    _charSequencesToEscape.put(charSequence, escapedVersion);
  }

  /**
   * Returns the maximum number of characters that a table name can have. This method is intended to
   * give platform specific builder implementations more control over the maximum length.
   * 
   * @return The number of characters, or -1 if not limited
   */
  public int getMaxTableNameLength() {
    return getPlatformInfo().getMaxTableNameLength();
  }

  /**
   * Returns the maximum number of characters that a column name can have. This method is intended
   * to give platform specific builder implementations more control over the maximum length.
   * 
   * @return The number of characters, or -1 if not limited
   */
  public int getMaxColumnNameLength() {
    return getPlatformInfo().getMaxColumnNameLength();
  }

  /**
   * Returns the maximum number of characters that a constraint name can have. This method is
   * intended to give platform specific builder implementations more control over the maximum
   * length.
   * 
   * @return The number of characters, or -1 if not limited
   */
  public int getMaxConstraintNameLength() {
    return getPlatformInfo().getMaxConstraintNameLength();
  }

  /**
   * Returns the maximum number of characters that a foreign key name can have. This method is
   * intended to give platform specific builder implementations more control over the maximum
   * length.
   * 
   * @return The number of characters, or -1 if not limited
   */
  public int getMaxForeignKeyNameLength() {
    return getPlatformInfo().getMaxForeignKeyNameLength();
  }

  //
  // public interface
  //

  /**
   * Outputs the DDL required to drop and (re)create all tables in the database model.
   * 
   * @param database
   *          The database model
   */
  public void createTables(Database database) throws IOException {
    createTables(database, null, true);
  }

  /**
   * Outputs the DDL required to drop (if requested) and (re)create all tables in the database
   * model.
   * 
   * @param database
   *          The database
   * @param dropTables
   *          Whether to drop tables before creating them
   */
  public void createTables(Database database, boolean dropTables) throws IOException {
    createTables(database, null, dropTables);
  }

  /**
   * Outputs the DDL required to drop (if requested) and (re)create all tables in the database
   * model.
   * 
   * @param database
   *          The database
   * @param params
   *          The parameters used in the creation
   * @param dropTables
   *          Whether to drop tables before creating them
   */

  public void initializeTranslators(Database database) {
    _PLSQLFunctionTranslation = createPLSQLFunctionTranslation(database);
    _PLSQLTriggerTranslation = createPLSQLTriggerTranslation(database);
    _SQLTranslation = createSQLTranslation(database);
  }

  public void nullTranslators(Database database) {
    _PLSQLFunctionTranslation = new NullTranslation();
    _PLSQLTriggerTranslation = new NullTranslation();
    _SQLTranslation = new NullTranslation();
  }

  public void createTables(Database database, CreationParameters params, boolean dropTables)
      throws IOException {

    initializeTranslators(database);
    if (dropTables) {
      dropTables(database);
    }

    /*
     * skip adding not null constraints when creating tables as they would need to be dropped for
     * data loading again
     */
    if (params == null) {
      params = new CreationParameters();
    }
    params.addParameter(null, "OB_OptimizeNotNull", "true");

    for (int idx = 0; idx < database.getTableCount(); idx++) {
      Table table = database.getTable(idx);

      // mark this table as created to properly manage defaults and onCreateDefaults
      createdTables.add(table.getName());

      createTable(database, table, params == null ? null : params.getParametersFor(table));
      writeExternalPrimaryKeysCreateStmt(table, table.getPrimaryKey(),
          table.getPrimaryKeyColumns());
      writeExternalIndicesCreateStmt(table);
    }

    // we don't write any foreign keys as those will be only activated after the data loading

    // Write the sequences
    for (int idx = 0; idx < database.getSequenceCount(); idx++) {
      createSequence(database.getSequence(idx));
    }

    // Write the functions
    for (int idx = 0; idx < database.getFunctionCount(); idx++) {
      createFunction(database.getFunction(idx));
    }

    // Write the views
    for (int idx = 0; idx < database.getViewCount(); idx++) {
      createView(database.getView(idx));
    }

    // Write the triggers
    for (int idx = 0; idx < database.getTriggerCount(); idx++) {
      createTrigger(database, database.getTrigger(idx));
    }
    nullTranslators(database);

  }

  /**
   * Generates the DDL to modify an existing database so the schema matches the specified database
   * schema by using drops, modifications and additions. Database-specific implementations can
   * change aspect of this algorithm by redefining the individual methods that compromise it.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   */
  public void alterDatabase(Database currentModel, Database desiredModel, CreationParameters params)
      throws IOException {

    ModelComparator comparator = new ModelComparator(getPlatformInfo(),
        getPlatform().isDelimitedIdentifierModeOn());
    List<ModelChange> changes = comparator.compare(currentModel, desiredModel);

    alterDatabase(currentModel, desiredModel, params, changes);
  }

  /**
   * Creates statements to remove external constraints, indexes and views. This method should be
   * invoked as a previous step to processChanges, as it is in charge of preparing the database for
   * the changes. It is split in two methods because the statementes added by
   * removeExternalConstraintsIndexesAndViews need to be evaluated in a different bach, see issue
   * https://issues.openbravo.com/view.php?id=36908
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @throws IOException
   */
  public void prepareDatabaseForAlter(Database currentModel, Database desiredModel,
      CreationParameters params) throws IOException {
    ModelComparator comparator = new ModelComparator(getPlatformInfo(),
        getPlatform().isDelimitedIdentifierModeOn());
    List<ModelChange> changes = comparator.compare(currentModel, desiredModel);
    prepareDatabaseForAlter(currentModel, desiredModel, params, changes);
  }

  /**
   * Creates statements to remove external constraints, indexes and views. This method should be
   * invoked as a previous step to processChanges, as it is in charge of preparing the database for
   * the changes. It is split in two methods because the statementes added by
   * removeExternalConstraintsIndexesAndViews need to be evaluated in a different bach, see issue
   * https://issues.openbravo.com/view.php?id=36908
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param changes
   *          The changes to be applied
   * @throws IOException
   */
  public void prepareDatabaseForAlter(Database currentModel, Database desiredModel,
      CreationParameters params, List<ModelChange> changes) throws IOException {
    CallbackClosure callbackClosure = new CallbackClosure(this, "processChange",
        new Class[] { Database.class, Database.class, CreationParameters.class, null },
        new Object[] { currentModel, desiredModel, params, null });
    removeExternalConstraintsIndexesAndViews(currentModel, changes, callbackClosure);
  }

  public void alterDatabase(Database currentModel, Database desiredModel, CreationParameters params,
      List<ModelChange> changes) throws IOException {
    _PLSQLFunctionTranslation = createPLSQLFunctionTranslation(desiredModel);
    _PLSQLTriggerTranslation = createPLSQLTriggerTranslation(desiredModel);
    _SQLTranslation = createSQLTranslation(desiredModel);

    for (int i = 0; i < desiredModel.getFunctionCount(); i++) {
      desiredModel.getFunction(i).setTranslation(_PLSQLFunctionTranslation);
    }
    for (int i = 0; i < desiredModel.getTriggerCount(); i++) {
      desiredModel.getTrigger(i).setTranslation(_PLSQLTriggerTranslation);
    }
    processChanges(currentModel, desiredModel, changes, params, false);
  }

  public void alterData(Database model, Vector<Change> changes) throws IOException {
    for (Change change : changes) {
      if (change instanceof AddRowChange) {
        printAddRowChangeChange(model, (AddRowChange) change);
      } else if (change instanceof RemoveRowChange) {
        printRemoveRowChange(model, (RemoveRowChange) change);
      } else if (change instanceof ColumnDataChange) {
        printColumnDataChange(model, (ColumnDataChange) change);
      }
    }
  }

  public void deleteInvalidConstraintRows(Database model, OBDataset dataset,
      boolean onlyOnDeleteCascade) {
    Set<String> allDatasetTables = new HashSet<String>();
    if (dataset != null) {
      Vector<OBDatasetTable> datasetTables = dataset.getTableList();
      for (int i = 0; i < datasetTables.size(); i++) {
        allDatasetTables.add(datasetTables.get(i).getName());
      }
    }
    deleteInvalidConstraintRows(model, dataset, allDatasetTables, onlyOnDeleteCascade);
  }

  public void deleteInvalidConstraintRows(Database model, OBDataset dataset,
      Set<String> tablesWithRemovedRecords, boolean onlyOnDeleteCascade) {

    try {
      // We will now delete the rows in tables which have a foreign key
      // constraint
      // with "on delete cascade" whose parent has been deleted
      for (int i = 0; i < model.getTableCount(); i++) {
        Table table = model.getTable(i);
        ForeignKey[] fksTable;
        if (dataset == null
            || isDatasetTableWithRemovedRecords(table, dataset, tablesWithRemovedRecords)) {
          fksTable = table.getForeignKeys();
        } else {
          List<ForeignKey> fks = new ArrayList<ForeignKey>();
          for (int j = 0; j < table.getForeignKeyCount(); j++) {
            ForeignKey fk = table.getForeignKey(j);
            String foreignTableName = fk.getForeignTableName();
            if (dataset.getTable(foreignTableName) != null
                && (tablesWithRemovedRecords.contains(foreignTableName))) {
              fks.add(fk);
            }
          }
          fksTable = fks.toArray(new ForeignKey[0]);
        }
        for (int j = 0; j < fksTable.length; j++) {
          ForeignKey fk = fksTable[j];
          Table parentTable = fk.getForeignTable();
          if ((!onlyOnDeleteCascade
              || (fk.getOnDelete() != null && fk.getOnDelete().contains("cascade")))) {
            ArrayList<String> localColumns = new ArrayList<String>();
            for (int k = 0; k < table.getColumnCount(); k++) {
              if (fk.hasLocalColumn(table.getColumn(k))) {
                localColumns.add(table.getColumn(k).getName());
              }
            }
            ArrayList<String> foreignColumns = new ArrayList<String>();
            for (int k = 0; k < parentTable.getColumnCount(); k++) {
              if (fk.hasForeignColumn(parentTable.getColumn(k))) {
                foreignColumns.add(parentTable.getColumn(k).getName());
              }
            }
            printScriptOptions("ITERATE = TRUE");
            if (fk.getOnDelete() != null && fk.getOnDelete().contains("cascade")) {
              print("DELETE FROM " + table.getName() + " t ");
            } else {
              print("UPDATE " + table.getName() + " t SET ");
              for (int indC = 0; indC < localColumns.size(); indC++) {
                if (indC != 0) {
                  print(", ");
                }
                print(localColumns.get(indC) + "=null");
              }
            }
            print(" WHERE NOT EXISTS (SELECT 1");
            print(" FROM " + parentTable.getName());
            print(" WHERE ");
            for (int indC = 0; indC < localColumns.size(); indC++) {
              if (indC > 0) {
                print(" AND ");
              }
              print("t." + localColumns.get(indC) + "=" + parentTable.getName() + "."
                  + foreignColumns.get(indC));
            }
            print(") AND ");

            for (int indC = 0; indC < localColumns.size(); indC++) {
              if (indC > 0) {
                print(" AND ");
              }
              print("t." + localColumns.get(indC) + " IS NOT NULL");
            }
            printEndOfStatement();
          }
        }
      }
    } catch (Exception e) {
      System.out.println(e);
      System.out.println(e.getMessage());
      _log.error(e.getLocalizedMessage());
    }
  }

  private boolean isDatasetTableWithRemovedRecords(Table table, OBDataset dataset,
      Set<String> tablesWithRemovedRecords) {
    return dataset.getTable(table.getName()) != null
        && tablesWithRemovedRecords.contains(table.getName());
  }

  public List alterDatabaseRecreatePKs(Database currentModel, Database desiredModel,
      CreationParameters params) throws IOException {

    ModelComparator comparator = new ModelComparator(getPlatformInfo(),
        getPlatform().isDelimitedIdentifierModeOn());
    List changes = comparator.compare(currentModel, desiredModel);
    Predicate predicate = new MultiInstanceofPredicate(new Class[] { RemovePrimaryKeyChange.class,
        AddPrimaryKeyChange.class, PrimaryKeyChange.class, RemoveColumnChange.class,
        AddColumnChange.class, ColumnOrderChange.class, ColumnAutoIncrementChange.class,
        ColumnDefaultValueChange.class, ColumnOnCreateDefaultValueChange.class,
        ColumnRequiredChange.class, ColumnDataTypeChange.class, ColumnSizeChange.class });
    Collection tableChanges = CollectionUtils.select(changes, predicate);
    for (int i = 0; i < desiredModel.getTableCount(); i++) {
      boolean recreated = false;
      boolean newColumn = false;
      Iterator itChanges = tableChanges.iterator();
      Vector<AddColumnChange> newColumnsThisTable = new Vector<AddColumnChange>();
      Vector<TableChange> changesOfTable = new Vector<TableChange>();
      while (itChanges.hasNext()) {
        TableChange currentChange = (TableChange) itChanges.next();

        if (currentChange.getChangedTable()
            .getName()
            .equalsIgnoreCase(desiredModel.getTable(i).getName())) {
          if (currentChange instanceof AddColumnChange) {
            newColumnsThisTable.add((AddColumnChange) currentChange);
            newColumn = true;
          }
          changesOfTable.add(currentChange);
        }

      }
      recreated = willBeRecreated(desiredModel.getTable(i), changesOfTable);
      if (recreated && !recreatedPKs.contains(desiredModel.getTable(i).getName())) {
        writeExternalPrimaryKeysCreateStmt(desiredModel.getTable(i),
            desiredModel.getTable(i).getPrimaryKey(),
            desiredModel.getTable(i).getPrimaryKeyColumns());
        writeExternalIndicesCreateStmt(desiredModel.getTable(i));
        recreatedPKs.add(desiredModel.getTable(i).getName());
      }
    }
    return changes;
  }

  /** Returns a list of new indexes to be processed afterwards */
  public List<AddIndexChange> alterDatabasePostScript(Database currentModel, Database desiredModel,
      CreationParameters params, List<ModelChange> changes, Database fullModel, OBDataset ad)
      throws IOException {
    Vector<AddColumnChange> newColumns = new Vector<AddColumnChange>();
    for (ModelChange change : changes) {
      if (change instanceof AddColumnChange) {
        newColumns.add((AddColumnChange) change);
      }
    }

    // We will now create the primary keys from recreated tables

    Predicate predicate = new MultiInstanceofPredicate(new Class[] { RemovePrimaryKeyChange.class,
        AddPrimaryKeyChange.class, PrimaryKeyChange.class, RemoveColumnChange.class,
        AddColumnChange.class, ColumnOrderChange.class, ColumnAutoIncrementChange.class,
        ColumnDefaultValueChange.class, ColumnOnCreateDefaultValueChange.class,
        ColumnRequiredChange.class, ColumnDataTypeChange.class, ColumnSizeChange.class });

    List<AddIndexChange> newIndexes = new ArrayList<>();
    Collection tableChanges = CollectionUtils.select(changes, predicate);
    ArrayList<String> recreatedTables = new ArrayList<String>();
    for (int i = 0; i < desiredModel.getTableCount(); i++) {
      boolean recreated = false;
      boolean newColumn = false;
      Table currentTable = desiredModel.getTable(i);
      Iterator itChanges = tableChanges.iterator();
      Vector<AddColumnChange> newColumnsThisTable = new Vector<AddColumnChange>();
      Vector<TableChange> changesOfTable = new Vector<TableChange>();
      while (itChanges.hasNext()) {
        TableChange currentChange = (TableChange) itChanges.next();

        if (currentChange.getChangedTable().getName().equalsIgnoreCase(currentTable.getName())) {
          if (currentChange instanceof AddColumnChange) {
            newColumnsThisTable.add((AddColumnChange) currentChange);
            newColumn = true;
          }
          changesOfTable.add(currentChange);
        }

      }
      recreated = willBeRecreated(currentTable, changesOfTable);

      for (int j = 0; j < newColumns.size(); j++) {
        AddColumnChange change = newColumns.get(j);
        Table table = change.getChangedTable();
        if (table.getName().equalsIgnoreCase(currentTable.getName())) {
          Table tempTable = getTemporaryTableFor(desiredModel, change.getChangedTable());
          Column changedNewColumn = change.getNewColumn();

          boolean isADTable = ad == null || ad.getTable(table.getName()) != null;

          if (changedNewColumn.getOnCreateDefault() != null
              && (isADTable || !desiredModel.isDeferredDefault(table, changedNewColumn))
              && (!changedNewColumn.isRequired() || !changedNewColumn.isSameDefaultAndOCD())) {
            executeOnCreateDefault(table, tempTable, change.getNewColumn(), recreated, true);
          }
          writeColumnCommentStmt(currentModel, change.getChangedTable(), change.getNewColumn(),
              true);
        }
      }
      if (recreated) {
        recreatedTables.add(currentTable.getName());
        if (newColumn) {

          if (fullModel != null) {
            // We have the full model. We will activate foreign keys pointing to recreated tables
            Table recreatedTable = desiredModel.getTable(i);
            for (int idxTable = 0; idxTable < fullModel.getTableCount(); idxTable++) {
              for (int idxFk = 0; idxFk < fullModel.getTable(idxTable)
                  .getForeignKeyCount(); idxFk++) {
                ForeignKey fk = fullModel.getTable(idxTable).getForeignKey(idxFk);
                if (currentTable.getName().equalsIgnoreCase(fk.getForeignTableName())
                    && !recreatedFKs.contains(fk.getName())) {
                  recreatedFKs.add(fk.getName());
                  writeExternalForeignKeyCreateStmt(fullModel, fullModel.getTable(idxTable), fk);
                }
              }
            }
          }
        }
      } else {
        // obtain new indexes to be returned to process them afterwards
        for (ModelChange change : changes) {
          if (!(change instanceof AddIndexChange)) {
            continue;
          }
          AddIndexChange ichange = (AddIndexChange) change;
          if (ichange.getChangedTable()
              .getName()
              .equalsIgnoreCase(desiredModel.getTable(i).getName())) {
            newIndexes.add((AddIndexChange) change);
          }
        }
      }
    }

    for (int i = 0; i < desiredModel.getViewCount(); i++) {
      createView(desiredModel.getView(i));
    }

    // We will now recreate the unchanged foreign keys
    ListOrderedMap changesPerTable = new ListOrderedMap();
    ListOrderedMap unchangedFKs = new ListOrderedMap();
    boolean caseSensitive = getPlatform().isDelimitedIdentifierModeOn();

    // we first sort the changes for the tables
    // however since the changes might contain source or target tables
    // we use the names rather than the table objects
    for (Iterator changeIt = tableChanges.iterator(); changeIt.hasNext();) {
      TableChange change = (TableChange) changeIt.next();
      String name = change.getChangedTable().getName();

      if (!caseSensitive) {
        name = name.toUpperCase();
      }

      List changesForTable = (ArrayList) changesPerTable.get(name);

      if (changesForTable == null) {
        changesForTable = new ArrayList();
        changesPerTable.put(name, changesForTable);
        unchangedFKs.put(name, getUnchangedForeignKeys(currentModel, desiredModel, name));
      }
      changesForTable.add(change);
    }

    // recreate unchanged FKs from non recreated tables to recreated tables
    addRelevantFKsFromUnchangedTables(currentModel, desiredModel, changesPerTable.keySet(),
        unchangedFKs);

    for (Iterator tableFKIt = unchangedFKs.entrySet().iterator(); tableFKIt.hasNext();) {
      Map.Entry entry = (Map.Entry) tableFKIt.next();
      Table targetTable = desiredModel.findTable((String) entry.getKey(), caseSensitive);

      for (Iterator fkIt = ((List) entry.getValue()).iterator(); fkIt.hasNext();) {
        ForeignKey fk = (ForeignKey) fkIt.next();
        if (droppedFKs.contains(fk.getName()) && !recreatedFKs.contains(fk.getName())) {
          recreatedFKs.add(fk.getName());
          writeExternalForeignKeyCreateStmt(desiredModel, targetTable, fk);
        }
      }
    }

    for (ModelChange change : changes) {
      if (change instanceof AddForeignKeyChange) {
        ForeignKey fk = ((AddForeignKeyChange) change).getNewForeignKey();
        if (!recreatedFKs.contains(fk.getName())) {
          recreatedFKs.add(fk.getName());
          writeExternalForeignKeyCreateStmt(desiredModel,
              ((AddForeignKeyChange) change).getChangedTable(),
              ((AddForeignKeyChange) change).getNewForeignKey());
        }
      } else if (change instanceof AddUniqueChange) {
        processChange(currentModel, desiredModel, params, ((AddUniqueChange) change));
      } else if (change instanceof AddCheckChange) {
        processChange(currentModel, desiredModel, params, ((AddCheckChange) change));
      }
    }

    return newIndexes;
  }

  /**
   * Hook that is executed after all the NewIndexChanges for a list of tables have been created
   * 
   * @param newIndexesMap
   *          a map of all the tables that have new indexes, along with the new indexes
   * @throws IOException
   */
  protected void newIndexesPostAction(Map<Table, List<Index>> newIndexesMap) throws IOException {
  }

  /**
   * Hook that is executed after all the NewIndexChanges for a table have been created
   * 
   * @param newIndexesMap
   *          a map of all the tables that have new indexes, along with the new indexes
   * @throws IOException
   */
  private void newIndexesPostAction(Table table, Index[] indexes) throws IOException {
    Map<Table, List<Index>> newIndexesMap = new HashMap<Table, List<Index>>();
    newIndexesMap.put(table, Arrays.asList(indexes));
    newIndexesPostAction(newIndexesMap);
  }

  public boolean hasBeenRecreated(Table table) {
    return recreatedTables.contains(table.getName());
  }

  public void executeStandardDefault(Table table, Column col) throws IOException {
    if (col.getDefaultValue() != null && !col.getDefaultValue().equals("")) {
      println("UPDATE " + table.getName() + " SET " + col.getName() + "=" + getDefaultValue(col)
          + " WHERE " + col.getName() + " IS NULL");
      printEndOfStatement();
    }
  }

  public void executeOnCreateDefault(Table table, Table tempTable, Column col, boolean recreated,
      boolean onlyForNullRows) throws IOException {
    String pk = "";
    Column[] pks1 = table.getPrimaryKeyColumns();
    if (recreated) {
      for (int i = 0; i < pks1.length; i++) {
        if (i > 0) {
          pk += " AND ";
        }
        pk += "TO_CHAR(" + table.getName() + "." + pks1[i].getName() + ")=TO_CHAR("
            + tempTable.getName() + "." + pks1[i].getName() + ")";
      }
    }
    String oncreatedefault = col.getOnCreateDefault();
    if (oncreatedefault != null && !oncreatedefault.equals("")) {
      print("UPDATE " + table.getName() + " SET " + col.getName() + "=(" + oncreatedefault + ")");
      if (onlyForNullRows) {
        print(" WHERE " + col.getName() + " IS NULL");
      }
      println();
      printEndOfStatement();
    }
  }

  /**
   * Calls the given closure for all changes that are of one of the given types, and then removes
   * them from the changes collection.
   * 
   * @param changes
   *          The changes
   * @param changeTypes
   *          The types to search for
   * @param closure
   *          The closure to invoke
   */
  protected void applyForSelectedChanges(Collection changes, Class[] changeTypes,
      final Closure closure) {
    final Predicate predicate = new MultiInstanceofPredicate(changeTypes);

    // basically we filter the changes for all objects where the above
    // predicate
    // returns true, and for these filtered objects we invoke the given
    // closure
    CollectionUtils.filter(changes, new Predicate() {
      @Override
      public boolean evaluate(Object obj) {
        if (predicate.evaluate(obj)) {
          closure.execute(obj);
          return false;
        } else {
          return true;
        }
      }
    });
  }

  private void removeExternalConstraintsIndexesAndViews(Database currentModel,
      List<ModelChange> changes, CallbackClosure callbackClosure) throws IOException {

    // The list of removed indexes that define an operator class
    // It is populated in processChange(Database currentModel, Database desiredModel,
    // CreationParameters params, RemoveIndexChange change), but cannot be passed as a parameter to
    // applyForSelectedChanges because it is a too generic method, that's why it is defined as a
    // class attribute
    _removedIndexesWithOperatorClassMap = new HashMap<>();

    // The list of removed partial index, populated also in processChange(Database currentModel,
    // Database desiredModel, CreationParameters params, RemoveIndexChange change)
    _removedIndexesWithWhereClause = new HashMap<>();
    // 1st pass: removing external constraints and indices
    applyForSelectedChanges(changes, new Class[] { RemoveForeignKeyChange.class,
        RemoveUniqueChange.class, RemoveIndexChange.class, RemoveCheckChange.class },
        callbackClosure);

    if (!_removedIndexesWithOperatorClassMap.isEmpty()) {
      removedOperatorClassIndexesPostAction(_removedIndexesWithOperatorClassMap);
    }

    if (!_removedIndexesWithWhereClause.isEmpty()) {
      removedPartialIndexesPostAction(_removedIndexesWithWhereClause);
    }

    for (View view : currentModel.getViews()) {
      dropView(view);
    }
  }

  /**
   * Processes the changes. The default argument performs several passes:
   * <ol>
   * <li>{@link org.apache.ddlutils.alteration.RemoveForeignKeyChange} and
   * {@link org.apache.ddlutils.alteration.RemoveIndexChange} come first to allow for e.g.
   * subsequent primary key changes or column removal.</li>
   * <li>{@link org.apache.ddlutils.alteration.RemoveTableChange} comes after the removal of foreign
   * keys and indices.</li>
   * <li>These are all handled together:<br/>
   * {@link org.apache.ddlutils.alteration.RemovePrimaryKeyChange}<br/>
   * {@link org.apache.ddlutils.alteration.AddPrimaryKeyChange}<br/>
   * {@link org.apache.ddlutils.alteration.PrimaryKeyChange}<br/>
   * {@link org.apache.ddlutils.alteration.RemoveColumnChange}<br/>
   * {@link org.apache.ddlutils.alteration.AddColumnChange}<br/>
   * {@link org.apache.ddlutils.alteration.ColumnAutoIncrementChange}<br/>
   * {@link org.apache.ddlutils.alteration.ColumnDefaultValueChange}<br/>
   * {@link org.apache.ddlutils.alteration.ColumnRequiredChange}<br/>
   * {@link org.apache.ddlutils.alteration.ColumnDataTypeChange}<br/>
   * {@link org.apache.ddlutils.alteration.ColumnSizeChange}<br/>
   * The reason for this is that the default algorithm rebuilds the table for these changes and thus
   * their order is irrelevant.</li>
   * <li>{@link org.apache.ddlutils.alteration.AddTableChange}<br/>
   * needs to come after the table removal (so that tables of the same name are removed) and before
   * the addition of foreign keys etc.</li>
   * <li>{@link org.apache.ddlutils.alteration.AddForeignKeyChange} and
   * {@link org.apache.ddlutils.alteration.AddIndexChange} come last after table/column/primary key
   * additions or changes.</li>
   * </ol>
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param changes
   *          The changes
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   */
  protected void processChanges(Database currentModel, Database desiredModel,
      List<ModelChange> changes, CreationParameters params, boolean createConstraints)
      throws IOException {
    CallbackClosure callbackClosure = new CallbackClosure(this, "processChange",
        new Class[] { Database.class, Database.class, CreationParameters.class, null },
        new Object[] { currentModel, desiredModel, params, null });

    // 2nd pass: removing tables and views and functions and triggers
    applyForSelectedChanges(changes, new Class[] { RemoveViewChange.class }, callbackClosure);
    applyForSelectedChanges(changes, new Class[] { RemoveTriggerChange.class,
        RemoveFunctionChange.class, RemoveTableChange.class, RemoveSequenceChange.class },
        callbackClosure);

    // 3rd pass: changing the structure of tables
    Predicate predicate = new MultiInstanceofPredicate(new Class[] { RemovePrimaryKeyChange.class,
        AddPrimaryKeyChange.class, PrimaryKeyChange.class, RemoveColumnChange.class,
        AddColumnChange.class, ColumnOrderChange.class, ColumnAutoIncrementChange.class,
        ColumnDefaultValueChange.class, ColumnOnCreateDefaultValueChange.class,
        ColumnRequiredChange.class, ColumnDataTypeChange.class, ColumnSizeChange.class });

    Predicate predicatetriggers = new MultiInstanceofPredicate(
        new Class[] { AddTriggerChange.class });

    processTableStructureChanges(currentModel, desiredModel, params,
        CollectionUtils.select(changes, predicate),
        CollectionUtils.select(changes, predicatetriggers));

    // 4th pass: adding tables
    applyForSelectedChanges(changes, new Class[] { AddTableChange.class }, callbackClosure);

    // 5th pass: adding external constraints and indices
    if (createConstraints) {
      applyForSelectedChanges(changes, new Class[] { AddForeignKeyChange.class,
          AddUniqueChange.class, AddIndexChange.class, AddCheckChange.class }, callbackClosure);
    }

    applyForSelectedChanges(changes, new Class[] { AddSequenceChange.class }, callbackClosure);

    applyForSelectedChanges(changes, new Class[] { SequenceDefinitionChange.class },
        callbackClosure);

    applyForSelectedChanges(changes, new Class[] { AddFunctionChange.class }, callbackClosure);

    applyForSelectedChanges(changes, new Class[] { AddViewChange.class }, callbackClosure);

    applyForSelectedChanges(changes, new Class[] { AddTriggerChange.class }, callbackClosure);

    if (!getPlatformInfo().isPartialIndexesSupported()) {
      applyForSelectedChanges(changes, new Class[] { PartialIndexInformationChange.class },
          callbackClosure);
      updatePartialIndexesPostAction();
    }

    if (!getPlatformInfo().isContainsSearchIndexesSupported()) {
      applyForSelectedChanges(changes, new Class[] { ContainsSearchIndexInformationChange.class },
          callbackClosure);
      updateContainsSearchIndexesPostAction();
    }
  }

  /**
   * Hook to execute actions after all the RemoveIndexChanges belonging to indexes that define an
   * operator class are defined
   * 
   * @param removedIndexesWithOperatorClassMap
   *          a map of all the tables that have indexes (with an operator class) to be removed,
   *          along with the removed indexes
   * @throws IOException
   */
  protected void removedOperatorClassIndexesPostAction(
      Map<String, List<Index>> removedIndexesWithOperatorClassMap) throws IOException {
  }

  /**
   * Hook to execute actions after all the RemoveIndexChanges belonging to partial indexes are
   * defined
   * 
   * @param removedIndexesWithWhereClause
   *          a map of all the tables that have partial indexes to be removed, along with the
   *          removed partial indexes
   * @throws IOException
   */
  protected void removedPartialIndexesPostAction(
      Map<String, List<Index>> removedIndexesWithWhereClause) throws IOException {
  }

  /**
   * Action to be executed when a change on the where clause of an index is detected. It must be
   * implemented for those platforms where partial indexing is not supported.
   * 
   * @param table
   *          the table where the changed index belongs
   * @param index
   *          the modified index
   * @param oldWhereClause
   *          the former where clause
   * @param newWhereClause
   *          the new where clause
   * @throws IOException
   */
  protected void updatePartialIndexAction(Table table, Index index, String oldWhereClause,
      String newWhereClause) throws IOException {
  }

  /**
   * Action to be executed once all changes in partial indexes have been applied in the model
   * 
   * @throws IOException
   */
  protected void updatePartialIndexesPostAction() throws IOException {
  }

  /**
   * Action to be executed when a change on the containsSearch property of an index is detected. It
   * must be implemented for those platforms where contains search indexes are not supported.
   * 
   * @param table
   *          the table where the changed index belongs
   * @param index
   *          the modified index
   * @param newContainsSearchValue
   *          the new value of the containsSearch property
   * @throws IOException
   */
  protected void updateContainsSearchIndexAction(Table table, Index index,
      boolean newContainsSearchValue) throws IOException {
  }

  /**
   * Action to be executed once all changes in contains search indexes have been applied in the
   * model
   * 
   * @throws IOException
   */
  protected void updateContainsSearchIndexesPostAction() throws IOException {
  }

  /**
   * This is a fall-through callback which generates a warning because a specific change type wasn't
   * handled.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, ModelChange change) throws IOException {
    _log.warn("Change of type " + change.getClass() + " was not handled");
  }

  /**
   * Processes the change representing the removal of a foreign key.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveForeignKeyChange change) throws IOException {
    writeExternalForeignKeyDropStmt(change.getChangedTable(), change.getForeignKey());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of an unique.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveUniqueChange change) throws IOException {
    writeExternalUniqueDropStmt(change.getChangedTable(), change.getUnique());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of an index.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveIndexChange change) throws IOException {
    Index removedIndex = change.getIndex();
    writeExternalIndexDropStmt(change.getChangedTable(), removedIndex);
    if (indexHasColumnWithOperatorClass(removedIndex) || removedIndex.isContainsSearch()) {
      // keep track of the removed indexes that use operator classes (including indexes flagged as
      // contains search), as in some platforms is it required to update the comments of the tables
      // that own them
      putRemovedIndex(_removedIndexesWithOperatorClassMap, change);
    }
    if (removedIndex.getWhereClause() != null && !removedIndex.getWhereClause().isEmpty()) {
      // keep track of the removed partial indexes, as in some platforms is it
      // required to update the comments of the tables that own them
      putRemovedIndex(_removedIndexesWithWhereClause, change);
    }
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  private void putRemovedIndex(Map<String, List<Index>> removedIndexesMap,
      RemoveIndexChange change) {
    String tableName = change.getChangedTable().getName();
    List<Index> indexList = removedIndexesMap.get(tableName);
    if (indexList == null) {
      indexList = new ArrayList<Index>();
    }
    indexList.add(change.getIndex());
    removedIndexesMap.put(tableName, indexList);
  }

  /**
   * Processes the change representing modifications in the information of partial indexes which is
   * stored to maintain consistency between the XML model and the database. This changes only apply
   * for those platforms where partial indexes are not supported as they are used just to keep
   * updated that information.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, PartialIndexInformationChange change) throws IOException {
    updatePartialIndexAction(change.getChangedTable(), change.getIndex(),
        change.getOldWhereClause(), change.getNewWhereClause());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing modifications in the information of contains search indexes
   * which is stored to maintain consistency between the XML model and the database. This changes
   * only apply for those platforms where contains search indexes are not supported as they are used
   * just to keep updated that information.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, ContainsSearchIndexInformationChange change) throws IOException {
    updateContainsSearchIndexAction(change.getChangedTable(), change.getIndex(),
        change.getNewContainsSearch());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of an check.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveCheckChange change) throws IOException {
    writeExternalCheckDropStmt(change.getChangedTable(), change.getCheck());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of a table.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveTableChange change) throws IOException {
    dropTable(change.getChangedTable());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of a table.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddTableChange change) throws IOException {
    createdTables.add(change.getNewTable().getName());
    createTable(desiredModel, change.getNewTable(),
        params == null ? null : params.getParametersFor(change.getNewTable()));
    writeExternalPrimaryKeysCreateStmt(change.getNewTable(), change.getNewTable().getPrimaryKey(),
        change.getNewTable().getPrimaryKeyColumns());
    writeExternalIndicesCreateStmt(change.getNewTable());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of a sequence.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveSequenceChange change) throws IOException {
    dropSequence(change.getSequence());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of a sequence.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddSequenceChange change) throws IOException {
    createSequence(change.getNewSequence());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, SequenceDefinitionChange change) throws IOException {
    alterSequence(change.getSequence());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of a view.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveViewChange change) throws IOException {
    dropView(change.getView());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of a view.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddViewChange change) throws IOException {
    createView(change.getNewView());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of a function.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveFunctionChange change) throws IOException {
    dropFunction(change.getFunction());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of a function.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddFunctionChange change) throws IOException {
    createFunction(change.getNewFunction());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the removal of a trigger.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, RemoveTriggerChange change) throws IOException {
    dropTrigger(currentModel, change.getTrigger());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of a trigger.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddTriggerChange change) throws IOException {
    createTrigger(desiredModel, change.getNewTrigger());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of a foreign key.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddForeignKeyChange change) throws IOException {
    writeExternalForeignKeyCreateStmt(desiredModel, change.getChangedTable(),
        change.getNewForeignKey());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of an unique.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddUniqueChange change) throws IOException {
    writeExternalUniqueCreateStmt(change.getChangedTable(), change.getNewUnique());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of an index.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddIndexChange change) throws IOException {
    writeExternalIndexCreateStmt(change.getChangedTable(), change.getNewIndex());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the change representing the addition of an index.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      CreationParameters params, AddCheckChange change) throws IOException {
    writeExternalCheckCreateStmt(change.getChangedTable(), change.getNewCheck());
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Processes the changes to the structure of tables.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param params
   *          The parameters used in the creation of new tables. Note that for existing tables, the
   *          parameters won't be applied
   * @param changes
   *          The change objects
   */
  protected void processTableStructureChanges(Database currentModel, Database desiredModel,
      CreationParameters params, Collection changes, Collection triggerchanges) throws IOException {
    // Get unchanged triggers
    Set<String> addedTriggers = new HashSet<String>();
    for (Iterator changeIt = triggerchanges.iterator(); changeIt.hasNext();) {
      AddTriggerChange change = (AddTriggerChange) changeIt.next();
      addedTriggers.add((change).getNewTrigger().getName());
    }

    Set<String> unchangedTriggers = new HashSet<String>();
    for (int i = 0; i < desiredModel.getTriggerCount(); i++) {
      if (!addedTriggers.contains(desiredModel.getTrigger(i).getName())) {
        unchangedTriggers.add(desiredModel.getTrigger(i).getName());
      }
    }

    ListOrderedMap changesPerTable = new ListOrderedMap();
    ListOrderedMap unchangedFKs = new ListOrderedMap();
    boolean caseSensitive = getPlatform().isDelimitedIdentifierModeOn();
    // we first sort the changes for the tables
    // however since the changes might contain source or target tables
    // we use the names rather than the table objects
    for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
      TableChange change = (TableChange) changeIt.next();
      String name = change.getChangedTable().getName();

      if (!caseSensitive) {
        name = name.toUpperCase();
      }

      List changesForTable = (ArrayList) changesPerTable.get(name);

      if (changesForTable == null) {
        changesForTable = new ArrayList();
        changesPerTable.put(name, changesForTable);
        unchangedFKs.put(name, getUnchangedForeignKeys(currentModel, desiredModel, name));
      }
      changesForTable.add(change);
    }

    addRelevantFKsFromUnchangedTables(currentModel, desiredModel, changesPerTable.keySet(),
        unchangedFKs);

    // We're using a copy of the current model so that the table structure
    // changes can
    // modify it
    Database copyOfCurrentModel = null;

    try {
      copyOfCurrentModel = (Database) currentModel.clone();
    } catch (CloneNotSupportedException ex) {
      throw new DdlUtilsException(ex);
    }

    for (Iterator tableChangeIt = changesPerTable.entrySet().iterator(); tableChangeIt.hasNext();) {
      Map.Entry entry = (Map.Entry) tableChangeIt.next();
      Table targetTable = desiredModel.findTable((String) entry.getKey(), caseSensitive);

      processTableStructureChanges(copyOfCurrentModel, desiredModel, (String) entry.getKey(),
          params == null ? null : params.getParametersFor(targetTable), (List) entry.getValue(),
          unchangedTriggers);
    }
  }

  /**
   * Determines the unchanged foreign keys of the indicated table.
   * 
   * @param currentModel
   *          The current model
   * @param desiredModel
   *          The desired model
   * @param tableName
   *          The name of the table
   * @return The list of unchanged foreign keys
   */
  private List getUnchangedForeignKeys(Database currentModel, Database desiredModel,
      String tableName) {
    ArrayList unchangedFKs = new ArrayList();
    boolean caseSensitive = getPlatform().isDelimitedIdentifierModeOn();
    Table sourceTable = currentModel.findTable(tableName, caseSensitive);
    Table targetTable = desiredModel.findTable(tableName, caseSensitive);

    for (int idx = 0; idx < targetTable.getForeignKeyCount(); idx++) {
      ForeignKey targetFK = targetTable.getForeignKey(idx);
      ForeignKey sourceFK = sourceTable.findForeignKey(targetFK, caseSensitive);

      if (sourceFK != null) {
        unchangedFKs.add(targetFK);
      }
    }
    return unchangedFKs;
  }

  /**
   * Adds the foreign keys of the unchanged tables that reference changed tables to the given map.
   * 
   * @param currentModel
   *          The current model
   * @param desiredModel
   *          The desired model
   * @param namesOfKnownChangedTables
   *          The known names of changed tables
   * @param fksPerTable
   *          The map table name -> foreign keys to which found foreign keys will be added to
   */
  private void addRelevantFKsFromUnchangedTables(Database currentModel, Database desiredModel,
      Set namesOfKnownChangedTables, Map fksPerTable) {
    boolean caseSensitive = getPlatform().isDelimitedIdentifierModeOn();

    for (int tableIdx = 0; tableIdx < desiredModel.getTableCount(); tableIdx++) {
      Table targetTable = desiredModel.getTable(tableIdx);
      String name = targetTable.getName();
      Table sourceTable = currentModel.findTable(name, caseSensitive);
      List relevantFks = null;

      if (!caseSensitive) {
        name = name.toUpperCase();
      }
      if ((sourceTable != null) && !namesOfKnownChangedTables.contains(name)) {
        for (int fkIdx = 0; fkIdx < targetTable.getForeignKeyCount(); fkIdx++) {
          ForeignKey targetFk = targetTable.getForeignKey(fkIdx);
          ForeignKey sourceFk = sourceTable.findForeignKey(targetFk, caseSensitive);
          String refName = targetFk.getForeignTableName();

          if (!caseSensitive) {
            refName = refName.toUpperCase();
          }
          if ((sourceFk != null) && namesOfKnownChangedTables.contains(refName)) {
            if (relevantFks == null) {
              relevantFks = new ArrayList();
              fksPerTable.put(name, relevantFks);
            }
            relevantFks.add(targetFk);
          }
        }
      }
    }
  }

  /**
   * Processes the changes to the structure of a single table. Database-specific implementations
   * might redefine this method, but it is usually sufficient to redefine the
   * {@link #processTableStructureChanges(Database, Database, Table, Table, Map, List)} method
   * instead.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param tableName
   *          The name of the changed table
   * @param parameters
   *          The creation parameters for the desired table
   * @param changes
   *          The change objects for this table
   */
  protected void processTableStructureChanges(Database currentModel, Database desiredModel,
      String tableName, Map parameters, List<TableChange> changes, Set<String> unchangedtriggers)
      throws IOException {
    Table sourceTable = currentModel.findTable(tableName,
        getPlatform().isDelimitedIdentifierModeOn());
    Table targetTable = desiredModel.findTable(tableName,
        getPlatform().isDelimitedIdentifierModeOn());

    processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable, parameters,
        changes);

    if (changes.isEmpty()) {
      // we're done, all changes for the table have been processed without need of recreating it
      return;
    }

    // there are changes that cannot be processed without table recreation, let's recreate the
    // whole table

    // drop FKs to recreated table
    int numTablesInOldModel = currentModel.getTableCount();
    for (int i = 0; i < numTablesInOldModel; i++) {
      Table oldTable = currentModel.getTable(i);

      for (int fkIdx = 0; fkIdx < oldTable.getForeignKeyCount(); fkIdx++) {
        ForeignKey oldFk = oldTable.getForeignKey(fkIdx);
        if (!tableName.equals(oldFk.getForeignTableName())) {
          // we only care at this point about FKs to current table
          continue;
        }

        if (!recreatedTables.contains(oldTable.getName())) {
          // no need to drop FKs in already recreated tables, but we need to remember it as dropped
          // to be recreated later
          writeExternalForeignKeyDropStmt(oldTable, oldFk);
        }

        droppedFKs.add(oldFk.getName());
      }
    }

    // read unchanged triggers
    List<Trigger> triggers = new ArrayList<Trigger>();
    for (int i = 0; i < desiredModel.getTriggerCount(); i++) {
      Trigger t = desiredModel.getTrigger(i);
      if (unchangedtriggers.contains(t.getName()) && t.getTable().equals(tableName)) {
        triggers.add(t);
      }
    }

    // drop unchanged triggers
    for (Iterator<Trigger> it = triggers.iterator(); it.hasNext();) {
      dropTrigger(desiredModel, it.next());
    }

    Table realTargetTable = getRealTargetTableFor(desiredModel, sourceTable, targetTable);

    if (recreatedPKs.contains(sourceTable.getName())) {
      recreatedPKs.remove(sourceTable.getName());
    }
    Table tempTable = getTemporaryTableFor(currentModel, sourceTable);
    createTemporaryTable(desiredModel, tempTable, parameters);
    disableAllNOTNULLColumns(tempTable);
    writeCopyDataStatement(sourceTable, tempTable);
    // Note that we don't drop the indices here because the DROP
    // TABLE will take care of that
    // Likewise, foreign keys have already been dropped as necessary
    dropTable(sourceTable);

    @SuppressWarnings("unchecked")
    List<String> originallyRecreatedTables = (List<String>) recreatedTables.clone();
    if (recreatedTables.contains(sourceTable.getName())) {
      recreatedTablesTwice.add(sourceTable.getName());
    }
    recreatedTables.add(sourceTable.getName());

    createTable(desiredModel, realTargetTable, parameters);
    disableAllNOTNULLColumns(realTargetTable, originallyRecreatedTables, desiredModel);
    writeCopyDataStatement(tempTable, targetTable);
    dropTemporaryTable(desiredModel, tempTable);

    // create unchanged triggers
    for (Iterator<Trigger> it = triggers.iterator(); it.hasNext();) {
      createTrigger(desiredModel, it.next());
    }
  }

  /**
   * Creates a temporary table object that corresponds to the given table. Database-specific
   * implementations may redefine this method if e.g. the database directly supports temporary
   * tables. The default implementation simply appends an underscore to the table name and uses that
   * as the table name.
   * 
   * @param targetModel
   *          The target database
   * @param targetTable
   *          The target table
   * @return The temporary table
   */
  protected Table getTemporaryTableFor(Database targetModel, Table targetTable) throws IOException {
    Table table = new Table();

    table.setCatalog(targetTable.getCatalog());
    table.setSchema(targetTable.getSchema());
    table.setName(targetTable.getName() + "_");
    table.setType(targetTable.getType());
    table.setPrimaryKey(null); // generated name
    for (int idx = 0; idx < targetTable.getColumnCount(); idx++) {
      try {
        table.addColumn((Column) targetTable.getColumn(idx).clone());
      } catch (CloneNotSupportedException ex) {
        throw new DdlUtilsException(ex);
      }
    }

    return table;
  }

  /**
   * Outputs the DDL to create the given temporary table. Per default this is simply a call to
   * {@link #createTable(Database, Table, Map)}.
   * 
   * @param database
   *          The database model
   * @param table
   *          The table
   * @param parameters
   *          Additional platform-specific parameters for the table creation
   */
  protected void createTemporaryTable(Database database, Table table, Map parameters)
      throws IOException {
    createTable(database, table, parameters);
  }

  protected void disableNOTNULLColumns(Vector<AddColumnChange> newColumns) throws IOException {
    for (int i = 0; i < newColumns.size(); i++) {
      Column column = newColumns.get(i).getNewColumn();
      if (column.isRequired() && !column.isPrimaryKey()) {
        println("ALTER TABLE " + newColumns.get(i).getChangedTable().getName() + " MODIFY "
            + getColumnName(column) + " " + getSqlType(column) + " NULL");
        printEndOfStatement();
      }

    }
  }

  protected void disableAllNOTNULLColumns(Table table) throws IOException {
    disableAllNOTNULLColumns(table, recreatedTables, null);
  }

  protected void disableAllNOTNULLColumns(Table table, Database database) throws IOException {
    disableAllNOTNULLColumns(table, recreatedTables, database);
  }

  protected void disableAllNOTNULLColumns(Table table, List<String> recreatedTbls,
      Database database) throws IOException {
    for (int i = 0; i < table.getColumnCount(); i++) {
      Column column = table.getColumn(i);

      if (shouldDisableNotNull(table, column, recreatedTbls, database)) {
        disableNotNull(table.getName(), column);
      }
    }
  }

  /** Print SQL statement to disable not-null contratint in for a given column. */
  protected void disableNotNull(String tableName, Column column) throws IOException {
    // Not implemented: it should be implemented based on platform
  }

  private boolean shouldDisableNotNull(Table table, Column column, List<String> recreatedTbls,
      Database database) {
    if (!(column.isRequired() && !column.isPrimaryKey()
        && !recreatedTbls.contains(table.getName()))) {
      return false;
    }

    if (database == null) {
      return true;
    }

    // do not disable not null columns with deferred constraint because it is not enabled yet
    return !database.isDeferredNotNull(table, column);
  }

  protected void disableAllChecks(Table table) throws IOException {

    for (int i = 0; i < table.getCheckCount(); i++) {
      Check check = table.getCheck(i);
      println("ALTER TABLE " + table.getName() + " DISABLE CONSTRAINT " + check.getName());
      printEndOfStatement();
    }

  }

  protected void disableTempNOTNULLColumns(Vector<AddColumnChange> newColumns) throws IOException {
    for (int i = 0; i < newColumns.size(); i++) {
      Column column = newColumns.get(i).getNewColumn();
      if (column.isRequired() && !column.isPrimaryKey()) {
        disableNotNull(newColumns.get(i).getChangedTable().getName() + "_", column);
      }
    }
  }

  protected void enableAllNOTNULLColumns(Table table) throws IOException {
    for (int i = 0; i < table.getColumnCount(); i++) {
      Column column = table.getColumn(i);
      if (column.isRequired() && !column.isPrimaryKey()
          && !recreatedTablesTwice.contains(table.getName())) {
        if (getSqlType(column).equalsIgnoreCase("CLOB")) {
          // In the case of CLOB columns in oracle, it is wrong to specify the type when changing
          // the null/not null constraint
          println(
              "ALTER TABLE " + table.getName() + " MODIFY " + getColumnName(column) + " NOT NULL");
        } else {
          println("ALTER TABLE " + table.getName() + " MODIFY " + getColumnName(column) + " "
              + getSqlType(column) + " NOT NULL");
        }
        printEndOfStatement();
      }

    }
  }

  protected void enableAllChecks(Table table) throws IOException {

    for (int i = 0; i < table.getCheckCount(); i++) {
      Check check = table.getCheck(i);
      println("ALTER TABLE " + table.getName() + " ENABLE CONSTRAINT " + check.getName());
      printEndOfStatement();
    }

  }

  protected void enableNOTNULLColumns(Vector<ColumnChange> newColumns) throws IOException {
    for (int i = 0; i < newColumns.size(); i++) {
      Column column = newColumns.get(i).getChangedColumn();
      if (column.isRequired() && !column.isPrimaryKey()) {
        println("ALTER TABLE " + newColumns.get(i).getChangedTable().getName() + " MODIFY "
            + getColumnName(column) + " " + getSqlType(column) + " NOT NULL");
        printEndOfStatement();
      }

    }
  }

  public void processDataChanges(Database database, DatabaseData databaseData, List changes)
      throws IOException {
    for (int i = 0; i < changes.size(); i++) {
      if (changes.get(i) instanceof AddRowChange) {
        AddRowChange change = (AddRowChange) changes.get(i);
        DynaBean row = change.getRow();
        DynaProperty[] properties = database.getDynaClassFor(row).getDynaProperties();
        HashMap parameters = new HashMap();

        for (int idx = 0; idx < properties.length; idx++) {
          parameters.put(properties[idx].getName(), row.get(properties[idx].getName()));
        }
        println(getInsertSql(change.getTable(), parameters, false));
        printEndOfStatement();
      } else if (changes.get(i) instanceof RemoveRowChange) {
        RemoveRowChange change = (RemoveRowChange) changes.get(i);
        DynaBean row = change.getRow();
        DynaProperty[] properties = database.getDynaClassFor(row).getPrimaryKeyProperties();
        HashMap parameters = new HashMap();

        for (int idx = 0; idx < properties.length; idx++) {
          parameters.put(properties[idx].getName(), row.get(properties[idx].getName()));
        }
        println(getDeleteSql(change.getTable(), parameters, false));
        printEndOfStatement();
      } else if (changes.get(i) instanceof ColumnDataChange) {
        ColumnDataChange change = (ColumnDataChange) changes.get(i);
        HashMap parameters = new HashMap();
        Column[] pkCols = change.getTable().getPrimaryKeyColumns();
        Object pkV = change.getPrimaryKey();
        for (int idx = 0; idx < pkCols.length; idx++) {
          parameters.put(pkCols[idx].getName(), pkV);
        }
        parameters.put(change.getColumn().getName(), change.getNewValue());
        println(getUpdateSql(change.getTable(), parameters, false));
        printEndOfStatement();
      } else {
        _log.error("Error: not a data change");
      }
    }

  }

  public void generateDataInsertions(Database database, DatabaseData databaseData)
      throws IOException {
    for (int i = 0; i < database.getTableCount(); i++) {
      Table table = database.getTable(i);
      Vector<DynaBean> rowsTable = databaseData.getRowsFromTable(table.getName());
      if (rowsTable != null) {
        for (int j = 0; j < rowsTable.size(); j++) {
          DynaBean row = rowsTable.get(j);
          DynaProperty[] properties = database.getDynaClassFor(row).getDynaProperties();
          HashMap parameters = new HashMap();

          for (int idx = 0; idx < properties.length; idx++) {
            parameters.put(properties[idx].getName(), row.get(properties[idx].getName()));
          }
          println(getInsertSql(table, parameters, false));
          printEndOfStatement();
        }
      }
    }
  }

  /**
   * Outputs the DDL to drop the given temporary table. Per default this is simply a call to
   * {@link #dropTable(Table)}.
   * 
   * @param database
   *          The database model
   * @param table
   *          The table
   */
  protected void dropTemporaryTable(Database database, Table table) throws IOException {
    dropTable(table);
  }

  /**
   * Creates the target table object that differs from the given target table only in the indices.
   * More specifically, only those indices are used that have not changed.
   * 
   * @param targetModel
   *          The target database
   * @param sourceTable
   *          The source table
   * @param targetTable
   *          The target table
   * @return The table
   */
  protected Table getRealTargetTableFor(Database targetModel, Table sourceTable, Table targetTable)
      throws IOException {
    Table table = new Table();

    table.setCatalog(targetTable.getCatalog());
    table.setSchema(targetTable.getSchema());
    table.setName(targetTable.getName());
    table.setType(targetTable.getType());
    table.setPrimaryKey(targetTable.getPrimaryKey());
    for (int idx = 0; idx < targetTable.getColumnCount(); idx++) {
      try {
        table.addColumn((Column) targetTable.getColumn(idx).clone());
      } catch (CloneNotSupportedException ex) {
        throw new DdlUtilsException(ex);
      }
    }

    boolean caseSensitive = getPlatform().isDelimitedIdentifierModeOn();

    for (int idx = 0; idx < targetTable.getUniqueCount(); idx++) {
      Unique targetUnique = targetTable.getUnique(idx);
      Unique sourceUnique = sourceTable.findUnique(targetUnique.getName(), caseSensitive);

      if (sourceUnique != null) {
        if ((caseSensitive && sourceUnique.equals(targetUnique))
            || (!caseSensitive && sourceUnique.equalsIgnoreCase(targetUnique))) {
          table.addUnique(targetUnique);
        }
      }
    }

    for (int idx = 0; idx < targetTable.getIndexCount(); idx++) {
      Index targetIndex = targetTable.getIndex(idx);
      Index sourceIndex = sourceTable.findIndex(targetIndex.getName(), caseSensitive);

      if (sourceIndex != null) {
        if ((caseSensitive && sourceIndex.equals(targetIndex))
            || (!caseSensitive && sourceIndex.equalsIgnoreCase(targetIndex))) {
          table.addIndex(targetIndex);
        }
      }
    }

    for (int idx = 0; idx < targetTable.getCheckCount(); idx++) {
      Check targetCheck = targetTable.getCheck(idx);
      Check sourceCheck = sourceTable.findCheck(targetCheck.getName(), caseSensitive);

      if (sourceCheck != null) {
        if ((caseSensitive && sourceCheck.equals(targetCheck))
            || (!caseSensitive && sourceCheck.equalsIgnoreCase(targetCheck))) {
          table.addCheck(targetCheck);
        }
      }
    }

    return table;
  }

  /**
   * Writes a statement that copies the data from the source to the target table. Note that this
   * copies only those columns that are in both tables. Database-specific implementations might
   * redefine this method though they usually it suffices to redefine the
   * {@link #writeCastExpression(Column, Column)} method.
   * 
   * @param sourceTable
   *          The source table
   * @param targetTable
   *          The target table
   */
  protected void writeCopyDataStatement(Table sourceTable, Table targetTable) throws IOException {
    ListOrderedMap columns = new ListOrderedMap();

    for (int idx = 0; idx < sourceTable.getColumnCount(); idx++) {
      Column sourceColumn = sourceTable.getColumn(idx);
      Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
          getPlatform().isDelimitedIdentifierModeOn());

      if (targetColumn != null) {
        columns.put(sourceColumn, targetColumn);
      }
    }

    for (int idx = 0; idx < targetTable.getColumnCount(); idx++) {
      Column targetColumn = targetTable.getColumn(idx);
      if (targetColumn.getOnCreateDefault() != null
          && sourceTable.findColumn(targetColumn.getName()) == null) {
        columns.put(targetColumn, null);
      }
    }
    printScriptOptions("CRITICAL = TRUE");
    print("INSERT INTO ");
    printIdentifier(getStructureObjectName(targetTable));
    print(" (");
    for (Iterator columnIt = columns.keySet().iterator(); columnIt.hasNext();) {
      printIdentifier(getColumnName((Column) columnIt.next()));
      if (columnIt.hasNext()) {
        print(",");
      }
    }
    print(") SELECT ");
    for (Iterator columnsIt = columns.entrySet().iterator(); columnsIt.hasNext();) {
      Map.Entry entry = (Map.Entry) columnsIt.next();
      if (entry.getValue() != null) {
        writeCastExpression((Column) entry.getKey(), (Column) entry.getValue());
      } else {
        print("(" + ((Column) entry.getKey()).getOnCreateDefault() + ")");
      }
      if (columnsIt.hasNext()) {
        print(",");
      }
    }
    print(" FROM ");
    printIdentifier(getStructureObjectName(sourceTable));
    printEndOfStatement();
  }

  /**
   * Writes a cast expression that converts the value of the source column to the data type of the
   * target column. Per default, simply the name of the source column is written thereby assuming
   * that any casts happen implicitly.
   * 
   * @param sourceColumn
   *          The source column
   * @param targetColumn
   *          The target column
   */
  protected void writeCastExpression(Column sourceColumn, Column targetColumn) throws IOException {
    printIdentifier(getColumnName(sourceColumn));
  }

  /**
   * Processes the addition of a primary key to a table.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      AddPrimaryKeyChange change) throws IOException {
    if (!recreatedPKs.contains(change.getChangedTable().getName())) {
      writeExternalPrimaryKeysCreateStmt(change.getChangedTable(), change.getprimaryKeyName(),
          change.getPrimaryKeyColumns());
      recreatedPKs.add(change.getChangedTable().getName());
    }
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  protected void processChange(Database currentModel, Database desiredModel,
      ColumnOnCreateDefaultValueChange change) throws IOException {
    writeColumnCommentStmt(currentModel, change.getChangedTable(), change.getChangedColumn(),
        false);
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * Searches in the given table for a corresponding foreign key. If the given key has no name, then
   * a foreign key to the same table with the same columns in the same order is searched. If the
   * given key has a name, then the a corresponding key also needs to have the same name, or no name
   * at all, but not a different one.
   * 
   * @param table
   *          The table to search in
   * @param fk
   *          The original foreign key
   * @return The corresponding foreign key if found
   */
  protected ForeignKey findCorrespondingForeignKey(Table table, ForeignKey fk) {
    boolean caseMatters = getPlatform().isDelimitedIdentifierModeOn();
    boolean checkFkName = (fk.getName() != null) && (fk.getName().length() > 0);
    Reference[] refs = fk.getReferences();
    ArrayList curRefs = new ArrayList();

    for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
      ForeignKey curFk = table.getForeignKey(fkIdx);
      boolean checkCurFkName = checkFkName && (curFk.getName() != null)
          && (curFk.getName().length() > 0);

      if ((!checkCurFkName || areEqual(fk.getName(), curFk.getName(), caseMatters))
          && areEqual(fk.getForeignTableName(), curFk.getForeignTableName(), caseMatters)) {
        curRefs.clear();
        CollectionUtils.addAll(curRefs, curFk.getReferences());

        // the order is not fixed, so we have to take this long way
        if (curRefs.size() == refs.length) {
          for (int refIdx = 0; refIdx < refs.length; refIdx++) {
            boolean found = false;

            for (int curRefIdx = 0; !found && (curRefIdx < curRefs.size()); curRefIdx++) {
              Reference curRef = (Reference) curRefs.get(curRefIdx);

              if ((caseMatters && refs[refIdx].equals(curRef))
                  || (!caseMatters && refs[refIdx].equalsIgnoreCase(curRef))) {
                curRefs.remove(curRefIdx);
                found = true;
              }
            }
          }
          if (curRefs.isEmpty()) {
            return curFk;
          }
        }
      }
    }
    return null;
  }

  /**
   * Compares the two strings.
   * 
   * @param string1
   *          The first string
   * @param string2
   *          The second string
   * @param caseMatters
   *          Whether case matters in the comparison
   * @return <code>true</code> if the string are equal
   */
  protected boolean areEqual(String string1, String string2, boolean caseMatters) {
    return (caseMatters && string1.equals(string2))
        || (!caseMatters && string1.equalsIgnoreCase(string2));
  }

  /**
   * Outputs the DDL to create the table along with any non-external constraints as well as with
   * external primary keys and indices (but not foreign keys).
   * 
   * @param database
   *          The database model
   * @param table
   *          The table
   */
  public void createTable(Database database, Table table) throws IOException {
    createTable(database, table, null);
  }

  /**
   * Outputs the DDL to create the table along with any non-external constraints as well as with
   * external primary keys and indices (but not foreign keys).
   * 
   * @param database
   *          The database model
   * @param table
   *          The table
   * @param parameters
   *          Additional platform-specific parameters for the table creation
   */
  public void createTable(Database database, Table table, Map parameters) throws IOException {
    writeTableCreationStmt(database, table, parameters);
    writeTableCreationStmtEnding(table, parameters);

    if (!getPlatformInfo().isPrimaryKeyEmbedded()) {
      writeExternalPrimaryKeysCreateStmt(table, table.getPrimaryKey(),
          table.getPrimaryKeyColumns());
    }
    if (!getPlatformInfo().isIndicesEmbedded()) {
      writeExternalUniquesCreateStmt(table);
    }
    if (!getPlatformInfo().isIndicesEmbedded()) {
      // writeExternalIndicesCreateStmt(table);
    }
    if (!getPlatformInfo().isChecksEmbedded()) {
      writeExternalChecksCreateStmt(table);
    }
  }

  public void writeTableCommentsStmt(Database database, Table table) throws IOException {

  }

  public void writeColumnCommentStmt(Database database, Table table, Column column,
      boolean keepComments) throws IOException {

  }

  /**
   * Creates the external foreignkey creation statements for all tables in the database.
   * 
   * @param database
   *          The database
   */
  public void createExternalForeignKeys(Database database) throws IOException {
    for (int idx = 0; idx < database.getTableCount(); idx++) {
      createExternalForeignKeys(database, database.getTable(idx));
    }
  }

  /**
   * Creates external foreignkey creation statements if necessary.
   * 
   * @param database
   *          The database model
   * @param table
   *          The table
   */
  public void createExternalForeignKeys(Database database, Table table) throws IOException {
    createExternalForeignKeys(database, table, false);
  }

  public void createExternalForeignKeys(Database database, Table table, boolean skipRecreated)
      throws IOException {
    if (!getPlatformInfo().isForeignKeysEmbedded()) {
      for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
        ForeignKey fk = table.getForeignKey(idx);
        if (!skipRecreated || !recreatedTables.contains(fk.getForeignTableName())) {
          writeExternalForeignKeyCreateStmt(database, table, fk);
        }
      }
    }
  }

  /**
   * Outputs the DDL required to drop the database.
   * 
   * @param database
   *          The database
   */
  public void dropTables(Database database) throws IOException {
    // we're dropping the external foreignkeys first
    for (int idx = database.getTableCount() - 1; idx >= 0; idx--) {
      Table table = database.getTable(idx);

      if ((table.getName() != null) && (table.getName().length() > 0)) {
        dropExternalForeignKeys(table);
      }
    }

    // Drop Triggers
    for (int idx = 0; idx < database.getTriggerCount(); idx++) {
      Trigger trg = database.getTrigger(idx);
      if ((trg.getName() != null) && (trg.getName().length() > 0)) {
        dropTrigger(database, trg);
      }
    }

    // Drop Views
    for (int idx = 0; idx < database.getViewCount(); idx++) {
      View view = database.getView(idx);
      if ((view.getName() != null) && (view.getName().length() > 0)) {
        dropView(view);
      }
    }

    // Drop Functions
    for (int idx = 0; idx < database.getFunctionCount(); idx++) {
      Function fun = database.getFunction(idx);
      if ((fun.getName() != null) && (fun.getName().length() > 0)) {
        dropFunction(fun);
      }
    }

    // Next we drop the tables in reverse order to avoid referencial
    // problems
    // TODO: It might be more useful to either (or both)
    // * determine an order in which the tables can be dropped safely (via
    // the foreignkeys)
    // * alter the tables first to drop the internal foreignkeys
    for (int idx = database.getTableCount() - 1; idx >= 0; idx--) {
      Table table = database.getTable(idx);

      if ((table.getName() != null) && (table.getName().length() > 0)) {
        dropTable(table);
      }
    }

    // Drop Sequences
    for (int idx = 0; idx < database.getSequenceCount(); idx++) {
      Sequence seq = database.getSequence(idx);
      if ((seq.getName() != null) && (seq.getName().length() > 0)) {
        dropSequence(seq);
      }
    }

  }

  /**
   * Outputs the DDL required to drop the given table. This method also drops foreign keys to the
   * table.
   * 
   * @param database
   *          The database
   * @param table
   *          The table
   */
  public void dropTable(Database database, Table table) throws IOException {
    // we're dropping the foreignkeys to the table first
    for (int idx = database.getTableCount() - 1; idx >= 0; idx--) {
      Table otherTable = database.getTable(idx);
      ForeignKey[] fks = otherTable.getForeignKeys();

      for (int fkIdx = 0; (fks != null) && (fkIdx < fks.length); fkIdx++) {
        if (fks[fkIdx].getForeignTable().equals(table)) {
          writeExternalForeignKeyDropStmt(otherTable, fks[fkIdx]);
        }
      }
    }
    // and the foreign keys from the table
    dropExternalForeignKeys(table);

    dropTable(table);
  }

  /**
   * Outputs the DDL to drop the table. Note that this method does not drop foreign keys to this
   * table. Use {@link #dropTable(Database, Table)} if you want that.
   * 
   * @param table
   *          The table to drop
   */
  public void dropTable(Table table) throws IOException {
    printStartOfStatement("TABLE", getStructureObjectName(table));

    print("DROP TABLE ");
    printIdentifier(getStructureObjectName(table));

    printEndOfStatement(getStructureObjectName(table));
  }

  /**
   * Creates external foreignkey drop statements.
   * 
   * @param table
   *          The table
   */
  public void dropExternalForeignKeys(Table table) throws IOException {
    if (!getPlatformInfo().isForeignKeysEmbedded()) {
      for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
        writeExternalForeignKeyDropStmt(table, table.getForeignKey(idx));
      }
    }
  }

  /**
   * Creates the SQL for inserting an object into the specified table. If values are given then a
   * concrete insert statement is created, otherwise an insert statement usable in a prepared
   * statement is build.
   * 
   * @param table
   *          The table
   * @param columnValues
   *          The columns values indexed by the column names
   * @param genPlaceholders
   *          Whether to generate value placeholders for a prepared statement
   * @return The insertion sql
   */
  public String getInsertSql(Table table, Map columnValues, boolean genPlaceholders) {
    StringBuffer buffer = new StringBuffer("INSERT INTO ");
    boolean addComma = false;

    buffer.append(getDelimitedIdentifier(getStructureObjectName(table)));
    buffer.append(" (");

    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      Column column = table.getColumn(idx);

      if (columnValues.containsKey(column.getName())) {
        if (addComma) {
          buffer.append(", ");
        }
        buffer.append(getDelimitedIdentifier(column.getName()));
        addComma = true;
      }
    }
    buffer.append(") VALUES (");
    if (genPlaceholders) {
      addComma = false;
      for (int idx = 0; idx < table.getColumnCount(); idx++) {
        Column column = table.getColumn(idx);

        if (columnValues.containsKey(column.getName())) {
          if (addComma) {
            buffer.append(", ");
          }
          buffer.append("?");
          addComma = true;
        }
      }
    } else {
      addComma = false;
      for (int idx = 0; idx < table.getColumnCount(); idx++) {
        Column column = table.getColumn(idx);

        if (columnValues.containsKey(column.getName())) {
          if (addComma) {
            buffer.append(", ");
          }
          buffer.append(getValueAsString(column, columnValues.get(column.getName())));
          addComma = true;
        }
      }
    }
    buffer.append(")");
    return buffer.toString();
  }

  /**
   * Creates the SQL for updating an object in the specified table. If values are given then a
   * concrete update statement is created, otherwise an update statement usable in a prepared
   * statement is build.
   * 
   * @param table
   *          The table
   * @param columnValues
   *          Contains the values for the columns to update, and should also contain the primary key
   *          values to identify the object to update in case <code>genPlaceholders</code> is
   *          <code>false</code>
   * @param genPlaceholders
   *          Whether to generate value placeholders for a prepared statement (both for the pk
   *          values and the object values)
   * @return The update sql
   */
  public String getUpdateSql(Table table, Map columnValues, boolean genPlaceholders) {
    StringBuffer buffer = new StringBuffer("UPDATE ");
    boolean addSep = false;

    buffer.append(getDelimitedIdentifier(getStructureObjectName(table)));
    buffer.append(" SET ");

    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      Column column = table.getColumn(idx);

      if (!column.isPrimaryKey() && columnValues.containsKey(column.getName())) {
        if (addSep) {
          buffer.append(", ");
        }
        buffer.append(getDelimitedIdentifier(column.getName()));
        buffer.append(" = ");
        if (genPlaceholders) {
          buffer.append("?");
        } else {
          buffer.append(getValueAsString(column, columnValues.get(column.getName())));
        }
        addSep = true;
      }
    }
    buffer.append(" WHERE ");
    addSep = false;
    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      Column column = table.getColumn(idx);

      if (column.isPrimaryKey() && columnValues.containsKey(column.getName())) {
        if (addSep) {
          buffer.append(" AND ");
        }
        buffer.append(getDelimitedIdentifier(column.getName()));
        buffer.append(" = ");
        if (genPlaceholders) {
          buffer.append("?");
        } else {
          buffer.append(getValueAsString(column, columnValues.get(column.getName())));
        }
        addSep = true;
      }
    }
    return buffer.toString();
  }

  /**
   * Creates the SQL for deleting an object from the specified table. If values are given then a
   * concrete delete statement is created, otherwise an delete statement usable in a prepared
   * statement is build.
   * 
   * @param table
   *          The table
   * @param pkValues
   *          The primary key values indexed by the column names, can be empty
   * @param genPlaceholders
   *          Whether to generate value placeholders for a prepared statement
   * @return The delete sql
   */
  public String getDeleteSql(Table table, Map pkValues, boolean genPlaceholders) {
    StringBuffer buffer = new StringBuffer("DELETE FROM ");
    boolean addSep = false;

    buffer.append(getDelimitedIdentifier(getStructureObjectName(table)));
    if ((pkValues != null) && !pkValues.isEmpty()) {
      buffer.append(" WHERE ");
      for (Iterator it = pkValues.entrySet().iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        Column column = table.findColumn((String) entry.getKey());

        if (addSep) {
          buffer.append(" AND ");
        }
        buffer.append(getDelimitedIdentifier(entry.getKey().toString()));
        buffer.append(" = ");
        if (genPlaceholders) {
          buffer.append("?");
        } else {
          buffer.append(
              column == null ? entry.getValue() : getValueAsString(column, entry.getValue()));
        }
        addSep = true;
      }
    }
    return buffer.toString();
  }

  /**
   * Generates the string representation of the given value.
   * 
   * @param column
   *          The column
   * @param value
   *          The value
   * @return The string representation
   */
  protected String getValueAsString(Column column, Object value) {
    if (value == null) {
      return "NULL";
    }

    StringBuffer result = new StringBuffer();

    // TODO: Handle binary types (BINARY, VARBINARY, LONGVARBINARY, BLOB)
    switch (column.getTypeCode()) {
      case Types.DATE:
        result.append(getPlatformInfo().getValueQuoteToken());
        if (!(value instanceof String) && (getValueDateFormat() != null)) {
          // TODO: Can the format method handle java.sql.Date properly ?
          result.append(getValueDateFormat().format(value));
        } else {
          result.append(value.toString());
        }
        result.append(getPlatformInfo().getValueQuoteToken());
        break;
      case Types.TIME:
        result.append(getPlatformInfo().getValueQuoteToken());
        if (!(value instanceof String) && (getValueTimeFormat() != null)) {
          // TODO: Can the format method handle java.sql.Date properly ?
          result.append(getValueTimeFormat().format(value));
        } else {
          result.append(value.toString());
        }
        result.append(getPlatformInfo().getValueQuoteToken());
        break;
      case Types.TIMESTAMP:
        if (value.toString().equals("now()")) {
          result.append("now()");
          break;
        }
        result.append("to_date(");
        result.append(getPlatformInfo().getValueQuoteToken());
        // TODO: SimpleDateFormat does not support nano seconds so we would
        // need a custom date formatter for timestamps
        result.append(value.toString().substring(0, value.toString().length() - 2));
        result.append(getPlatformInfo().getValueQuoteToken());
        result.append(",'YYYY-MM-DD HH24:MI:SS')");
        break;
      case Types.REAL:
      case Types.NUMERIC:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.DECIMAL:
        result.append(getPlatformInfo().getValueQuoteToken());
        if (!(value instanceof String) && (getValueNumberFormat() != null)) {
          result.append(getValueNumberFormat().format(value));
        } else {
          result.append(value.toString());
        }
        result.append(getPlatformInfo().getValueQuoteToken());
        break;
      default:
        result.append(getPlatformInfo().getValueQuoteToken());
        result.append(escapeStringValue(value.toString()));
        result.append(getPlatformInfo().getValueQuoteToken());
        break;
    }
    return result.toString();
  }

  /**
   * Generates the SQL for querying the id that was created in the last insertion operation. This is
   * obviously only useful for pk fields that are auto-incrementing. A database that does not
   * support this, will return <code>null</code>.
   * 
   * @param table
   *          The table
   * @return The sql, or <code>null</code> if the database does not support this
   */
  public String getSelectLastIdentityValues(Table table) {
    // No default possible as the databases are quite different in this
    // respect
    return null;
  }

  //
  // implementation methods that may be overridden by specific database
  // builders
  //

  /**
   * Generates a version of the name that has at most the specified length.
   * 
   * @param name
   *          The original name
   * @param desiredLength
   *          The desired maximum length
   * @return The shortened version
   */
  public String shortenName(String name, int desiredLength) {
    // TODO: Find an algorithm that generates unique names
    int originalLength = name.length();

    if ((desiredLength <= 0) || (originalLength <= desiredLength)) {
      return name;
    }
    _log.warn("Warning: Name of object " + name + " is too long (" + originalLength
        + " characters; maximum length: " + desiredLength + " characters)");
    int delta = originalLength - desiredLength;
    int startCut = desiredLength / 2;

    StringBuffer result = new StringBuffer();

    result.append(name.substring(0, startCut));
    if (((startCut == 0) || (name.charAt(startCut - 1) != '_'))
        && ((startCut + delta + 1 == originalLength)
            || (name.charAt(startCut + delta + 1) != '_'))) {
      // just to make sure that there isn't already a '_' right before or
      // right
      // after the cutting place (which would look odd with an aditional
      // one)
      result.append("_");
    }
    result.append(name.substring(startCut + delta + 1, originalLength));
    return result.toString();
  }

  /**
   * Outputs the DDL required to delete data from the given table.
   * 
   * @param database
   *          The database
   * @param table
   *          The table
   */
  public void writeDeleteTable(Database database, Table table) throws IOException {
    print("DELETE FROM ");
    printIdentifier(getStructureObjectName(table));
    printEndOfStatement();
  }

  public void writeDeleteTable(Database database, String table, String sqlfilter)
      throws IOException {
    print("DELETE FROM ");
    printIdentifier(table);
    if (!"".equals(sqlfilter)) {
      print(" WHERE ");
      print(sqlfilter);
    }
    printEndOfStatement();
  }

  /**
   * Generates the first part of the ALTER TABLE statement including the table name.
   * 
   * @param table
   *          The table being altered
   */
  protected void writeTableAlterStmt(Table table) throws IOException {
    print("ALTER TABLE ");
    printlnIdentifier(getStructureObjectName(table));
    printIndent();
  }

  /**
   * Writes the table creation statement without the statement end.
   * 
   * @param database
   *          The model
   * @param table
   *          The table
   * @param parameters
   *          Additional platform-specific parameters for the table creation
   */
  protected void writeTableCreationStmt(Database database, Table table,
      Map<String, Object> parameters) throws IOException {
    printStartOfStatement("TABLE", getStructureObjectName(table));
    printScriptOptions("CRITICAL = TRUE");

    print("CREATE TABLE ");
    printlnIdentifier(getStructureObjectName(table));
    println("(");

    // optimize null handling (used in create database)
    // if requested do not mark any columns as not null
    if (parameters != null && parameters.containsKey("OB_OptimizeNotNull")) {
      try {
        Table clonedTable = (Table) table.clone();
        for (Column col : clonedTable.getColumns()) {
          col.setRequired(false);
        }
        writeColumns(clonedTable);
      } catch (CloneNotSupportedException e) {
        // will not happen as clone() is implemented on Table class
      }
    } else {
      writeColumns(table);
    }

    if (getPlatformInfo().isPrimaryKeyEmbedded()) {
      // writeEmbeddedPrimaryKeysStmt(table);
    }
    if (getPlatformInfo().isForeignKeysEmbedded()) {
      writeEmbeddedForeignKeysStmt(database, table);
    }
    if (getPlatformInfo().isIndicesEmbedded()) {
      writeEmbeddedUniquesStmt(table);
    }
    if (getPlatformInfo().isIndicesEmbedded()) {
      writeEmbeddedIndicesStmt(table);
    }
    if (getPlatformInfo().isChecksEmbedded()) {
      writeEmbeddedChecksStmt(table);
    }
    println();
    print(")");
  }

  /**
   * Writes the end of the table creation statement. Per default, only the end of the statement is
   * written, but this can be changed in subclasses.
   * 
   * @param table
   *          The table
   * @param parameters
   *          Additional platform-specific parameters for the table creation
   */
  protected void writeTableCreationStmtEnding(Table table, Map parameters) throws IOException {
    printEndOfStatement(getStructureObjectName(table));
  }

  /**
   * Writes the columns of the given table.
   * 
   * @param table
   *          The table
   */
  protected void writeColumns(Table table) throws IOException {
    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      printIndent();
      writeColumn(table, table.getColumn(idx));
      if (idx < table.getColumnCount() - 1) {
        println(",");
      }
    }
  }

  /**
   * Returns the column name. This method takes care of length limitations imposed by some
   * databases.
   * 
   * @param column
   *          The column
   * @return The column name
   */
  protected String getColumnName(Column column) throws IOException {
    return shortenName(column.getName(), getMaxColumnNameLength());
  }

  /**
   * Outputs the DDL for the specified column.
   * 
   * @param table
   *          The table containing the column
   * @param column
   *          The column
   */
  protected void writeColumn(Table table, Column column) throws IOException {
    writeColumn(table, column, false);
  }

  /**
   * Outputs the DDL for the specified column.
   * 
   * @param table
   *          The table containing the column
   * @param column
   *          The column
   * @param delayNotNull
   *          if true, not null will not be created at this point, it will require of later
   *          management
   */
  protected void writeColumn(Table table, Column column, boolean deferNotNull) throws IOException {
    // see comments in columnsDiffer about null/"" defaults

    writeColumnType(column);

    String value;
    String onCreateDefault = column.getLiteralOnCreateDefault();
    if ((!createdTables.contains(table.getName()) && !recreatedTables.contains(table.getName()))
        && onCreateDefault != null) {
      value = onCreateDefault;
    } else {
      value = getDefaultValue(column);
    }

    if (value != null) {
      print(" DEFAULT ");
      print(value);
    }

    if (column.isRequired()) {
      if (!deferNotNull) {
        print(" ");
        writeColumnNotNullableStmt();
      }
    } else if (getPlatformInfo().isNullAsDefaultValueRequired()
        && getPlatformInfo().hasNullDefault(column.getTypeCode())) {
      print(" ");
      writeColumnNullableStmt();
    }
  }

  protected void writeColumnType(Column column) throws IOException {
    printIdentifier(getColumnName(column));
    print(" ");
    print(getSqlType(column));
  }

  /**
   * Returns the full SQL type specification (including size and precision/scale) for the given
   * column.
   * 
   * @param column
   *          The column
   * @return The full SQL type string including the size
   */
  protected String getSqlType(Column column) {
    String nativeType = getNativeType(column);
    int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);
    StringBuffer sqlType = new StringBuffer();

    sqlType.append(sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType);

    Object sizeSpec = column.getSize();

    if (sizeSpec == null) {
      sizeSpec = getPlatformInfo().getDefaultSize(column.getTypeCode());
    }
    if (sizeSpec != null) {
      if (getPlatformInfo().hasSize(column.getTypeCode())) {
        sqlType.append("(");
        sqlType.append(sizeSpec.toString());
        sqlType.append(")");
      } else if (getPlatformInfo().hasPrecisionAndScale(column.getTypeCode())) {
        sqlType.append("(");
        sqlType.append(column.getSizeAsInt());
        if (column.getScale() != null) {
          sqlType.append(",");
          sqlType.append(column.getScale());
        }
        sqlType.append(")");
      }
    }
    sqlType.append(sizePos >= 0 ? nativeType.substring(sizePos + SIZE_PLACEHOLDER.length()) : "");

    return sqlType.toString();
  }

  /**
   * Returns the database-native type for the given column.
   * 
   * @param column
   *          The column
   * @return The native type
   */
  protected String getNativeType(Column column) {
    String nativeType = getPlatformInfo().getNativeType(column.getTypeCode());

    return nativeType == null ? column.getType() : nativeType;
  }

  /**
   * Returns the bare database-native type for the given column without any size specifies.
   * 
   * @param column
   *          The column
   * @return The native type
   */
  protected String getBareNativeType(Column column) {
    String nativeType = getNativeType(column);
    int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);

    return sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType;
  }

  /**
   * Returns the native default value for the column.
   * 
   * @param column
   *          The column
   * @return The native default value
   */
  protected String getNativeDefaultValue(ValueObject column) {
    return column.getDefaultValue();
  }

  /**
   * Returns the native function for the neutral function.
   * 
   * @param neutralFunction
   *          The neutral function
   * @param typeCode
   *          The return type of the function
   * @return The native function
   */
  protected String getNativeFunction(String neutralFunction, int typeCode) throws IOException {
    return neutralFunction;
  }

  /**
   * Escapes the necessary characters in given string value.
   * 
   * @param value
   *          The value
   * @return The corresponding string with the special characters properly escaped
   */
  protected String escapeStringValue(String value) {
    String result = value;

    for (Iterator it = _charSequencesToEscape.entrySet().iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();

      result = StringUtils.replace(result, (String) entry.getKey(), (String) entry.getValue());
    }
    return result;
  }

  /**
   * Determines whether the given default spec is a non-empty spec that shall be used in a DEFAULT
   * expression. E.g. if the spec is an empty string and the type is a numeric type, then it is no
   * valid default value whereas if it is a string type, then it is valid.
   * 
   * @param defaultSpec
   *          The default value spec
   * @param typeCode
   *          The JDBC type code
   * @return <code>true</code> if the default value spec is valid
   */
  protected boolean isValidDefaultValue(String defaultSpec, int typeCode) {
    return (defaultSpec != null) && ((defaultSpec.length() > 0)
        || (!TypeMap.isNumericType(typeCode) && !TypeMap.isDateTimeType(typeCode)));
  }

  /**
   * Prints the default value of the column.
   * 
   * @param defaultValue
   *          The default value
   * @param typeCode
   *          The type code to write the default value for
   */
  protected String getDefaultValue(Object defaultValue, int typeCode) throws IOException {
    if (defaultValue == null) {
      return null;
    } else {
      boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode);

      if (shouldUseQuotes) {
        // characters are only escaped when within a string literal
        return getPlatformInfo().getValueQuoteToken() + escapeStringValue(defaultValue.toString())
            + getPlatformInfo().getValueQuoteToken();
      } else {
        return defaultValue.toString();
      }
    }
  }

  /**
   * Prints that the column is an auto increment column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   */
  protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException {
    print("IDENTITY");
  }

  /**
   * Prints that a column is nullable.
   */
  protected void writeColumnNullableStmt() throws IOException {
    print("NULL");
  }

  /**
   * Prints that a column is not nullable.
   */
  protected void writeColumnNotNullableStmt() throws IOException {
    print("NOT NULL");
  }

  /**
   * Compares the current column in the database with the desired one. Type, nullability, size,
   * scale, default value, and precision radix are the attributes checked. Currently default values
   * are compared, and null and empty string are considered equal.
   * 
   * @param currentColumn
   *          The current column as it is in the database
   * @param desiredColumn
   *          The desired column
   * @return <code>true</code> if the column specifications differ
   */
  protected boolean columnsDiffer(Column currentColumn, Column desiredColumn) {
    // The createColumn method leaves off the default clause if
    // column.getDefaultValue()
    // is null. mySQL interprets this as a default of "" or 0, and thus the
    // columns
    // are always different according to this method. alterDatabase will
    // generate
    // an alter statement for the column, but it will be the exact same
    // definition
    // as before. In order to avoid this situation I am ignoring the
    // comparison
    // if the desired default is null. In order to "un-default" a column
    // you'll
    // have to have a default="" or default="0" in the schema xml.
    // If this is bad for other databases, it is recommended that the
    // createColumn
    // method use a "DEFAULT NULL" statement if that is what is needed.
    // A good way to get this would be to require a defaultValue="<NULL>" in
    // the
    // schema xml if you really want null and not just unspecified.

    String desiredDefault = desiredColumn.getDefaultValue();
    String currentDefault = currentColumn.getDefaultValue();
    boolean defaultsEqual = (desiredDefault == null) || desiredDefault.equals(currentDefault);
    boolean sizeMatters = getPlatformInfo().hasSize(currentColumn.getTypeCode())
        && (desiredColumn.getSize() != null);

    // We're comparing the jdbc type that corresponds to the native type for
    // the
    // desired type, in order to avoid repeated altering of a perfectly
    // valid column
    if ((getPlatformInfo().getTargetJdbcType(desiredColumn.getTypeCode()) != currentColumn
        .getTypeCode()) || (desiredColumn.isRequired() != currentColumn.isRequired())
        || (sizeMatters && !StringUtils.equals(desiredColumn.getSize(), currentColumn.getSize()))
        || !defaultsEqual) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns the name to be used for the given foreign key. If the foreign key has no specified
   * name, this method determines a unique name for it. The name will also be shortened to honor the
   * maximum identifier length imposed by the platform.
   * 
   * @param table
   *          The table for whith the foreign key is defined
   * @param fk
   *          The foreign key
   * @return The name
   */
  public String getForeignKeyName(Table table, ForeignKey fk) {
    String fkName = fk.getName();
    boolean needsName = (fkName == null) || (fkName.length() == 0);

    if (needsName) {
      StringBuffer name = new StringBuffer();

      for (int idx = 0; idx < fk.getReferenceCount(); idx++) {
        name.append(fk.getReference(idx).getLocalColumnName());
        name.append("_");
      }
      name.append(fk.getForeignTableName());
      fkName = getConstraintName(null, table, "FK", name.toString());
    }
    fkName = shortenName(fkName, getMaxForeignKeyNameLength());

    if (needsName) {
      _log.warn("Encountered a foreign key in table " + table.getName() + " that has no name. "
          + "DdlUtils will use the auto-generated and shortened name " + fkName + " instead.");
    }

    return fkName;
  }

  /**
   * Returns the constraint name. This method takes care of length limitations imposed by some
   * databases.
   * 
   * @param prefix
   *          The constraint prefix, can be <code>null</code>
   * @param table
   *          The table that the constraint belongs to
   * @param secondPart
   *          The second name part, e.g. the name of the constraint column
   * @param suffix
   *          The constraint suffix, e.g. a counter (can be <code>null</code>)
   * @return The constraint name
   */
  public String getConstraintName(String prefix, Table table, String secondPart, String suffix) {
    StringBuffer result = new StringBuffer();

    if (prefix != null) {
      result.append(prefix);
      result.append("_");
    }
    result.append(table.getName());
    result.append("_");
    result.append(secondPart);
    if (suffix != null) {
      result.append("_");
      result.append(suffix);
    }
    return shortenName(result.toString(), getMaxConstraintNameLength());
  }

  /**
   * Writes the primary key constraints of the table inside its definition.
   * 
   * @param table
   *          The table
   */
  protected void writeEmbeddedPrimaryKeysStmt(Table table) throws IOException {
    Column[] primaryKeyColumns = table.getPrimaryKeyColumns();

    if ((primaryKeyColumns.length > 0) && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
      printStartOfEmbeddedStatement();
      if (table.getPrimaryKey() != null && !table.getPrimaryKey().equals("")) {
        print("CONSTRAINT ");
        printIdentifier(table.getPrimaryKey());
        print(" ");
      }
      writePrimaryKeyStmt(table, primaryKeyColumns);
    }
  }

  /**
   * Writes the primary key constraints of the table as alter table statements.
   * 
   * @param table
   *          The table
   * @param primaryKeyColumns
   *          The primary key columns
   */
  protected void writeExternalPrimaryKeysCreateStmt(Table table, String primaryKeyName,
      Column[] primaryKeyColumns) throws IOException {
    if ((primaryKeyColumns.length > 0) && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
      print("ALTER TABLE ");
      printlnIdentifier(getStructureObjectName(table));
      printIndent();
      print("ADD CONSTRAINT ");

      if (primaryKeyName == null || primaryKeyName.equals("")) {
        printIdentifier(getConstraintName(null, table, "PK", null));
      } else {
        printIdentifier(primaryKeyName);
      }

      print(" ");
      writePrimaryKeyStmt(table, primaryKeyColumns);
      printEndOfStatement();
    }
  }

  /**
   * Determines whether we should generate a primary key constraint for the given primary key
   * columns.
   * 
   * @param primaryKeyColumns
   *          The pk columns
   * @return <code>true</code> if a pk statement should be generated for the columns
   */
  protected boolean shouldGeneratePrimaryKeys(Column[] primaryKeyColumns) {
    return true;
  }

  /**
   * Writes a primary key statement for the given columns.
   * 
   * @param table
   *          The table
   * @param primaryKeyColumns
   *          The primary columns
   */
  protected void writePrimaryKeyStmt(Table table, Column[] primaryKeyColumns) throws IOException {

    print("PRIMARY KEY (");
    for (int idx = 0; idx < primaryKeyColumns.length; idx++) {
      printIdentifier(getColumnName(primaryKeyColumns[idx]));
      if (idx < primaryKeyColumns.length - 1) {
        print(", ");
      }
    }
    print(")");
  }

  /**
   * Writes the uniques of the given table.
   * 
   * @param table
   *          The table
   */
  protected void writeExternalUniquesCreateStmt(Table table) throws IOException {
    for (int idx = 0; idx < table.getUniqueCount(); idx++) {
      Unique unique = table.getUnique(idx);
      writeExternalUniqueCreateStmt(table, unique);
    }
  }

  /**
   * Writes the uniques embedded within the create table statement.
   * 
   * @param table
   *          The table
   */
  protected void writeEmbeddedUniquesStmt(Table table) throws IOException {
    if (getPlatformInfo().isIndicesSupported()) {
      for (int idx = 0; idx < table.getUniqueCount(); idx++) {
        printStartOfEmbeddedStatement();
        writeEmbeddedUniqueCreateStmt(table, table.getUnique(idx));
      }
    }
  }

  /**
   * Writes the given unique of the table.
   * 
   * @param table
   *          The table
   * @param unique
   *          The unique
   */
  protected void writeExternalUniqueCreateStmt(Table table, Unique unique) throws IOException {
    if (getPlatformInfo().isIndicesSupported()) {
      if (unique.getName() == null) {
        _log.warn("Cannot write unnamed unique " + unique);
      } else {
        print("ALTER TABLE ");
        printIdentifier(getStructureObjectName(table));
        print(" ADD CONSTRAINT ");
        printIdentifier(getConstraintObjectName(unique));
        print(" UNIQUE (");

        for (int idx = 0; idx < unique.getColumnCount(); idx++) {
          IndexColumn idxColumn = unique.getColumn(idx);
          Column col = table.findColumn(idxColumn.getName());

          if (col == null) {
            // would get null pointer on next line anyway, so throw
            // exception
            throw new ModelException("Invalid column '" + idxColumn.getName() + "' on unique "
                + unique.getName() + " for table " + table.getName());
          }
          if (idx > 0) {
            print(", ");
          }
          printIdentifier(getColumnName(col));
        }

        print(")");
        printEndOfStatement();
      }
    }
  }

  /**
   * Writes the given embedded unique of the table.
   * 
   * @param table
   *          The table
   * @param unique
   *          The unique
   */
  protected void writeEmbeddedUniqueCreateStmt(Table table, Unique unique) throws IOException {
    if ((unique.getName() != null) && (unique.getName().length() > 0)) {
      print(" CONSTRAINT ");
      printIdentifier(getConstraintObjectName(unique));
    }
    print(" UNIQUE (");

    for (int idx = 0; idx < unique.getColumnCount(); idx++) {
      IndexColumn idxColumn = unique.getColumn(idx);
      Column col = table.findColumn(idxColumn.getName());

      if (col == null) {
        // would get null pointer on next line anyway, so throw
        // exception
        throw new ModelException("Invalid column '" + idxColumn.getName() + "' on unique "
            + unique.getName() + " for table " + table.getName());
      }
      if (idx > 0) {
        print(", ");
      }
      printIdentifier(getColumnName(col));
    }

    print(")");
  }

  /**
   * Generates the statement to drop a non-embedded unique from the database.
   * 
   * @param table
   *          The table the unique is on
   * @param unique
   *          The unique to drop
   */
  public void writeExternalUniqueDropStmt(Table table, Unique unique) throws IOException {
    print("ALTER TABLE ");
    printIdentifier(getStructureObjectName(table));
    print(" DROP CONSTRAINT ");
    printIdentifier(getConstraintObjectName(unique));
    printEndOfStatement();
  }

  /**
   * Writes the indexes of the given table.
   * 
   * @param table
   *          The table
   */
  protected void writeExternalIndicesCreateStmt(Table table) throws IOException {
    for (int idx = 0; idx < table.getIndexCount(); idx++) {
      Index index = table.getIndex(idx);

      if (!index.isUnique() && !getPlatformInfo().isIndicesSupported()) {
        throw new ModelException("Platform does not support non-unique indices");
      }
      writeExternalIndexCreateStmt(table, index);
    }
    newIndexesPostAction(table, table.getIndices());
  }

  /**
   * Writes the indexes embedded within the create table statement.
   * 
   * @param table
   *          The table
   */
  protected void writeEmbeddedIndicesStmt(Table table) throws IOException {
    if (getPlatformInfo().isIndicesSupported()) {
      for (int idx = 0; idx < table.getIndexCount(); idx++) {
        printStartOfEmbeddedStatement();
        writeEmbeddedIndexCreateStmt(table, table.getIndex(idx));
      }
    }
  }

  /**
   * Writes the given index of the table.
   * 
   * @param table
   *          The table
   * @param index
   *          The index
   */
  protected void writeExternalIndexCreateStmt(Table table, Index index) throws IOException {
    if (getPlatformInfo().isIndicesSupported()) {
      if (index.getName() == null) {
        _log.warn("Cannot write unnamed index " + index);
      } else {
        print("CREATE");
        if (index.isUnique()) {
          print(" UNIQUE");
        }
        print(" INDEX ");
        printIdentifier(getConstraintObjectName(index));
        print(" ON ");
        printIdentifier(getStructureObjectName(table));
        writeMethod(index);
        print(" (");

        List<IndexColumn> columnsWithOperatorClass = new ArrayList<IndexColumn>();
        for (int idx = 0; idx < index.getColumnCount(); idx++) {
          if (idx > 0) {
            print(", ");
          }
          IndexColumn idxColumn = index.getColumn(idx);
          if ("functionBasedColumn".equals(idxColumn.getName())) {
            // print the expression instead of just the column name, surround it with extra
            // parenthesis to support ORA-PG compatibility
            print("(" + idxColumn.getFunctionExpression() + ")");
          } else {
            Column col = table.findColumn(idxColumn.getName());

            if (col == null) {
              // would get null pointer on next line anyway, so throw
              // exception
              throw new ModelException("Invalid column '" + idxColumn.getName() + "' on index "
                  + index.getName() + " for table " + table.getName());
            }
            printIdentifier(getColumnName(col));
          }
          if (idxColumn.getOperatorClass() != null && !idxColumn.getOperatorClass().isEmpty()) {
            // Store the index columns that define an operator class, as they will have to be
            // included in the comments of the table in Oracle
            columnsWithOperatorClass.add(idxColumn);
          }
          writeOperatorClass(index, idxColumn);
        }

        print(")");
        // Add the where clause (if defined) for partial indexing
        writeWhereClause(index);
        printEndOfStatement();
      }
    }
  }

  /**
   * Writes the access method used by the index
   * 
   * @param index
   *          the index
   * @throws IOException
   */
  protected void writeMethod(Index index) throws IOException {
  }

  /**
   * Writes the operator class of the index column, if any.
   * 
   * @param index
   *          the index owner of the index column, it can be used to retrieve information about the
   *          index itself
   * @param idxColumn
   *          the index column
   * @throws IOException
   */
  protected void writeOperatorClass(Index index, IndexColumn idxColumn) throws IOException {
  }

  /**
   * Writes the where clause of a partial index, if any
   * 
   * @param index
   *          the index
   * @throws IOException
   */
  protected void writeWhereClause(Index index) throws IOException {
  }

  /**
   * Writes the given embedded index of the table.
   * 
   * @param table
   *          The table
   * @param index
   *          The index
   */
  protected void writeEmbeddedIndexCreateStmt(Table table, Index index) throws IOException {
    if ((index.getName() != null) && (index.getName().length() > 0)) {
      print(" CONSTRAINT ");
      printIdentifier(getConstraintObjectName(index));
    }
    if (index.isUnique()) {
      print(" UNIQUE");
    } else {
      print(" INDEX ");
    }
    print(" (");

    for (int idx = 0; idx < index.getColumnCount(); idx++) {
      IndexColumn idxColumn = index.getColumn(idx);
      Column col = table.findColumn(idxColumn.getName());

      if (col == null) {
        // would get null pointer on next line anyway, so throw
        // exception
        throw new ModelException("Invalid column '" + idxColumn.getName() + "' on index "
            + index.getName() + " for table " + table.getName());
      }
      if (idx > 0) {
        print(", ");
      }
      printIdentifier(getColumnName(col));
    }

    print(")");
  }

  /**
   * Generates the statement to drop a non-embedded index from the database.
   * 
   * @param table
   *          The table the index is on
   * @param index
   *          The index to drop
   */
  public void writeExternalIndexDropStmt(Table table, Index index) throws IOException {
    if (getPlatformInfo().isAlterTableForDropUsed()) {
      writeTableAlterStmt(table);
    }
    print("DROP INDEX ");
    printIdentifier(getConstraintObjectName(index));
    if (!getPlatformInfo().isAlterTableForDropUsed()) {
      print(" ON ");
      printIdentifier(getStructureObjectName(table));
    }
    printEndOfStatement();
  }

  /**
   * Checks whether any column of the provided index defines an operator class
   * 
   * @param index
   *          the index that will be checked
   * @return true if any columns if the provided index defines an operator class, false otherwise
   */
  protected boolean indexHasColumnWithOperatorClass(Index index) {
    for (IndexColumn indexColumn : index.getColumns()) {
      if (indexColumn.getOperatorClass() != null && !indexColumn.getOperatorClass().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Writes the foreign key constraints inside a create table () clause.
   * 
   * @param database
   *          The database model
   * @param table
   *          The table
   */
  protected void writeEmbeddedForeignKeysStmt(Database database, Table table) throws IOException {
    for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
      ForeignKey key = table.getForeignKey(idx);

      if (key.getForeignTableName() == null) {
        _log.warn("Foreign key table is null for key " + key);
      } else {
        printStartOfEmbeddedStatement();
        if (getPlatformInfo().isEmbeddedForeignKeysNamed()) {
          print("CONSTRAINT ");
          printIdentifier(getForeignKeyName(table, key));
          print(" ");
        }
        print(" FOREIGN KEY (");
        writeLocalReferences(key);
        print(") REFERENCES ");
        printIdentifier(getStructureObjectName(database.findTable(key.getForeignTableName())));
        print(" (");
        writeForeignReferences(key);
        print(")");
        writeForeignKeyOnUpdateOption(key);
        writeForeignKeyOnDeleteOption(key);
      }
    }
  }

  /**
   * Writes a single foreign key constraint using a alter table statement.
   * 
   * @param database
   *          The database model
   * @param table
   *          The table
   * @param key
   *          The foreign key
   */
  protected void writeExternalForeignKeyCreateStmt(Database database, Table table, ForeignKey key)
      throws IOException {
    if (key.getForeignTableName() == null) {
      _log.warn("Foreign key table is null for key " + key);
    } else {
      writeTableAlterStmt(table);

      print("ADD CONSTRAINT ");
      printIdentifier(getForeignKeyName(table, key));
      print(" FOREIGN KEY (");
      writeLocalReferences(key);
      print(") REFERENCES ");
      printIdentifier(shortenName(key.getForeignTableName(), getMaxTableNameLength()));
      print(" (");
      writeForeignReferences(key);
      print(")");
      writeForeignKeyOnUpdateOption(key);
      writeForeignKeyOnDeleteOption(key);
      printEndOfStatement();
    }
  }

  /**
   * Writes the On Update option of the given foreign key.
   * 
   * @param key
   *          The foreign key
   */
  protected void writeForeignKeyOnUpdateOption(ForeignKey key) throws IOException {
    if (key.getOnUpdateCode() == DatabaseMetaData.importedKeyCascade) {
      print(" ON UPDATE CASCADE");
    } else if (key.getOnUpdateCode() == DatabaseMetaData.importedKeySetNull) {
      print(" ON UPDATE SET NULL");
    } else if (key.getOnUpdateCode() == DatabaseMetaData.importedKeyRestrict) {
      print(" ON UPDATE RESTRICT");
    }
  }

  /**
   * Writes the On Delete option of the given foreign key.
   * 
   * @param key
   *          The foreign key
   */
  protected void writeForeignKeyOnDeleteOption(ForeignKey key) throws IOException {
    if (key.getOnDeleteCode() == DatabaseMetaData.importedKeyCascade) {
      print(" ON DELETE CASCADE");
    } else if (key.getOnDeleteCode() == DatabaseMetaData.importedKeySetNull) {
      print(" ON DELETE SET NULL");
    } else if (key.getOnDeleteCode() == DatabaseMetaData.importedKeyRestrict) {
      print(" ON DELETE RESTRICT");
    }
  }

  /**
   * Writes a list of local references for the given foreign key.
   * 
   * @param key
   *          The foreign key
   */
  protected void writeLocalReferences(ForeignKey key) throws IOException {
    for (int idx = 0; idx < key.getReferenceCount(); idx++) {
      if (idx > 0) {
        print(", ");
      }
      printIdentifier(key.getReference(idx).getLocalColumnName());
    }
  }

  /**
   * Writes a list of foreign references for the given foreign key.
   * 
   * @param key
   *          The foreign key
   */
  protected void writeForeignReferences(ForeignKey key) throws IOException {
    for (int idx = 0; idx < key.getReferenceCount(); idx++) {
      if (idx > 0) {
        print(", ");
      }
      printIdentifier(key.getReference(idx).getForeignColumnName());
    }
  }

  /**
   * Generates the statement to drop a foreignkey constraint from the database using an alter table
   * statement.
   * 
   * @param table
   *          The table
   * @param foreignKey
   *          The foreign key
   */
  protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey)
      throws IOException {
    writeTableAlterStmt(table);
    print("DROP CONSTRAINT ");
    printIdentifier(getForeignKeyName(table, foreignKey));
    printEndOfStatement();
  }

  /**
   * Writes the checks of the given table.
   * 
   * @param table
   *          The table
   */
  protected void writeExternalChecksCreateStmt(Table table) throws IOException {
    for (int idx = 0; idx < table.getCheckCount(); idx++) {
      Check check = table.getCheck(idx);
      writeExternalCheckCreateStmt(table, check);
    }
  }

  /**
   * Writes the given check of the table.
   * 
   * @param table
   *          The table
   * @param check
   *          The check
   */
  protected void writeExternalCheckCreateStmt(Table table, Check check) throws IOException {
    if (getPlatformInfo().isChecksSupported()) {
      if (check.getName() == null) {
        _log.warn("Cannot write unnamed index " + check);
      } else {
        writeTableAlterStmt(table);

        print("ADD ");

        if ((check.getName() != null) && (check.getName().length() > 0)) {
          print("CONSTRAINT ");
          printIdentifier(getConstraintObjectName(check));
        }

        print(" CHECK (");
        print(check.getCondition());
        print(")");

        printEndOfStatement();
      }
    }
  }

  /**
   * Writes the checks embedded within the create table statement.
   * 
   * @param table
   *          The table
   */
  protected void writeEmbeddedChecksStmt(Table table) throws IOException {
    if (getPlatformInfo().isChecksSupported()) {
      for (int idx = 0; idx < table.getCheckCount(); idx++) {
        printStartOfEmbeddedStatement();
        writeEmbeddedCheckCreateStmt(table, table.getCheck(idx));
      }
    }
  }

  protected void writeEmbeddedCheckCreateStmt(Table table, Check check) throws IOException {

    if ((check.getName() != null) && (check.getName().length() > 0)) {
      print("CONSTRAINT ");
      printIdentifier(getConstraintObjectName(check));
    }

    print(" CHECK (");
    print(check.getCondition());
    print(")");
  }

  /**
   * Generates the statement to drop a non-embedded check from the database.
   * 
   * @param table
   *          The table the index is on
   * @param check
   *          The check to drop
   */
  public void writeExternalCheckDropStmt(Table table, Check check) throws IOException {

    writeTableAlterStmt(table);
    print("DROP CONSTRAINT ");
    printIdentifier(getConstraintObjectName(check));
    printEndOfStatement();
  }

  /**
   * Writes the given sequence .
   * 
   * @param sequence
   *          The sequence
   */
  protected void createSequence(Sequence sequence) throws IOException {

    if (getPlatformInfo().isSequencesSupported()) {
      if (sequence.getName() == null) {
        _log.warn("Cannot write unnamed sequence " + sequence);
      } else {
        printStartOfStatement("SEQUENCE", getStructureObjectName(sequence));

        print("CREATE SEQUENCE ");
        printIdentifier(getStructureObjectName(sequence));
        print(" MINVALUE ");
        print(Integer.toString(sequence.getStart()));
        print(" INCREMENT BY ");
        print(Integer.toString(sequence.getIncrement()));

        printEndOfStatement(getStructureObjectName(sequence));
      }
    }
  }

  protected void alterSequence(Sequence sequence) throws IOException {

    if (getPlatformInfo().isSequencesSupported()) {
      if (sequence.getName() == null) {
        _log.warn("Cannot write unnamed sequence " + sequence);
      } else {
        printStartOfStatement("SEQUENCE", getStructureObjectName(sequence));

        print("ALTER SEQUENCE ");
        printIdentifier(getStructureObjectName(sequence));
        print(" MINVALUE ");
        print(Integer.toString(sequence.getStart()));
        print(" INCREMENT BY ");
        print(Integer.toString(sequence.getIncrement()));

        printEndOfStatement(getStructureObjectName(sequence));
      }
    }
  }

  /**
   * Drops the given sequence .
   * 
   * @param sequence
   *          The sequence
   */
  protected void dropSequence(Sequence sequence) throws IOException {

    if (getPlatformInfo().isSequencesSupported()) {
      if (sequence.getName() == null) {
        _log.warn("Cannot write unnamed sequence " + sequence);
      } else {
        printStartOfStatement("SEQUENCE", getStructureObjectName(sequence));

        print("DROP SEQUENCE ");
        printIdentifier(getStructureObjectName(sequence));

        printEndOfStatement(getStructureObjectName(sequence));
      }
    }
  }

  /**
   * Writes the given view .
   * 
   * @param view
   *          The view
   */
  protected void createView(View view) throws IOException {
    if (getPlatformInfo().isViewsSupported()) {
      if (view.getName() == null) {
        _log.warn("Cannot write unnamed view " + view);
      } else {
        printStartOfStatement("VIEW", getStructureObjectName(view));
        writeCreateViewStatement(view);
        printEndOfStatement(getStructureObjectName(view));

        createUpdateRules(view);
      }
    }
  }

  protected void writeCreateViewStatement(View view) throws IOException {

    print("CREATE VIEW ");
    printIdentifier(getStructureObjectName(view));
    print(" AS ");
    print(getSQLTranslation().exec(view.getStatement()));
  }

  protected void createUpdateRules(View view) throws IOException {
  }

  /**
   * Drops the given view .
   * 
   * @param view
   *          The view
   */
  protected void dropView(View view) throws IOException {

    if (getPlatformInfo().isViewsSupported()) {
      if (view.getName() == null) {
        _log.warn("Cannot write unnamed view " + view);
      } else {

        dropUpdateRules(view);

        printStartOfStatement("VIEW", getStructureObjectName(view));

        print("DROP VIEW ");
        printIdentifier(getStructureObjectName(view));

        printEndOfStatement(getStructureObjectName(view));
      }
    }
  }

  protected void dropUpdateRules(View view) throws IOException {
  }

  /**
   * Writes the given function .
   * 
   * @param function
   *          The function
   */
  protected void createFunction(Function function) throws IOException {

    if (getPlatformInfo().isFunctionsSupported()) {

      if (function.getName() == null) {
        _log.warn("Cannot write unnamed function " + function);
      } else {
        printStartOfStatement("FUNCTION", getStructureObjectName(function));

        writeCreateFunctionStmt(function);

        if (function.getParameterCount() == 0) {
          print(getNoParametersDeclaration());
        } else {
          print("(");
          for (int idx = 0; idx < function.getParameterCount(); idx++) {
            if (idx > 0) {
              print(", ");
            }
            writeParameter(function.getParameter(idx));
          }
          print(")");
        }

        print(" ");
        println(getFunctionReturn(function));
        println();

        print(getFunctionBeginBody());
        println();

        String body = function.getBody();

        LiteralFilter litFilter = new LiteralFilter();
        CommentFilter comFilter = new CommentFilter();
        body = litFilter.removeLiterals(body);
        body = comFilter.removeComments(body);
        body = getPLSQLFunctionTranslation().exec(body);

        body = comFilter.restoreComments(body);
        body = litFilter.restoreLiterals(body);

        print(body);
        // println();
        print(getFunctionEndBody(function));

        printEndOfStatement(getStructureObjectName(function));
      }
    }
  }

  /**
   * Writes the create clause for a function.
   * 
   * @param function
   *          The function
   */
  protected void writeCreateFunctionStmt(Function function) throws IOException {
    if (function.getTypeCode() == Types.NULL) {
      print("CREATE OR REPLACE PROCEDURE ");
    } else {
      print("CREATE OR REPLACE FUNCTION ");
    }
    printIdentifier(getStructureObjectName(function));
  }

  /**
   * Gets the return reserved identifier for a function.
   */
  protected String getFunctionReturn(Function function) {
    return function.getTypeCode() == Types.NULL ? ""
        : "RETURN " + getSqlType(function.getTypeCode());
  }

  /**
   * Gets the begin body clause for a function.
   */
  protected String getFunctionBeginBody() {
    return "AS";
  }

  /**
   * Gets the end body clause for a function.
   */
  protected String getFunctionEndBody(Function function) {
    return "";
  }

  /**
   * Gets the end clause for a function with no parameters. Usually is empty and for Postgre for
   * example is "()"
   */
  protected String getNoParametersDeclaration() {
    return "";
  }

  /**
   * Drops the given function .
   * 
   * @param function
   *          The function
   */
  protected void dropFunction(Function function) throws IOException {

    if (getPlatformInfo().isFunctionsSupported()) {
      if (function.getName() == null) {
        _log.warn("Cannot write unnamed function " + function);
      } else {

        printStartOfStatement("FUNCTION", getStructureObjectName(function));

        writeDropFunctionStmt(function);

        printEndOfStatement(getStructureObjectName(function));
      }
    }
  }

  /**
   * Writes the drop clause for a function.
   * 
   * @param function
   *          The function
   */
  protected void writeDropFunctionStmt(Function function) throws IOException {
    if (function.getTypeCode() == Types.NULL) {
      print("DROP PROCEDURE ");
    } else {
      print("DROP FUNCTION ");
    }
    printIdentifier(getStructureObjectName(function));
  }

  protected void writeParameter(Parameter parameter) throws IOException {

    if (parameter.getName() != null) {
      print(parameter.getName());
      print(" ");
    }

    String mode = getParameterMode(parameter);
    if (mode != null) {
      print(mode);
      print(" ");
    }

    print(getSqlType(parameter.getTypeCode()));

    String value = getDefaultValue(parameter);
    if (value != null) {
      print(" DEFAULT ");
      print(value);
    }
  }

  protected String getParameterMode(Parameter parameter) {

    switch (parameter.getModeCode()) {
      case Parameter.MODE_IN:
        return "IN";
      case Parameter.MODE_OUT:
        return "OUT";
      case Parameter.MODE_NONE:
        return null;
      default:
        return null;
    }
  }

  /**
   * Gets the default value stmt part for the column.
   * 
   * @param identifier
   *          The column or parameter
   */
  protected String getDefaultValue(ValueObject identifier) throws IOException {

    if (identifier.isDefaultFunction()) {
      return getNativeFunction(identifier.getDefaultValue(), identifier.getTypeCode());
    } else {
      Object parsedDefault = identifier.getParsedDefaultValue();

      if (parsedDefault == null) {
        return null;
      } else {
        if (!getPlatformInfo().isDefaultValuesForLongTypesSupported()
            && ((identifier.getTypeCode() == Types.LONGVARBINARY)
                || (identifier.getTypeCode() == Types.LONGVARCHAR))) {
          throw new ModelException(
              "The platform does not support default values for LONGVARCHAR or LONGVARBINARY columns");
        }
        // we write empty default value strings only if the type is not
        // a numeric or date/time type
        if (isValidDefaultValue(identifier.getDefaultValue(), identifier.getTypeCode())) {
          return getDefaultValue(getNativeDefaultValue(identifier), identifier.getTypeCode());
        } else {
          return null;
        }
      }
    }
  }

  public void createTrigger(Database database, Trigger trigger) throws IOException {
    List<String> follows = new ArrayList<String>();
    createTrigger(database, trigger, follows);
  }

  /**
   * Writes the given trigger .
   * 
   * @param trigger
   *          The trigger
   */
  public void createTrigger(Database database, Trigger trigger, List<String> follows)
      throws IOException {

    if (getPlatformInfo().isTriggersSupported()) {

      if (trigger.getName() == null) {
        _log.warn("Cannot write unnamed trigger " + trigger);
      } else {
        writeCreateTriggerFunction(trigger);

        printStartOfStatement("TRIGGER", getStructureObjectName(trigger));
        print("CREATE TRIGGER ");
        printIdentifier(getStructureObjectName(trigger));
        println();

        switch (trigger.getFiresCode()) {
          case Trigger.FIRES_AFTER:
            print("AFTER");
            break;
          case Trigger.FIRES_BEFORE:
          default:
            print("BEFORE");
            break;
        }

        if (trigger.isInsert()) {
          print(" INSERT");
        }
        if (trigger.isUpdate()) {
          if (trigger.isInsert()) {
            print(" OR UPDATE");
          } else {
            print(" UPDATE");
          }
        }
        if (trigger.isDelete()) {
          if (trigger.isInsert() || trigger.isUpdate()) {
            print(" OR DELETE");
          } else {
            print(" DELETE");
          }
        }
        println();

        print("ON ");
        printIdentifier(shortenName(trigger.getTable(), getMaxTableNameLength()));// database.findTable(trigger.getTable())));

        switch (trigger.getForeachCode()) {
          case Trigger.FOR_EACH_ROW:
            print(" FOR EACH ROW");
            break;
        }
        println();
        writeFollows(follows);
        writeTriggerExecuteStmt(trigger);
        printEndOfStatement(getStructureObjectName(trigger));
      }
    }
  }

  /**
   * Writes a clause before the DECLARE statement to ensure that the trigger being created is
   * invoked after other triggers of the same type (only applicable in Oracle)
   * 
   * @param follows
   *          The list of names of the triggers that should be invoked before the trigger being
   *          created
   * @throws IOException
   */
  protected void writeFollows(List<String> follows) throws IOException {
  }

  public void writeCreateTriggerFunction(Trigger trigger) throws IOException {
  }

  public void writeTriggerExecuteStmt(Trigger trigger) throws IOException {
    print("DECLARE");

    String body = trigger.getBody();
    if (!body.substring(0, 1).equals("\n")) {
      body = "\n" + body;
    }
    LiteralFilter litFilter = new LiteralFilter();
    CommentFilter comFilter = new CommentFilter();
    body = litFilter.removeLiterals(body);
    body = comFilter.removeComments(body);
    body = getPLSQLTriggerTranslation().exec(body);
    body = comFilter.restoreComments(body);
    body = litFilter.restoreLiterals(body);

    print(body);
    // println();
    print(";");
  }

  /**
   * Drops the given trigger .
   * 
   * @param trigger
   *          The trigger
   */
  public void dropTrigger(Database database, Trigger trigger) throws IOException {

    if (getPlatformInfo().isTriggersSupported()) {
      if (trigger.getName() == null) {
        _log.warn("Cannot write unnamed trigger " + trigger);
      } else {
        printStartOfStatement("TRIGGER", getStructureObjectName(trigger));

        print("DROP TRIGGER ");
        printIdentifier(getStructureObjectName(trigger));
        writeDropTriggerEndStatement(database, trigger);
        printEndOfStatement(getStructureObjectName(trigger));

        writeDropTriggerFunction(trigger);
      }
    }
  }

  protected void writeDropTriggerEndStatement(Database database, Trigger trigger)
      throws IOException {
  }

  protected void writeDropTriggerFunction(Trigger trigger) throws IOException {
  }

  // pseudo abstract here as only implemented for PostgreSqlPlatform at the moment
  public void disableTrigger(Database database, Trigger trigger) throws IOException {
    throw new RuntimeException("Needs to be implemented by a specific db class");
  }

  // pseudo abstract here as only implemented for PostgreSqlPlatform at the moment
  public void enableTrigger(Database database, Trigger trigger) throws IOException {
    throw new RuntimeException("Needs to be implemented by a specific db class");
  }

  protected Translation createPLSQLFunctionTranslation(Database database) {
    return new NullTranslation();
  }

  public Translation createPLSQLTriggerTranslation(Database database) {
    return new NullTranslation();
  }

  protected Translation createSQLTranslation(Database database) {
    return new NullTranslation();
  }

  protected final Translation getPLSQLFunctionTranslation() {
    return _PLSQLFunctionTranslation;
  }

  protected final Translation getPLSQLTriggerTranslation() {
    return _PLSQLTriggerTranslation;
  }

  protected final Translation getSQLTranslation() {
    return _SQLTranslation;
  }

  /**
   * Returns the constraint name. This method takes care of length limitations imposed by some
   * databases.
   * 
   * @param obj
   *          The constraint
   * @return The constraint name
   */
  public String getConstraintObjectName(ConstraintObject obj) {
    return shortenName(obj.getName(), getMaxConstraintNameLength());
  }

  /**
   * Returns the structure name. This method takes care of length limitations imposed by some
   * databases.
   * 
   * @param obj
   *          The structure
   * @return The structure name
   */
  public String getStructureObjectName(StructureObject obj) {
    return shortenName(obj.getName(), getMaxTableNameLength());
  }

  /**
   * Returns the structure name. This method takes care of length limitations imposed by some
   * databases.
   * 
   * @param name
   *          The structure name
   * @return The shortened structure name
   */
  public String getStructureObjectName(String name) {
    return shortenName(name, getMaxTableNameLength());
  }

  /**
   * Returns the full SQL type specification for the given type.
   * 
   * @param column
   *          The column
   * @return The full SQL type string including the size
   */
  protected String getSqlType(int typeCode) {

    String nativeType = getPlatformInfo().getNativeType(typeCode);
    int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);
    StringBuffer sqlType = new StringBuffer();

    sqlType.append(sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType);
    sqlType.append(sizePos >= 0 ? nativeType.substring(sizePos + SIZE_PLACEHOLDER.length()) : "");

    return sqlType.toString();
  }

  //
  // Helper methods
  //

  /**
   * Prints an SQL comment to the current stream.
   * 
   * @param text
   *          The comment text
   */
  protected void printComment(String text) throws IOException {
    if (getPlatform().isSqlCommentsOn()) {
      print(getPlatformInfo().getCommentPrefix());
      // Some databases insist on a space after the prefix
      print(" ");
      print(text);
      print(" ");
      print(getPlatformInfo().getCommentSuffix());
      println();
    }
  }

  /**
   * Prints the start of an embedded statement.
   */
  protected void printStartOfEmbeddedStatement() throws IOException {
    println(",");
    printIndent();
  }

  /**
   * Prints the end of statement text, which is typically a semi colon followed by a carriage
   * return.
   */
  public void printEndOfStatement() throws IOException {
    printEndOfStatement("");
  }

  /**
   * Prints the end of statement text, which is typically a semi colon followed by a carriage
   * return.
   */
  protected void printEndOfStatement(String statementName) throws IOException {
    println();
    print(getPlatformInfo().getSqlCommandDelimiter());
    if (script) {
      println();
    } else {
      printComment("END");
    }
    // println();
  }

  protected void printStartOfStatement(String type, String statementName) throws IOException {
    printComment("-----------------------------------------------------------------------");
    printComment(type + " " + statementName);
    printComment("-----------------------------------------------------------------------");
  }

  protected void printScriptOptions(String options) throws IOException {
    printComment("SCRIPT OPTIONS (" + options + ")");
  }

  /**
   * Prints a newline.
   */
  protected void println() throws IOException {
    print(LINE_SEPARATOR);
  }

  /**
   * Prints some text.
   * 
   * @param text
   *          The text to print
   */
  protected void print(String text) throws IOException {
    _writer.write(text);
  }

  /**
   * Returns the delimited version of the identifier (if configured).
   * 
   * @param identifier
   *          The identifier
   * @return The delimited version of the identifier unless the platform is configured to use
   *         undelimited identifiers; in that case, the identifier is returned unchanged
   */
  protected String getDelimitedIdentifier(String identifier) {
    if (getPlatform().isDelimitedIdentifierModeOn()) {
      return getPlatformInfo().getDelimiterToken() + identifier
          + getPlatformInfo().getDelimiterToken();
    } else {
      return identifier;
    }
  }

  /**
   * Prints the given identifier. For most databases, this will be a delimited identifier.
   * 
   * @param identifier
   *          The identifier
   */
  protected void printIdentifier(String identifier) throws IOException {
    print(getDelimitedIdentifier(identifier));
  }

  /**
   * Prints the given identifier followed by a newline. For most databases, this will be a delimited
   * identifier.
   * 
   * @param identifier
   *          The identifier
   */
  protected void printlnIdentifier(String identifier) throws IOException {
    println(getDelimitedIdentifier(identifier));
  }

  /**
   * Prints some text followed by a newline.
   * 
   * @param text
   *          The text to print
   */
  protected void println(String text) throws IOException {
    print(text);
    println();
  }

  /**
   * Prints the characters used to indent SQL.
   */
  protected void printIndent() throws IOException {
    print(getIndent());
  }

  /**
   * Creates a reasonably unique identifier only consisting of hexadecimal characters and
   * underscores. It looks like <code>d578271282b42fce__2955b56e_107df3fbc96__8000</code> and is 48
   * characters long.
   * 
   * @return The identifier
   */
  protected String createUniqueIdentifier() {
    return new UID().toString().replace(':', '_').replace('-', '_');
  }

  public void printColumnDataChange(Database database, ColumnDataChange change) throws IOException {
    HashMap map = new HashMap();
    Table table = database.findTable(change.getTablename());
    String pk = table.getPrimaryKeyColumns()[0].getName();
    map.put(pk, change.getPkRow());
    map.put(change.getColumnname(), change.getNewValue());
    println(getUpdateSql(table, map, false));
    printEndOfStatement();

  }

  public void printColumnSizeChange(Database database, ColumnSizeChange change) throws IOException {
    _log.error("Column size change not supported.");
  }

  public void printRemoveTriggerChange(Database database, RemoveTriggerChange change)
      throws IOException {
    _log.error("Remove Trigger change not supported.");
  }

  public void printRemoveIndexChange(Database database, RemoveIndexChange change)
      throws IOException {
    _log.error("Remove Index change not supported.");
  }

  public void printColumnRequiredChange(Database database, ColumnRequiredChange change)
      throws IOException {
    _log.error("Column Required change not supported.");
  }

  public void printRemoveCheckChange(Database database, RemoveCheckChange change)
      throws IOException {
    _log.error("Remove Check change not supported.");
  }

  public void printAddRowChangeChange(Database model, AddRowChange change) throws IOException {
    Table table = change.getTable();
    DynaBean db = change.getRow();
    HashMap result = new HashMap();

    Column[] columns = table.getColumns();
    for (int i = 0; i < columns.length; i++) {
      result.put(columns[i].getName(), db.get(columns[i].getName()));
    }
    println(getInsertSql(table, result, false));
    printEndOfStatement();
  }

  public void printRemoveRowChange(Database model, RemoveRowChange change) throws IOException {
    Table table = change.getTable();
    Column[] pk = table.getPrimaryKeyColumns();
    DynaBean object = change.getRow();
    HashMap pkValues = new HashMap();
    pkValues.put(pk[0].getName(), object.get(pk[0].getName()));
    println(getDeleteSql(table, pkValues, false));
    printEndOfStatement();
  }

  public boolean willBeRecreated(Table table, Vector<TableChange> changes) {
    return requiresRecreation(table, changes, false);
  }

  /** Checks whether table requires recreation base on the changes that require */
  private boolean requiresRecreation(Table table, List<TableChange> changes,
      boolean logRecreation) {
    if (changes == null || changes.isEmpty()) {
      return false;
    }

    if (isRecreationForced(table)) {
      if (logRecreation) {
        _log.info(
            "Table " + table.getName() + " will be recreated because it is forced by parameter");
      }
      return true;
    }

    boolean recreationRequired = false;
    List<TableChange> unsupportedChanges = new ArrayList<TableChange>();
    for (TableChange change : changes) {
      boolean changeRequiresRecreation = true;
      Method m = null;
      try {
        m = this.getClass().getMethod("requiresRecreation", change.getClass());
      } catch (Exception ignore) {
      }
      if (m != null) {
        try {
          changeRequiresRecreation = (Boolean) m.invoke(this, change);
        } catch (Exception ignore) {
        }
      } else {
        changeRequiresRecreation = requiresRecreation(change);
      }
      if (changeRequiresRecreation) {
        recreationRequired = true;
        unsupportedChanges.add(change);
      }
    }

    if (logRecreation && recreationRequired) {
      _log.info("Table " + table.getName() + " will be recreated because of these changes");
      for (Object recChange : unsupportedChanges) {
        _log.info("       " + recChange);
      }
    }

    return recreationRequired;
  }

  private boolean isRecreationForced(Table table) {
    if (StringUtils.isEmpty(forcedRecreation)) {
      return false;
    }
    if (forcedRecreation.equalsIgnoreCase("all")) {
      return true;
    }
    for (String tableName : forcedRecreation.split(",")) {
      if (table.getName().equalsIgnoreCase(tableName)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks if a TableChange requires full table recreation. This method can be overridden for
   * specific changes
   */
  public boolean requiresRecreation(TableChange change) {
    // list of changes not requiring table recreation
    Class[] supportedTypes = new Class[] { AddColumnChange.class, //
        RemovePrimaryKeyChange.class, //
        PrimaryKeyChange.class, //
        AddColumnChange.class, //
        RemoveColumnChange.class, //
        AddPrimaryKeyChange.class, //
        ColumnOnCreateDefaultValueChange.class, //
        ColumnRequiredChange.class, //
        ColumnDefaultValueChange.class //
    };

    Predicate p = new MultiInstanceofPredicate(supportedTypes);
    return !p.evaluate(change);
  }

  /**
   * Processes changes on a table to try to apply them to the existing table without recreating it,
   * if this is possible changes list will be empty after invocation of this method, if not,
   * remaining changes will be in that list.
   */
  protected void processTableStructureChanges(Database currentModel, Database desiredModel,
      Table sourceTable, Table targetTable, Map parameters, List<TableChange> changes)
      throws IOException {

    if (requiresRecreation(targetTable, changes, true)) {
      return;
    }

    // // First we drop primary keys as necessary
    for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
      TableChange change = (TableChange) changeIt.next();

      if (change instanceof RemovePrimaryKeyChange) {
        processChange(currentModel, desiredModel, (RemovePrimaryKeyChange) change);
        changeIt.remove();
      } else if (change instanceof PrimaryKeyChange) {
        PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
        RemovePrimaryKeyChange removePkChange = new RemovePrimaryKeyChange(
            pkChange.getChangedTable(), pkChange.getOldPrimaryKeyColumns());

        processChange(currentModel, desiredModel, removePkChange);
      } else if (change instanceof ColumnOnCreateDefaultValueChange) {
        processChange(currentModel, desiredModel, (ColumnOnCreateDefaultValueChange) change);
        changeIt.remove();
      } else if (change instanceof ColumnRequiredChange) {
        processChange(currentModel, desiredModel, (ColumnRequiredChange) change);
        changeIt.remove();
      } else if (change instanceof ColumnDefaultValueChange) {
        processChange(currentModel, desiredModel, (ColumnDefaultValueChange) change);
        changeIt.remove();
      } else if (change instanceof ColumnDataTypeChange) {
        processChange(currentModel, desiredModel, (ColumnDataTypeChange) change);
        changeIt.remove();
      } else if (change instanceof ColumnSizeChange) {
        processChange(currentModel, desiredModel, (ColumnSizeChange) change);
        changeIt.remove();
      }
    }

    boolean newColumnsShouldBeAdded = false;
    boolean oldColumnsShouldBeDropped = false;
    for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
      TableChange change = (TableChange) changeIt.next();

      if (change instanceof AddColumnChange) {
        newColumnsShouldBeAdded = true;
      } else if (change instanceof RemoveColumnChange) {
        oldColumnsShouldBeDropped = true;
      }
    }

    if (newColumnsShouldBeAdded) {
      addNewColumns(targetTable, changes, currentModel, desiredModel);
    }

    if (oldColumnsShouldBeDropped) {
      dropOldColumns(targetTable, changes, currentModel, desiredModel);
    }

    // Finally we add primary keys
    for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
      TableChange change = (TableChange) changeIt.next();

      if (change instanceof AddPrimaryKeyChange) {
        processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change);
        changeIt.remove();
      } else if (change instanceof PrimaryKeyChange) {
        PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
        AddPrimaryKeyChange addPkChange = new AddPrimaryKeyChange(pkChange.getChangedTable(),
            pkChange.getNewName(), pkChange.getNewPrimaryKeyColumns());

        processChange(currentModel, desiredModel, addPkChange);
        changeIt.remove();
      }
    }
  }

  /**
   * Adds new columns to an existing table, all of these new columns are added in a single ALTER
   * TABLE statement
   */
  private void addNewColumns(Table targetTable, List changes, Database currentModel,
      Database desiredModel) throws IOException {

    print("ALTER TABLE ");
    printlnIdentifier(getStructureObjectName(targetTable.getName()));

    int i = 0;
    for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
      TableChange change = (TableChange) changeIt.next();
      if (change instanceof AddColumnChange) {
        addColumnStatement(i);
        processChange(currentModel, desiredModel, (AddColumnChange) change);
        changeIt.remove();
        i++;
      }
    }

    endAlterTable();
  }

  /**
   * Drops columns from an existing table, all these drops are included in a single ALTER TABLE
   * statement
   */
  private void dropOldColumns(Table targetTable, List changes, Database currentModel,
      Database desiredModel) throws IOException {

    print("ALTER TABLE ");
    printlnIdentifier(getStructureObjectName(targetTable.getName()));

    int i = 0;
    for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
      TableChange change = (TableChange) changeIt.next();
      if (change instanceof RemoveColumnChange) {
        dropColumnStatement(i);
        processChange(currentModel, desiredModel, (RemoveColumnChange) change);
        changeIt.remove();
        i++;
      }
    }

    endAlterTable();
  }

  /**
   * Writes SQL required to add a single column within an ALTER TABLE statement.
   * 
   * This is DB specific so it must be implemented per DB
   */
  protected void addColumnStatement(int position) throws IOException {
    // no default implementation
  }

  /**
   * Writes SQL required to drop a single column within an ALTER TABLE statement.
   * 
   * This is DB specific so it must be implemented per DB
   */
  protected void dropColumnStatement(int position) throws IOException {
    // no default implementation
  }

  /**
   * Writes SQL required terminate ALTER TABLE statement
   * 
   * This is DB specific so it must be implemented per DB
   */
  protected void endAlterTable() throws IOException {
    // no default implementation
  }

  /**
   * Processes the addition of a column to a table.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel, AddColumnChange change)
      throws IOException {

    Column newColumn = change.getNewColumn();
    boolean deferNotNull = newColumn.isRequired()
        && StringUtils.isNotEmpty(newColumn.getOnCreateDefault())
        && newColumn.getLiteralOnCreateDefault() == null;

    writeColumn(change.getChangedTable(), change.getNewColumn(), deferNotNull);

    if (change.getNewColumn().isAutoIncrement()) {
      createAutoIncrementSequence(change.getChangedTable(), change.getNewColumn());
      createAutoIncrementTrigger(change.getChangedTable(), change.getNewColumn());
    }
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    desiredModel.addNewColumnChange(change);
    if (deferNotNull) {
      desiredModel.addDeferredNotNull(change);
    }

    if (!newColumn.isSameDefaultAndOCD()) {
      desiredModel.addDeferredDefault(change);
    }
  }

  /**
   * Processes the removal of a column from a table.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      RemoveColumnChange change) throws IOException {
    if (change.getColumn().isAutoIncrement()) {
      dropAutoIncrementTrigger(change.getChangedTable(), change.getColumn());
      dropAutoIncrementSequence(change.getChangedTable(), change.getColumn());
    }
    printIdentifier(getColumnName(change.getColumn()));
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  protected void processChange(Database currentModel, Database desiredModel,
      ColumnDefaultValueChange change) throws IOException {
    // no default implementation
  }

  protected void processChange(Database currentModel, Database desiredModel,
      ColumnRequiredChange change) throws IOException {
    // no default implementation
  }

  protected void processChange(Database currentModel, Database desiredModel,
      ColumnDataTypeChange change) throws IOException {
    // no default implementation
  }

  protected void processChange(Database currentModel, Database desiredModel,
      ColumnSizeChange change) throws IOException {
    // no default implementation
  }

  /**
   * Creates the sequence necessary for the auto-increment of the given column. To be implemented
   * platform specific: default unimplemented
   */
  protected void createAutoIncrementSequence(Table table, Column column) throws IOException {
  }

  /**
   * Creates the trigger necessary for the auto-increment of the given column. To be implemented
   * platform specific: default unimplemented
   */
  protected void createAutoIncrementTrigger(Table table, Column column) throws IOException {
  }

  /**
   * Drops the sequence used for the auto-increment of the given column. To be implemented platform
   * specific: default unimplemented
   */
  protected void dropAutoIncrementSequence(Table table, Column column) throws IOException {
  }

  /**
   * Drops the trigger used for the auto-increment of the given column. To be implemented platform
   * specific: default unimplemented
   */
  protected void dropAutoIncrementTrigger(Table table, Column column) throws IOException {
  }

  /**
   * Processes the removal of a primary key from a table.To be implemented platform specific:
   * default unimplemented
   */
  protected void processChange(Database currentModel, Database desiredModel,
      RemovePrimaryKeyChange change) throws IOException {
  }

  protected void addDefault(AddColumnChange deferredDefault) throws IOException {

  }

  public void setForcedRecreation(String forcedRecreation) {
    this.forcedRecreation = forcedRecreation;
  }
}
