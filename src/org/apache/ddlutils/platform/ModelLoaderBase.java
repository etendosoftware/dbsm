/*
 ************************************************************************************
 * Copyright (C) 2001-2019 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.model.Reference;
import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.Unique;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.util.ExtTypes;

/**
 * 
 * @author adrian
 */
public abstract class ModelLoaderBase implements ModelLoader {

  protected Connection _connection;
  protected ExcludeFilter _filter;

  protected PreparedStatement _stmt_listtables;
  protected PreparedStatement _stmt_pkname;
  protected PreparedStatement _stmt_pkname_prefix;
  protected PreparedStatement _stmt_pkname_noprefix;
  protected PreparedStatement _stmt_listcolumns;
  protected PreparedStatement _stmt_listcolumns_prefix;
  protected PreparedStatement _stmt_listcolumns_noprefix;
  protected PreparedStatement _stmt_pkcolumns;
  protected PreparedStatement _stmt_listchecks;
  protected PreparedStatement _stmt_listchecks_prefix;
  protected PreparedStatement _stmt_listchecks_noprefix;

  protected PreparedStatement _stmt_listfks;
  protected PreparedStatement _stmt_listfks_prefix;
  protected PreparedStatement _stmt_listfks_noprefix;
  protected PreparedStatement _stmt_fkcolumns;

  protected PreparedStatement _stmt_listindexes;
  protected PreparedStatement _stmt_listindexes_prefix;
  protected PreparedStatement _stmt_listindexes_noprefix;
  protected PreparedStatement _stmt_indexcolumns;

  protected PreparedStatement _stmt_listuniques;
  protected PreparedStatement _stmt_listuniques_prefix;
  protected PreparedStatement _stmt_listuniques_noprefix;
  protected PreparedStatement _stmt_uniquecolumns;

  protected PreparedStatement _stmt_listviews;

  protected PreparedStatement _stmt_listsequences;

  protected PreparedStatement _stmt_listtriggers;

  protected PreparedStatement _stmt_listfunctions;
  protected PreparedStatement _stmt_functioncode;

  protected Log _log;

  protected String _prefix;
  protected boolean _loadCompleteTables;
  protected String _moduleId;

  private boolean onlyLoadTableColumns = false;
  private int maxThreads = 1;

  /** Creates a new instance of BasicModelLoader */
  public ModelLoaderBase() {

  }

  @Override
  public Database getDatabase(Connection connection, ExcludeFilter filter) throws SQLException {
    boolean doPlSqlStandardization = true;
    return getDatabase(connection, filter, doPlSqlStandardization);
  }

  @Override
  public Database getDatabase(Connection connection, ExcludeFilter filter,
      boolean doPlSqlStandardization) throws SQLException {

    _filter = filter == null ? new ExcludeFilter() : filter;

    try {
      _connection = connection;

      initMetadataSentences();

      return readDatabase(doPlSqlStandardization);

    } finally {
      closeMetadataSentences();
    }
  }

  @Override
  public Database getDatabase(Connection connection, ExcludeFilter filter, String prefix,
      boolean loadCompleteTables, String moduleId) throws SQLException {

    _filter = filter == null ? new ExcludeFilter() : filter;
    _prefix = prefix.toUpperCase();
    _loadCompleteTables = loadCompleteTables;
    _moduleId = moduleId;
    try {
      _connection = connection;

      initMetadataSentences();

      return readDatabase();

    } finally {
      closeMetadataSentences();
    }
  }

  @Override
  public void setLog(Log log) {
    _log = log;
  }

  @Override
  public Log getLog() {
    return _log;
  }

  protected abstract void initMetadataSentences() throws SQLException;

  protected void closeMetadataSentences() throws SQLException {

    _stmt_listtables.close();
    _stmt_pkname.close();
    _stmt_listcolumns.close();
    _stmt_pkcolumns.close();
    _stmt_listchecks.close();
    _stmt_listfks.close();
    _stmt_fkcolumns.close();

    _stmt_listindexes.close();
    _stmt_indexcolumns.close();

    _stmt_listuniques.close();
    _stmt_uniquecolumns.close();

    _stmt_listviews.close();

    _stmt_listsequences.close();

    _stmt_listtriggers.close();

    _stmt_listfunctions.close();
  }

  protected Database readDatabase() throws SQLException {
    boolean doPlSqlStandardization = true;
    return readDatabase(doPlSqlStandardization);
  }

  protected Database readDatabase(boolean doPlSqlStandardization) throws SQLException {
    Database db = new Database();
    db.setName(readName());
    _log.info("Reading tables...");
    Collection tables = readTables();
    Iterator it = tables.iterator();
    ArrayList filteredTables = new ArrayList();
    while (it.hasNext()) {
      Table table = (Table) it.next();
      if (!table.isEmpty()) {
        filteredTables.add(table);
      }
    }
    if (_prefix != null && _loadCompleteTables) {
      addTablesToCorrectPlace(db, readTablesFromList(filteredTables));
    } else {
      addTablesToCorrectPlace(db, filteredTables);
    }
    if (!onlyLoadTableColumns) {
      _log.info("Reading views...");
      db.addViews(readViews());
      _log.info("Reading sequences...");
      db.addSequences(readSequences());
      _log.info("Reading triggers...");
      db.addTriggers(readTriggers());
      _log.info("Reading functions...");
      db.addFunctions(readFunctions());
    }

    _log.info("Sorting foreign keys, indexes and checks...");
    for (int tableIdx = 0; tableIdx < db.getTableCount(); tableIdx++) {
      db.getTable(tableIdx).sortForeignKeys(false);
      db.getTable(tableIdx).sortIndices(false);
      db.getTable(tableIdx).sortChecks(false);
    }
    return db;
  }

  protected void addTablesToCorrectPlace(Database db, Collection tables) {
    Iterator it = tables.iterator();
    while (it.hasNext()) {
      Table table = (Table) it.next();
      if (_prefix == null) {
        db.addTable(table);
      } else if (_filter.compliesWithNamingRuleObject(table.getName())) {
        db.addTable(table);
      } else {
        db.addModifiedTable(table);
      }
    }
  }

  protected Collection readTables() throws SQLException {

    return readList(_stmt_listtables, new RowConstructor() {
      @Override
      public Object getRow(ResultSet r) throws SQLException {
        _log.debug("Table " + r.getString(1));
        return readTable(r.getString(1), true);
      }
    });
  }

  protected List readTableNames() throws SQLException {

    return readList(_stmt_listtables, new RowConstructor() {
      @Override
      public Object getRow(ResultSet r) throws SQLException {
        _log.debug("Table " + r.getString(1));
        return r.getString(1);
      }
    });
  }

  protected Collection readTablesFromList(Collection tables) throws SQLException {
    ArrayList newTables = new ArrayList();
    Iterator it = tables.iterator();
    while (it.hasNext()) {
      Table table = (Table) it.next();
      newTables.add(readTable(table.getName(), false));
    }
    return newTables;
  }

  protected Table readTable(String tablename, boolean usePrefix) throws SQLException {

    // on postgres this readTable gets called from the PostgreSqlModelLoader
    // with the tablename having the same case as the table has in the database.
    // To get same behavior in rest of dbsm which expects the name in uppercase
    // we use the uppercase name in the Table object

    // is in correct case (like in db) on postgres, and in upperCase on oracle
    String tableRealName = tablename;

    // is always in upperCase on both oracle & postgres
    tablename = tableRealName.toUpperCase();

    final Table t = new Table();
    // final object just to hold the real-case sensitive name of the primaryKeyName
    final StringBuilder pkNameHolder = new StringBuilder();

    t.setName(tablename);

    if (_prefix == null || !usePrefix) {
      _stmt_pkname.setString(1, tableRealName);
      fillRow(_stmt_pkname, new RowFiller() {
        @Override
        public void fillRow(ResultSet r) throws SQLException {
          t.setPrimaryKey(r.getString(1).toUpperCase());
          pkNameHolder.append(r.getString(1));
        }
      });
    } else if (_filter.compliesWithNamingRuleObject(tableRealName)) {
      _stmt_pkname_noprefix.setString(1, tableRealName);
      fillRow(_stmt_pkname_noprefix, new RowFiller() {
        @Override
        public void fillRow(ResultSet r) throws SQLException {
          t.setPrimaryKey(r.getString(1).toUpperCase());
          pkNameHolder.append(r.getString(1));
        }
      });
    } else {
      _stmt_pkname_prefix.setString(1, tableRealName);
      fillRow(_stmt_pkname_prefix, new RowFiller() {
        @Override
        public void fillRow(ResultSet r) throws SQLException {
          t.setPrimaryKey(r.getString(1).toUpperCase());
          pkNameHolder.append(r.getString(1));
        }
      });
    }
    // Columns
    t.addColumns(readColumns(tableRealName, usePrefix));

    // PKS
    if (t.getPrimaryKey() != null && !t.getPrimaryKey().equals("")) {
      _stmt_pkcolumns.setString(1, pkNameHolder.toString());
      fillList(_stmt_pkcolumns, new RowFiller() {
        @Override
        public void fillRow(ResultSet r) throws SQLException {
          t.findColumn(r.getString(1)).setPrimaryKey(true);
        }
      });
    }

    // Checks
    t.addChecks(readChecks(tableRealName, usePrefix));

    // FKS
    t.addForeignKeys(readForeignKeys(tableRealName, usePrefix));

    // Indexes
    t.addIndices(readIndexes(tableRealName, usePrefix));

    // Uniques
    t.adduniques(readUniques(tableRealName, usePrefix));

    return t;
  }

  protected Collection readColumns(String tablename, boolean usePrefix) throws SQLException {
    if (_prefix == null || !usePrefix) {
      _stmt_listcolumns.setString(1, tablename);

      return readList(_stmt_listcolumns, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readColumn(r);
        }
      });
    } else if (_filter.compliesWithNamingRuleObject(tablename)) {
      _stmt_listcolumns_noprefix.setString(1, tablename);

      return readList(_stmt_listcolumns_noprefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readColumn(r);
        }
      });
    } else {
      _stmt_listcolumns_prefix.setString(1, tablename);

      return readList(_stmt_listcolumns_prefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readColumn(r);
        }
      });
    }
  }

  protected Column readColumn(ResultSet rs) throws SQLException {

    Column c = new Column();

    c.setName(rs.getString(1));
    c.setTypeCode(translateColumnType(rs.getString(2)));

    if (c.getTypeCode() == Types.DECIMAL) {
      int size = rs.getInt(5);
      if (size == 0) {
        c.setSize(null);
      } else {
        c.setSizeAndScale(rs.getInt(5), rs.getInt(6));
      }
    } else if (c.getTypeCode() == Types.CHAR || c.getTypeCode() == Types.VARCHAR
        || c.getTypeCode() == ExtTypes.NCHAR || c.getTypeCode() == ExtTypes.NVARCHAR) {
      c.setSizeAndScale(rs.getInt(3), null);
    } else if (c.getTypeCode() == Types.TIMESTAMP) {
      c.setSizeAndScale(7, null);
    } else if (c.getTypeCode() == Types.CLOB || c.getTypeCode() == Types.BLOB) {
      c.setSizeAndScale(rs.getInt(4), null);
    }
    c.setRequired(translateRequired(rs.getString(7)));
    c.setDefaultValue(translateColumnDefault(rs.getString(8), c.getTypeCode()));

    return c;
  }

  protected Collection readChecks(String tablename, boolean usePrefix) throws SQLException {
    if (_prefix == null || !usePrefix) {
      _stmt_listchecks.setString(1, tablename);
      return readList(_stmt_listchecks, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readCheck(r);
        }
      });
    } else if (_filter.compliesWithNamingRuleObject(tablename)) {
      _stmt_listchecks_noprefix.setString(1, tablename);
      return readList(_stmt_listchecks_noprefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readCheck(r);
        }
      });
    } else {
      _stmt_listchecks_prefix.setString(1, tablename);
      return readList(_stmt_listchecks_prefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readCheck(r);
        }
      });
    }
  }

  protected Check readCheck(ResultSet rs) throws SQLException {

    Check c = new Check();

    final String checkName = rs.getString(1);
    final String originalCheck = rs.getString(2);
    final String translatedCheck = translateCheckCondition(originalCheck);

    _log.debug("  check " + checkName + " - original: " + originalCheck + " - translated: "
        + translatedCheck);
    c.setName(checkName);
    c.setCondition(translatedCheck);

    return c;
  }

  protected Collection readForeignKeys(String tablename, boolean usePrefix) throws SQLException {
    if (_prefix == null || !usePrefix) {
      _stmt_listfks.setString(1, tablename);
      return readList(_stmt_listfks, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readForeignKey(r);
        }
      });
    } else if (_filter.compliesWithNamingRuleObject(tablename)) {
      _stmt_listfks_noprefix.setString(1, tablename);
      return readList(_stmt_listfks_noprefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readForeignKey(r);
        }
      });
    } else {
      _stmt_listfks_prefix.setString(1, tablename);
      return readList(_stmt_listfks_prefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readForeignKey(r);
        }
      });
    }
  }

  protected ForeignKey readForeignKey(ResultSet rs) throws SQLException {
    String fkRealName = rs.getString(1);
    String fkName = fkRealName.toUpperCase();

    final ForeignKey fk = new ForeignKey();

    fk.setName(fkName);
    fk.setForeignTableName(rs.getString(2));
    fk.setOnDeleteCode(translateFKEvent(rs.getString(3)));
    fk.setOnUpdateCode(translateFKEvent(rs.getString(4)));

    _stmt_fkcolumns.setString(1, fkRealName);
    fillList(_stmt_fkcolumns, new RowFiller() {
      @Override
      public void fillRow(ResultSet r) throws SQLException {
        Reference ref = new Reference();
        ref.setLocalColumnName(r.getString(1));
        ref.setForeignColumnName(r.getString(2));
        fk.addReference(ref);
      }
    });

    return fk;
  }

  protected Collection readIndexes(String tablename, boolean usePrefix) throws SQLException {
    if (_prefix == null || !usePrefix) {
      _stmt_listindexes.setString(1, tablename);
      return readList(_stmt_listindexes, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readIndex(r);
        }
      });
    } else if (_filter.compliesWithNamingRuleObject(tablename)) {
      _stmt_listindexes_noprefix.setString(1, tablename);
      return readList(_stmt_listindexes_noprefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readIndex(r);
        }
      });
    } else {
      _stmt_listindexes_prefix.setString(1, tablename);
      return readList(_stmt_listindexes_prefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readIndex(r);
        }
      });
    }
  }

  protected Index readIndex(ResultSet rs) throws SQLException {
    String indexRealName = rs.getString(1);
    String indexName = indexRealName.toUpperCase();

    final Index inx = new Index();

    inx.setName(indexName);
    inx.setUnique(translateUniqueness(rs.getString(2)));

    _stmt_indexcolumns.setString(1, indexRealName);
    fillList(_stmt_indexcolumns, new RowFiller() {
      @Override
      public void fillRow(ResultSet r) throws SQLException {
        IndexColumn inxcol = new IndexColumn();
        inxcol.setName(r.getString(1));
        inx.addColumn(inxcol);
      }
    });

    return inx;
  }

  protected Collection readUniques(String tablename, boolean usePrefix) throws SQLException {
    if (_prefix == null || !usePrefix) {
      _stmt_listuniques.setString(1, tablename);
      return readList(_stmt_listuniques, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readUnique(r);
        }
      });
    } else if (_filter.compliesWithNamingRuleObject(tablename)) {
      _stmt_listuniques_noprefix.setString(1, tablename);
      return readList(_stmt_listuniques_noprefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readUnique(r);
        }
      });
    } else {
      _stmt_listuniques_prefix.setString(1, tablename);
      return readList(_stmt_listuniques_prefix, new RowConstructor() {
        @Override
        public Object getRow(ResultSet r) throws SQLException {
          return readUnique(r);
        }
      });
    }
  }

  protected Unique readUnique(ResultSet rs) throws SQLException {
    // similar to readTable, see there for definition of both (regarding case)
    String constraintRealName = rs.getString(1);
    String constraintName = constraintRealName.toUpperCase();

    final Unique uni = new Unique();

    uni.setName(constraintName);

    _stmt_uniquecolumns.setString(1, constraintRealName);
    fillList(_stmt_uniquecolumns, new RowFiller() {
      @Override
      public void fillRow(ResultSet r) throws SQLException {
        IndexColumn inxcol = new IndexColumn();
        inxcol.setName(r.getString(1));
        uni.addColumn(inxcol);
      }
    });

    return uni;
  }

  protected Collection readViews() throws SQLException {

    return readList(_stmt_listviews, new RowConstructor() {
      @Override
      public Object getRow(ResultSet r) throws SQLException {
        _log.debug("View " + r.getString(1));
        View v = new View();
        v.setName(r.getString(1));
        v.setStatement(translateSQL(r.getString(2)));
        return v;
      }
    });
  }

  protected Collection readSequences() throws SQLException {

    return readList(_stmt_listsequences, new RowConstructor() {
      @Override
      public Object getRow(ResultSet r) throws SQLException {
        _log.debug("Sequence " + r.getString(1));
        Sequence s = new Sequence();
        s.setName(r.getString(1));
        s.setStart(r.getInt(2));
        s.setIncrement(r.getInt(3));
        return s;
      }
    });
  }

  protected Collection readTriggers() throws SQLException {

    return readList(_stmt_listtriggers, new RowConstructor() {
      @Override
      public Object getRow(ResultSet r) throws SQLException {
        _log.debug("Trigger " + r.getString(1));
        Trigger t = new Trigger();
        t.setName(r.getString(1));
        t.setTable(r.getString(2));
        t.setFiresCode(translateFires(r.getString(3)));
        t.setForeachCode(translateForeach(r.getString(3)));
        t.setInsert(translateIsInsert(r.getString(4)));
        t.setUpdate(translateIsUpdate(r.getString(4)));
        t.setDelete(translateIsDelete(r.getString(4)));
        t.setBody(translatePLSQLBody(r.getString(5)));
        return t;
      }
    });
  }

  protected Collection readFunctions() throws SQLException {

    return readList(_stmt_listfunctions, new RowConstructor() {
      @Override
      public Object getRow(ResultSet r) throws SQLException {
        _log.debug("Function " + r.getString(1));
        return readFunction(r.getString(1));
      }
    });
  }

  abstract protected Function readFunction(String name) throws SQLException;

  protected String removeLineComments(String scode) {

    StringBuffer sreturn = new StringBuffer();
    BufferedReader bf = new BufferedReader(new StringReader(scode));

    try {
      String line;
      while ((line = bf.readLine()) != null) {
        int i = line.indexOf("--");
        if (i >= 0) {
          sreturn.append(line.substring(0, i));
        } else {
          sreturn.append(line);
        }
        sreturn.append('\n');
      }
    } catch (IOException ex) {
      // never happens
    }

    return sreturn.toString();
  }

  protected String readFunctionCode(String function) throws SQLException {

    final StringBuffer code = new StringBuffer();

    _stmt_functioncode.setString(1, function);
    fillList(_stmt_functioncode, new RowFiller() {
      @Override
      public void fillRow(ResultSet r) throws SQLException {
        code.append(r.getString(1));
      }
    });

    return code.toString();
  }

  protected int translateMode(String value) {
    if ("IN".equalsIgnoreCase(value)) {
      return Parameter.MODE_IN;
    } else if ("OUT".equalsIgnoreCase(value)) {
      return Parameter.MODE_OUT;
    } else {
      return Parameter.MODE_NONE;
    }
  }

  protected int translateFires(String value) {
    return value.startsWith("BEFORE") ? Trigger.FIRES_BEFORE : Trigger.FIRES_AFTER;
  }

  protected int translateForeach(String value) {
    return value.endsWith("EACH ROW") ? Trigger.FOR_EACH_ROW : Trigger.FOR_EACH_STATEMENT;
  }

  protected boolean translateIsInsert(String value) {
    return value.contains("INSERT");
  }

  protected boolean translateIsUpdate(String value) {
    return value.contains("UPDATE");
  }

  protected boolean translateIsDelete(String value) {
    return value.contains("DELETE");
  }

  protected String translatePLSQLBody(String value) {
    String body = value.trim();
    if (body.startsWith("DECLARE")) {
      body = body.substring(7);
    }
    if (body.endsWith(";")) {
      body = body.substring(0, body.length() - 1);
    }
    return body;
  }

  protected String translateCheckCondition(String code) {
    if (code.substring(0, 1).equals("\n")) {
      code = code.substring(1);
    }
    if (code.substring(code.length() - 1, code.length()).equals("\n")) {
      code = code.substring(0, code.length() - 1);
    }
    return code;
  }

  protected String translateSQL(String sql) {
    if (sql.substring(0, 1).equals("\n")) {
      sql = sql.substring(1);
    }
    if (sql.substring(sql.length() - 1, sql.length()).equals("\n")) {
      sql = sql.substring(0, sql.length() - 1);
    }
    return sql;
  }

  protected boolean translateUniqueness(String uniqueness) {

    return "UNIQUE".equalsIgnoreCase(uniqueness);
  }

  protected String translateColumnDefault(String value, int type) {
    String translatedValue;
    if (value == null) {
      return null;
    } else {
      translatedValue = value.trim();
      if (translatedValue.equalsIgnoreCase("NULL")) {
        return null;
      } else {
        return translateDefault(translatedValue, type);
      }
    }
  }

  protected String translateParamDefault(String value, int type) {

    if (value == null) {
      return null;
    } else {
      return translateDefault(value.trim(), type);
    }
  }

  protected abstract String readName();

  protected abstract int translateFKEvent(String fkevent);

  protected abstract String translateDefault(String value, int type);

  protected abstract boolean translateRequired(String required);

  protected abstract int translateParamType(String nativeType);

  protected abstract int translateColumnType(String nativeType);

  protected List readList(PreparedStatement stmt, RowConstructor r) throws SQLException {

    List l = new ArrayList();
    ResultSet rs = null;

    try {
      rs = stmt.executeQuery();
      while (rs.next()) {
        try {
          l.add(r.getRow(rs));
        } catch (InvalidRowException e) {
          // just skip the current row
          getLog().warn("Skipping row: " + e.getMessage());
        }
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
    return l;
  }

  protected Object readRow(PreparedStatement stmt, RowConstructor r) throws SQLException {

    ResultSet rs = null;

    try {
      rs = stmt.executeQuery();
      if (rs.next()) {
        return r.getRow(rs);
      } else {
        return null;
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
  }

  protected void fillRow(PreparedStatement stmt, RowFiller r) throws SQLException {

    ResultSet rs = null;

    try {
      rs = stmt.executeQuery();
      if (rs.next()) {
        r.fillRow(rs);
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
  }

  protected void fillList(PreparedStatement stmt, RowFiller r) throws SQLException {

    ResultSet rs = null;

    try {
      rs = stmt.executeQuery();
      while (rs.next()) {
        r.fillRow(rs);
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
  }

  public boolean isOnlyLoadTableColumns() {
    return onlyLoadTableColumns;
  }

  @Override
  public void setOnlyLoadTableColumns(boolean onlyLoadTableColumns) {
    this.onlyLoadTableColumns = onlyLoadTableColumns;
  }

  @Override
  public void addAdditionalTableIfExists(Connection connection, Database model, String tablename) {
    try {
      this._connection = connection;
      initMetadataSentences();
      List tablenames = readTableNames();
      for (int i = 0; i < tablenames.size(); i++) {
        String tablenameDB = (String) tablenames.get(i);
        if (tablenameDB.equals(tablename)) {
          // The table exists, we need to load it.
          Table table = readTable(tablename, false);
          model.addTable(table);
        }
      }
    } catch (Exception e) {
      getLog().error(e);
    }
  }

  @Override
  public void setMaxThreads(int maxThreads) {
    this.maxThreads = maxThreads;
  }

  protected int getMaxThreads() {
    return maxThreads;
  }
}
