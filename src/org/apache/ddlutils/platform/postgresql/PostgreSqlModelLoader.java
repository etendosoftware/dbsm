/*
 ************************************************************************************
 * Copyright (C) 2001-2018 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.postgresql;

import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Unique;
import org.apache.ddlutils.platform.ModelLoaderBase;
import org.apache.ddlutils.platform.RowConstructor;
import org.apache.ddlutils.platform.RowFiller;
import org.apache.ddlutils.translation.Translation;
import org.apache.ddlutils.util.ExtTypes;

/**
 * 
 * @author adrian
 */
public class PostgreSqlModelLoader extends ModelLoaderBase {

  protected PreparedStatement _stmt_functionparams;
  protected PreparedStatement _stmt_functiondefaults;
  protected PreparedStatement _stmt_functiondefaults0;
  protected PreparedStatement _stmt_paramtypes;
  protected PreparedStatement _stmt_oids_funcs;
  protected PreparedStatement _stmt_comments_funcs;
  protected PreparedStatement _stmt_oids_tables;
  protected PreparedStatement _stmt_comments_tables;
  protected PreparedStatement _stmt_column_indexes;

  protected Translation _checkTranslation = new PostgreSqlCheckTranslation();
  protected Translation _SQLTranslation = new PostgreSQLStandarization();

  protected Map<Integer, Integer> _paramtypes = new HashMap<Integer, Integer>();

  private static final int FUNCTION_BASED_COLUMN_INDEX_POSITION = 0;

  /** Creates a new instance of PostgreSqlModelLoader */
  public PostgreSqlModelLoader() {
  }

  @Override
  protected String readName() {
    return "PostgreSql server";
  }

  @Override
  protected Database readDatabase() throws SQLException {
    boolean doPlSqlStandardization = true;
    return readDatabase(doPlSqlStandardization);
  }

  @Override
  protected Database readDatabase(boolean doPlSqlStandardization) throws SQLException {
    Database db = super.readDatabase(doPlSqlStandardization);
    if (doPlSqlStandardization) {
      standardizePlSql(db);
    }
    return db;

  }

  private void standardizePlSql(Database db) {
    long t = System.currentTimeMillis();
    _log.info("Starting function trigger and view standardization in " + getMaxThreads()
        + " threads");
    PostgrePLSQLStandarization.generateOutPatterns(db);

    ExecutorService executor = Executors.newFixedThreadPool(getMaxThreads());

    int functionCnt = db.getFunctionCount();
    for (int i = 0; i < functionCnt; i++) {
      executor.execute(new PostgrePLSQLFunctionConcurrentStandardization(db, i));
    }

    int trgCnt = db.getTriggerCount();
    for (int i = 0; i < trgCnt; i++) {
      executor.execute(new PostgrePLSQLTriggerConcurrentStandardization(db, i));
    }

    int viewCnt = db.getViewCount();
    for (int i = 0; i < viewCnt; i++) {
      executor.execute(new PostgrePLSQLViewConcurrentStandardization(db, i));
    }

    executor.shutdown();
    try {
      // await till actual standardization occurs, this should take few seconds at most
      executor.awaitTermination(20L, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      _log.error("Error completing pl standardization", e);
    }

    _log.info("Standardidized " + functionCnt + " functions, " + trgCnt + " triggers and "
        + viewCnt + " views in " + (System.currentTimeMillis() - t) + " ms");
  }

  @Override
  protected String translatePLSQLBody(String value) {
    String body = value.trim();
    if (body.startsWith("DECLARE ")) {
      body = body.substring(9);
    } else if (body.startsWith("DECLARE")) {
      body = body.substring(8);
    }
    if (body.endsWith(";")) {
      body = body.substring(0, body.length() - 1);
    }
    return body;
  }

  @Override
  protected void initMetadataSentences() throws SQLException {
    String sql;
    boolean firstExpressionInWhereClause = false;
    _stmt_listtables = _connection
        .prepareStatement("SELECT UPPER(TABLENAME) FROM PG_TABLES WHERE SCHEMANAME = CURRENT_SCHEMA() "
            + _filter.getExcludeFilterWhereClause("TABLENAME", _filter.getExcludedTables(),
                firstExpressionInWhereClause) + " ORDER BY UPPER(TABLENAME)");

    sql = "SELECT PG_CONSTRAINT.CONNAME FROM PG_CONSTRAINT JOIN PG_CLASS ON PG_CLASS.OID = PG_CONSTRAINT.CONRELID WHERE PG_CONSTRAINT.CONTYPE = 'p' AND PG_CLASS.RELNAME =  ?";
    _stmt_pkname = _connection.prepareStatement(sql);
    _stmt_pkname_noprefix = _connection.prepareStatement(sql
        + " AND upper(PG_CONSTRAINT.CONNAME::TEXT) NOT LIKE 'EM\\\\_%'");
    _stmt_pkname_prefix = _connection
        .prepareStatement(sql
            + " AND (upper(PG_CONSTRAINT.CONNAME::TEXT) LIKE 'EM_"
            + _prefix
            + "\\\\_%' OR (upper(PG_CONSTRAINT.CONNAME::TEXT)||UPPER(PG_CLASS.RELNAME::TEXT) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "')))");

    sql = "SELECT UPPER(PG_ATTRIBUTE.ATTNAME::TEXT), UPPER(PG_TYPE.TYPNAME::TEXT), "
        + " CASE PG_TYPE.TYPNAME"
        + "     WHEN 'varchar'::name THEN pg_attribute.atttypmod - 4"
        + "     WHEN 'bpchar'::name THEN pg_attribute.atttypmod - 4"
        + "     ELSE NULL::integer"
        + " END,"
        + " CASE PG_TYPE.TYPNAME"
        + "     WHEN 'bytea'::name THEN 4000"
        + "     WHEN 'text'::name THEN 4000"
        + "     WHEN 'oid'::name THEN 4000"
        + "     ELSE CASE PG_ATTRIBUTE.ATTLEN "
        + "              WHEN -1 THEN PG_ATTRIBUTE.ATTTYPMOD - 4 "
        + "              ELSE PG_ATTRIBUTE.ATTLEN "
        + "          END"
        + " END,"
        + " CASE pg_type.typname"
        + "     WHEN 'bytea'::name THEN 4000"
        + "     WHEN 'text'::name THEN 4000"
        + "     WHEN 'oid'::name THEN 4000"
        + "     ELSE"
        + "         CASE atttypmod"
        + "             WHEN -1 THEN 0"
        + "             ELSE numeric_precision"
        + "         END"
        + " END,"
        + " numeric_scale, not pg_attribute.attnotnull,"
        + " CASE pg_attribute.atthasdef"
        + "     WHEN true THEN ( SELECT pg_attrdef.adsrc FROM pg_attrdef WHERE pg_attrdef.adrelid = pg_class.oid AND pg_attrdef.adnum = pg_attribute.attnum)"
        + "     ELSE NULL::text"
        + " END"
        + " FROM pg_class, pg_namespace, pg_attribute, pg_type,information_schema.columns"
        + " WHERE pg_attribute.attrelid = pg_class.oid AND pg_attribute.atttypid = pg_type.oid AND pg_class.relnamespace = pg_namespace.oid AND pg_namespace.nspname = current_schema() AND pg_attribute.attnum > 0 "
        + " AND PG_ATTRIBUTE.ATTNAME = information_schema.columns.column_name"
        + " AND pg_class.relname = information_schema.columns.table_name"
        + " AND pg_class.relname = ? ";
    _stmt_listcolumns = _connection.prepareStatement(sql + " ORDER BY pg_attribute.attnum");
    _stmt_listcolumns_noprefix = _connection.prepareStatement(sql
        + " AND upper(PG_ATTRIBUTE.ATTNAME::TEXT) NOT LIKE 'EM\\\\_%'"
        + " ORDER BY pg_attribute.attnum");
    _stmt_listcolumns_prefix = _connection
        .prepareStatement(sql
            + " AND (upper(PG_ATTRIBUTE.ATTNAME::TEXT) LIKE 'EM_"
            + _prefix
            + "\\\\_%' OR (UPPER(PG_ATTRIBUTE.ATTNAME::TEXT)||UPPER(PG_CLASS.RELNAME::TEXT) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "')))" + " ORDER BY pg_attribute.attnum");

    _stmt_pkcolumns = _connection
        .prepareStatement("SELECT upper(pg_attribute.attname::text)"
            + " FROM pg_constraint, pg_class, pg_attribute"
            + " WHERE pg_constraint.conrelid = pg_class.oid AND pg_attribute.attrelid = pg_constraint.conrelid AND (pg_attribute.attnum = ANY (pg_constraint.conkey))"
            + " AND pg_constraint.conname = ?" + " ORDER BY pg_attribute.attnum::integer");

    sql = "SELECT upper(pg_constraint.conname::text), regexp_replace(pg_get_constraintdef(pg_constraint.oid, true), E'CHECK \\\\((.*)\\\\).*', E'\\\\1')"
        + " FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid"
        + " WHERE pg_constraint.contype = 'c' and pg_class.relname = ?";
    _stmt_listchecks = _connection.prepareStatement(sql
        + " ORDER BY upper(pg_constraint.conname::text)");
    _stmt_listchecks_noprefix = _connection.prepareStatement(sql
        + " AND upper(pg_constraint.conname::text) NOT LIKE 'EM\\\\_%'"
        + " ORDER BY upper(pg_constraint.conname::text)");
    _stmt_listchecks_prefix = _connection
        .prepareStatement(sql
            + " AND (upper(pg_constraint.conname::text) LIKE 'EM_"
            + _prefix
            + "\\\\_%' OR (UPPER(pg_constraint.conname::text)||UPPER(PG_CLASS.RELNAME::TEXT) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "')))" + " ORDER BY upper(pg_constraint.conname::text)");

    sql = "SELECT pg_constraint.conname AS constraint_name, upper(fk_table.relname::text), upper(pg_constraint.confdeltype::text), 'A'"
        + " FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid LEFT JOIN pg_class fk_table ON fk_table.oid = pg_constraint.confrelid"
        + " WHERE pg_constraint.contype = 'f' and pg_class.relname = ?";
    _stmt_listfks = _connection.prepareStatement(sql
        + " ORDER BY upper(pg_constraint.conname::text)");
    _stmt_listfks_noprefix = _connection.prepareStatement(sql
        + " AND upper(pg_constraint.conname::text) NOT LIKE 'EM\\\\_%'"
        + " ORDER BY upper(pg_constraint.conname::text)");
    _stmt_listfks_prefix = _connection
        .prepareStatement(sql
            + " AND (upper(pg_constraint.conname::text) LIKE 'EM_"
            + _prefix
            + "\\\\_%' OR (UPPER(pg_constraint.conname::text)||UPPER(PG_CLASS.RELNAME::TEXT) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "')))" + " ORDER BY upper(pg_constraint.conname::text)");

    _stmt_fkcolumns = _connection
        .prepareStatement("SELECT upper(pa1.attname), upper(pa2.attname)"
            + " FROM pg_constraint pc, pg_class pc1, pg_attribute pa1, pg_class pc2, pg_attribute pa2"
            + " WHERE pc.contype='f' and pc.conrelid= pc1.oid and pc.conname = ? and pa1.attrelid = pc1.oid and pa1.attnum = ANY(pc.conkey)"
            + " and pc.confrelid = pc2.oid and pa2.attrelid = pc2.oid and pa2.attnum = ANY(pc.confkey)");

    sql = "SELECT PG_CLASS.RELNAME, CASE PG_INDEX.indisunique WHEN true THEN 'UNIQUE' ELSE 'NONUNIQUE' END, "
        + " PG_INDEX.indclass, PG_GET_EXPR(PG_INDEX.indpred,PG_INDEX.indrelid,true)"
        + " FROM PG_INDEX, PG_CLASS, PG_CLASS PG_CLASS1, PG_NAMESPACE"
        + " WHERE PG_INDEX.indexrelid = PG_CLASS.OID"
        + " AND PG_INDEX.indrelid = PG_CLASS1.OID"
        + " AND PG_CLASS.RELNAMESPACE = PG_NAMESPACE.OID"
        + " AND PG_CLASS1.RELNAMESPACE = PG_NAMESPACE.OID"
        + " AND PG_NAMESPACE.NSPNAME = CURRENT_SCHEMA()"
        + " AND PG_INDEX.INDISPRIMARY ='f'"
        + " AND PG_CLASS1.RELNAME = ?"
        + " AND PG_CLASS.RELNAME NOT IN (SELECT pg_constraint.conname::text "
        + "    FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid"
        + "    WHERE pg_constraint.contype = 'u')";

    _stmt_listindexes = _connection.prepareStatement(sql + " ORDER BY UPPER(PG_CLASS.RELNAME)");
    _stmt_listindexes_noprefix = _connection.prepareStatement(sql
        + " AND upper(PG_CLASS.RELNAME) NOT LIKE 'EM\\\\_%'" + " ORDER BY UPPER(PG_CLASS.RELNAME)");
    _stmt_listindexes_prefix = _connection
        .prepareStatement(sql
            + " AND (upper(PG_CLASS.RELNAME) LIKE 'EM_"
            + _prefix
            + "\\\\_%' OR (UPPER(PG_CLASS.RELNAME::TEXT)||UPPER(PG_CLASS1.RELNAME::TEXT) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "')))" + " ORDER BY UPPER(PG_CLASS.RELNAME)");

    // 1. index def: can be column name for standard indexes, or expression if function based
    // 2. key: 0-> identifies function based index, any other value -> standard column index
    _stmt_indexcolumns = _connection
        .prepareStatement("select pg_get_indexdef(c.oid, an, true), indkey[an-1]\n"
            + "from pg_index i,pg_class c, generate_series(1, i.indnatts) an\n"
            + "where c.oid = i.indexrelid and c.relname = ?\n" //
            + "order by an");

    sql = "SELECT pg_constraint.conname"
        + " FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid"
        + " WHERE pg_constraint.contype = 'u' AND pg_class.relname = ?";
    _stmt_listuniques = _connection.prepareStatement(sql
        + " ORDER BY upper(pg_constraint.conname::text)");
    _stmt_listuniques_noprefix = _connection.prepareStatement(sql
        + " AND upper(PG_CLASS.RELNAME) NOT LIKE 'EM\\\\_%'"
        + " ORDER BY upper(pg_constraint.conname::text)");
    _stmt_listuniques_prefix = _connection
        .prepareStatement(sql
            + " AND (upper(pg_constraint.conname::text) LIKE 'EM_"
            + _prefix
            + "\\\\_%' OR (UPPER(pg_constraint.conname::text)||UPPER(PG_CLASS.RELNAME::TEXT) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "')))" + " ORDER BY upper(pg_constraint.conname::text)");

    _stmt_uniquecolumns = _connection
        .prepareStatement("SELECT upper(pg_attribute.attname::text), pg_attribute.attnum, array_to_string(pg_constraint.conkey,',') "
            + " FROM pg_constraint, pg_class, pg_attribute"
            + " WHERE pg_constraint.conrelid = pg_class.oid AND pg_attribute.attrelid = pg_constraint.conrelid AND (pg_attribute.attnum = ANY (pg_constraint.conkey))"
            + " AND pg_constraint.conname = ?" + " ORDER BY pg_attribute.attnum");
    firstExpressionInWhereClause = false;
    sql = "SELECT upper(viewname), pg_get_viewdef(viewname, true) FROM pg_views "
        + "WHERE SCHEMANAME = CURRENT_SCHEMA() AND viewname !~ '^pg_'"
        + _filter.getExcludeFilterWhereClause("viewname", _filter.getExcludedViews(),
            firstExpressionInWhereClause);
    if (_prefix != null) {
      sql += " AND (upper(viewname) LIKE '"
          + _prefix
          + "\\\\_%' OR (upper(viewname) IN (SELECT upper(name1) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
          + _moduleId + "')))";
    }
    _stmt_listviews = _connection.prepareStatement(sql);

    // sequences are partially loaded from catalog, details (start number and increment) are read by
    // readSequences method
    firstExpressionInWhereClause = false;
    sql = "SELECT upper(relname), oid FROM pg_class WHERE relkind = 'S' "
        + _filter.getExcludeFilterWhereClause("relname", _filter.getExcludedSequences(),
            firstExpressionInWhereClause);
    if (_prefix != null) {
      sql += " AND upper(relname) LIKE '" + _prefix + "\\\\_%'";
    }
    _stmt_listsequences = _connection.prepareStatement(sql);
    firstExpressionInWhereClause = false;
    sql = "SELECT upper(trg.tgname) AS trigger_name, upper(tbl.relname) AS table_name, "
        + "CASE trg.tgtype & cast(3 as int2) "
        + "WHEN 0 THEN 'AFTER EACH STATEMENT' "
        + "WHEN 1 THEN 'AFTER EACH ROW' "
        + "WHEN 2 THEN 'BEFORE EACH STATEMENT' "
        + "WHEN 3 THEN 'BEFORE EACH ROW' "
        + "END AS trigger_type, "
        + "CASE trg.tgtype & cast(28 as int2) WHEN 16 THEN 'UPDATE' "
        + "WHEN  8 THEN 'DELETE' "
        + "WHEN  4 THEN 'INSERT' "
        + "WHEN 20 THEN 'INSERT, UPDATE' "
        + "WHEN 28 THEN 'INSERT, UPDATE, DELETE' "
        + "WHEN 24 THEN 'UPDATE, DELETE' "
        + "WHEN 12 THEN 'INSERT, DELETE' "
        + "END AS trigger_event, "
        + "p.prosrc AS function_code "
        + "FROM pg_trigger trg, pg_class tbl, pg_proc p "
        + "WHERE trg.tgrelid = tbl.oid AND trg.tgfoid = p.oid AND tbl.relname !~ '^pg_' AND trg.tgname !~ '^RI'"
        + " AND upper(trg.tgname) NOT LIKE 'AU\\\\_%'"
        + _filter.getExcludeFilterWhereClause("trg.tgname", _filter.getExcludedTriggers(),
            firstExpressionInWhereClause);

    if (_prefix != null) {
      sql += "AND (upper(trg.tgname) LIKE '"
          + _prefix
          + "\\\\_%' OR (upper(trg.tgname) IN (SELECT upper(name1) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
          + _moduleId + "')))";
    }
    _stmt_listtriggers = _connection.prepareStatement(sql);
    firstExpressionInWhereClause = false;
    sql = "select distinct proname from pg_proc p, pg_namespace n "
        + "where  pronamespace = n.oid "
        + "and n.nspname=current_schema() "
        + "and p.oid not in (select tgfoid "
        + "from pg_trigger) "
        + _filter.getExcludeFilterWhereClause("p.proname", _filter.getExcludedFunctions(),
            firstExpressionInWhereClause);
    if (_prefix != null) {
      sql += " AND (upper(proname) LIKE '"
          + _prefix
          + "\\\\_%' OR (upper(proname) IN (SELECT upper(name1) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
          + _moduleId + "')))";
    }
    _stmt_listfunctions = _connection.prepareStatement(sql);

    _stmt_functioncode = _connection
        .prepareStatement("select p.prosrc FROM pg_proc p WHERE UPPER(p.proname) = ?"); // dummy
    // sentence

    _stmt_functionparams = _connection.prepareStatement("  SELECT "
        + "         pg_proc.prorettype," + "         pg_proc.proargtypes,"
        + "         pg_proc.proallargtypes," + "         pg_proc.proargmodes,"
        + "         pg_proc.proargnames," + "         pg_proc.prosrc"
        + "    FROM pg_catalog.pg_proc" + "         JOIN pg_catalog.pg_namespace"
        + "         ON (pg_proc.pronamespace = pg_namespace.oid)"
        + "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype"
        + "     AND (pg_proc.proargtypes[0] IS NULL"
        + "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)"
        + "     AND NOT pg_proc.proisagg"
        + "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)"
        + "     AND pg_proc.proname = ?"
        + "         ORDER BY pg_proc.pronargs DESC, length(pg_proc.prosrc) DESC");
    // we order by pronargs to get the correct source when a function is
    // overridden by DBSourceManager
    // (because of default parameters).
    // we order by source to get the correct source when two functions have
    // the same number of parameters
    // and they are different

    _stmt_functiondefaults = _connection.prepareStatement("  SELECT " + "         pg_proc.proname,"
        + "         pg_proc.proargtypes," + "         pg_proc.proallargtypes,"
        + "         pg_proc.prosrc" + "    FROM pg_catalog.pg_proc"
        + "         JOIN pg_catalog.pg_namespace"
        + "         ON (pg_proc.pronamespace = pg_namespace.oid)"
        + "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype"
        + "     AND (pg_proc.proargtypes[0] IS NULL"
        + "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)"
        + "     AND NOT pg_proc.proisagg"
        + "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)"
        + "     AND pg_proc.proname = ? ORDER BY pg_proc.pronargs ASC");

    _stmt_functiondefaults0 = _connection.prepareStatement("  SELECT "
        + "         pg_proc.proname," + "         pg_proc.proargtypes,"
        + "         pg_proc.proallargtypes," + "         pg_proc.prosrc"
        + "    FROM pg_catalog.pg_proc" + "         JOIN pg_catalog.pg_namespace"
        + "         ON (pg_proc.pronamespace = pg_namespace.oid)"
        + "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype"
        + "     AND (pg_proc.proargtypes[0] IS NULL"
        + "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)"
        + "     AND NOT pg_proc.proisagg"
        + "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)"
        + "     AND pg_proc.proname = ? ORDER BY pg_proc.proargtypes ASC");

    _stmt_paramtypes = _connection.prepareStatement("SELECT pg_catalog.format_type(?, NULL)");

    _stmt_oids_funcs = _connection
        .prepareStatement("SELECT oid, proconfig FROM pg_proc WHERE proname = ?");

    _stmt_comments_funcs = _connection.prepareStatement("SELECT obj_description(?,'pg_proc')");

    _stmt_oids_tables = _connection
        .prepareStatement("SELECT oid, relname FROM pg_class WHERE upper(relname) = ?");

    _stmt_comments_tables = _connection.prepareStatement("SELECT col_description(?,?)");

    _stmt_column_indexes = _connection
        .prepareStatement("SELECT ordinal_position FROM information_schema.columns WHERE table_name = ? order by ordinal_position;");

  }

  @Override
  protected void closeMetadataSentences() throws SQLException {
    super.closeMetadataSentences();
    _stmt_functionparams.close();
    _stmt_paramtypes.close();

  }

  int numDefaults;
  int numDefaultsDif;
  int numRemDefaults;
  Vector<String> paramsNVARCHAR = new Vector<String>();
  int oidFunc;
  String comment = "";
  Array configs = null;

  @Override
  protected Function readFunction(String name) throws SQLException {

    final Function f = new Function();
    f.setName(name.toUpperCase());

    final FinalBoolean firststep = new FinalBoolean();

    _stmt_functionparams.setString(1, name);
    _stmt_functiondefaults.setString(1, name);
    _stmt_functiondefaults0.setString(1, name + "0");
    numDefaults = 0;
    numDefaultsDif = 0;

    numRemDefaults = 0;

    _stmt_oids_funcs.setString(1, name);
    fillList(_stmt_oids_funcs, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {
        oidFunc = r.getInt(1);
        configs = r.getArray(2);
      }
    });

    boolean hasSearchPath = false;
    if (configs != null && configs.getArray() instanceof String[]) {
      String[] confs = (String[]) configs.getArray();
      for (String conf : confs) {
        hasSearchPath = conf != null && conf.startsWith("search_path=");
        if (hasSearchPath) {
          break;
        }
      }
    }
    // If function in DB has no search path, force recreation to include it.
    // If it already has it, don't do it: it could be it already has set by DBSM or it was manually
    // set, in this case don't overwrite
    f.setRecreationRequired(!hasSearchPath);

    _stmt_comments_funcs.setInt(1, oidFunc);
    fillList(_stmt_comments_funcs, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {
        comment = r.getString(1);
      }
    });

    if (comment != null && !comment.equals("")) {
      Pattern pat = Pattern.compile("--OBTG:(.*)--");
      Matcher match = pat.matcher(comment);
      if (match.matches()) {
        comment = match.group(1);
        Pattern elem = Pattern.compile("(.*)=(.*)");
        StringTokenizer tok = new StringTokenizer(comment, ",");
        while (tok.hasMoreTokens()) {
          String token = tok.nextToken();
          Matcher el = elem.matcher(token);
          if (el.matches()) {
            paramsNVARCHAR.add(el.group(1));
          }
        }
      }
    }

    fillList(_stmt_functionparams, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {

        if (firststep.get()) {
          // just set defaults
          Integer[] atypes = getIntArray(r, 2);
          Integer[] aalltypes = getIntArray2(r, 3);
          if (aalltypes != null) {
            atypes = aalltypes;
          }

          /*
           * for (int i = atypes.length; i < f.getParameterCount(); i++) {
           * f.getParameter(i).setDefaultValue("0"); // a dummy default value }
           */
          numDefaults++;

        } else {
          int ireturn = r.getInt(1);
          Integer[] atypes = getIntArray(r, 2);
          Integer[] aalltypes = getIntArray2(r, 3);
          String[] modes = getStringArray(r, 4);
          String[] names = getStringArray(r, 5);

          Iterator<String> itf = paramsNVARCHAR.iterator();
          boolean nvarcf = false;
          while (itf.hasNext() && !nvarcf) {
            String np = itf.next();
            if (np.equalsIgnoreCase(f.getName() + "func")) {
              f.setTypeCode(ExtTypes.NVARCHAR);
              nvarcf = true;
            }
          }
          if (!nvarcf) {
            if (aalltypes == null) {
              f.setTypeCode(getParamType(ireturn));
            } else {
              f.setTypeCode(Types.NULL);
              atypes = aalltypes;
            }
          }

          for (int i = 0; i < atypes.length; i++) {
            Parameter p = new Parameter();
            if (names != null) {
              p.setName(names[i]);
            }
            Iterator<String> it = paramsNVARCHAR.iterator();
            boolean nvarc = false;
            while (it.hasNext() && !nvarc) {
              String np = it.next();
              if (np.equalsIgnoreCase(p.getName())) {
                p.setTypeCode(ExtTypes.NVARCHAR);
                nvarc = true;
              }
            }
            if (!nvarc)
              p.setTypeCode(getParamType(atypes[i]));
            if (modes == null) {
              p.setModeCode(Parameter.MODE_IN);
            } else {
              p.setModeCode("i".equals(modes[i]) ? Parameter.MODE_IN : Parameter.MODE_OUT);
            }

            f.addParameter(p);
          }
          firststep.set(true);

          f.setBody(translatePLSQLBody(r.getString(6)));

          numDefaults = 0;
        }
      }
    });
    firststep.set(false);
    paramsNVARCHAR.clear();

    numRemDefaults = numDefaults;

    fillList(_stmt_functiondefaults, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {

        // int numParams=f.getParameterCount();
        if (numRemDefaults > 0) {
          Integer[] types = getIntArray(r, 2);
          Integer[] alltypes = getIntArray2(r, 3);
          int numParamsMin = numRemDefaults;// types.length;
          // if(alltypes!=null && alltypes.length>numParamsMin)
          // numParamsMin=alltypes.length;
          // System.out.println(r.getString(1)+": "+numParams+";;;"+numParamsMin);
          try {
            String bodyMin = r.getString(4);
            String patternSearched = "" + f.getName().toUpperCase() + "(.*)\\)";
            Pattern pattern = Pattern.compile(patternSearched);
            Matcher matcher = pattern.matcher(bodyMin);
            if (matcher.find()) {
              String defaults = matcher.group(1).trim();
              // System.out.println("hemos encontrado: "+defaults);
              defaults = defaults.substring(1, defaults.length());
              // System.out.println("default: "+defaults);
              StringTokenizer sT = new StringTokenizer(defaults);

              Vector<String> strs = new Vector<String>();
              while (sT.hasMoreTokens())
                strs.add(sT.nextToken());

              String pvalue = strs.lastElement().replaceAll("'", "");
              // System.out.println("intento meter: "+pvalue);
              f.getParameter(f.getParameterCount() - numRemDefaults).setDefaultValue(
                  PostgrePLSQLStandarization.translateDefault(pvalue));

              numRemDefaults--;
            } else {
              System.out.println("Error reading default values for parameters in function "
                  + r.getString(1) + " (pattern not found)");
            }
          } catch (Exception e) {
            System.out.println("Error reading default values for parameters in function "
                + r.getString(1) + ": " + e.toString());
            // System.out.println("We'll try to find them in function "+r.getString(1)+"0");

          }
        }

      }
    });

    if (numRemDefaults != 0) {
      System.out.println("Error reading default values for parameters in function " + name);
    }

    return f;
  }

  int oidTable;
  // tablename from readTable with the exact case like in the database
  String tableRealName;
  String commentCol;

  ArrayList<Integer> columnIndexes;

  @Override
  protected Table readTable(String tablename, boolean usePrefix) throws SQLException {
    _stmt_oids_tables.setString(1, tablename);
    fillList(_stmt_oids_tables, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {
        oidTable = r.getInt(1);
        tableRealName = r.getString(2);
      }
    });

    Table t = super.readTable(tableRealName, usePrefix);

    columnIndexes = new ArrayList<Integer>();
    _stmt_column_indexes.setString(1, tableRealName);
    fillList(_stmt_column_indexes, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {
        columnIndexes.add(r.getInt(1));
      }
    });

    // We'll change the types of NVarchar columns (which should have a
    // comment in the database)
    for (int i = 0; i < t.getColumnCount(); i++) {
      _stmt_comments_tables.setInt(1, oidTable);
      _stmt_comments_tables.setInt(2, columnIndexes.get(i));
      fillList(_stmt_comments_tables, new RowFiller() {
        public void fillRow(ResultSet r) throws SQLException {
          commentCol = r.getString(1);
        }
      });
      if (commentCol != null && !commentCol.equals("")) {
        Pattern pat = Pattern.compile("--OBTG:NVARCHAR--");
        Matcher match = pat.matcher(commentCol);
        if (match.find()) {
          t.getColumn(i).setTypeCode(ExtTypes.NVARCHAR);
        }
        Pattern pat2 = Pattern.compile("--OBTG:NCHAR--");
        Matcher match2 = pat2.matcher(commentCol);
        if (match2.find()) {
          t.getColumn(i).setTypeCode(ExtTypes.NCHAR);
        }

        Pattern pat3 = Pattern.compile("--OBTG:ONCREATEDEFAULT:(.*)--");
        Matcher match3 = pat3.matcher(commentCol);
        if (match3.find()) {
          t.getColumn(i).setOnCreateDefault(match3.group(1));
        }
      }
    }

    return t;
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected Collection readSequences() throws SQLException {
    return readList(_stmt_listsequences, new RowConstructor() {
      public Object getRow(ResultSet r) throws SQLException {
        _log.debug("Sequence " + r.getString(1));
        final Sequence sequence = new Sequence();
        String sequenceName = r.getString(1);
        sequence.setName(sequenceName);

        // PG does not store sequence details in catalog but in a pseudo-table named with the
        // sequence name, let's query them to get the details
        String sql = "select min_value, increment_by from " + sequenceName;
        PreparedStatement stmtCurrentSequence = _connection.prepareStatement(sql);
        readList(stmtCurrentSequence, new RowConstructor() {
          @Override
          public Object getRow(ResultSet seqDetails) throws SQLException {
            sequence.setStart(seqDetails.getInt(1));
            sequence.setIncrement(seqDetails.getInt(2));
            return sequence;
          }
        });

        stmtCurrentSequence.close();

        return sequence;
      }
    });
  }

  protected Integer[] getIntArray(ResultSet r, int iposition) throws SQLException {
    String s = r.getString(iposition);
    if (s == null) {
      return null;
    } else {
      ArrayList<Integer> list = new ArrayList<Integer>();

      StringTokenizer st = new StringTokenizer(s);

      while (st.hasMoreTokens()) {
        list.add(Integer.parseInt(st.nextToken()));
      }

      return list.toArray(new Integer[list.size()]);
    }
  }

  protected Integer[] getIntArray2(ResultSet r, int iposition) throws SQLException {
    Array sqlArray = r.getArray(iposition);
    if (sqlArray == null) {
      return null;
    }

    Object array = sqlArray.getArray();
    if (array instanceof Long[]) {
      List<Integer> intArray = new ArrayList<Integer>();
      for (Long l : (Long[]) array) {
        intArray.add(l.intValue());
      }
      return intArray.toArray(new Integer[intArray.size()]);
    } else {
      return (Integer[]) array;
    }
  }

  protected String[] getStringArray(ResultSet r, int iposition) throws SQLException {
    Array sqlArray = r.getArray(iposition);
    if (sqlArray == null) {
      return null;
    }
    return (String[]) sqlArray.getArray();
  }

  protected int getParamType(int pgtype) throws SQLException {

    if (!_paramtypes.containsKey(pgtype)) {

      _stmt_paramtypes.setInt(1, pgtype);
      String stype = (String) readRow(_stmt_paramtypes, new RowConstructor() {
        public Object getRow(ResultSet r) throws SQLException {
          return r.getString(1);
        }
      });

      _paramtypes.put(pgtype, translateParamType(stype));
    }

    return _paramtypes.get(pgtype);
  }

  @Override
  protected String translateCheckCondition(String code) {
    return _checkTranslation.exec(code);
  }

  @Override
  protected String translateSQL(String sql) {
    return _SQLTranslation.exec(sql).trim();
  }

  @Override
  protected boolean translateRequired(String required) {
    return "f".equals(required);
  }

  @Override
  protected String translateDefault(String value, int type) {

    switch (type) {
    case Types.CHAR:
    case Types.VARCHAR:
    case ExtTypes.NCHAR:
    case ExtTypes.NVARCHAR:
    case Types.LONGVARCHAR:
      if (value.endsWith("::character varying")) {
        value = value.substring(0, value.length() - 19);
      }
      if (value.endsWith("::bpchar")) {
        value = value.substring(0, value.length() - 8);
      }

      if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
        value = value.substring(1, value.length() - 1);
        int i = 0;
        StringBuffer sunescaped = new StringBuffer();
        while (i < value.length()) {
          char c = value.charAt(i);
          if (c == '\'') {
            i++;
            if (i < value.length()) {
              sunescaped.append(c);
              i++;
            }
          } else {
            sunescaped.append(c);
            i++;
          }
        }
        if (sunescaped.length() == 0)
          return null;
        else
          return sunescaped.toString();
      } else {
        return value;
      }
    case Types.TIMESTAMP:
      if ("now()".equalsIgnoreCase(value)) {
        return "SYSDATE";
      } else {
        return value;
      }
    default:
      return value;
    }
  }

  @Override
  protected int translateColumnType(String nativeType) {

    if (nativeType == null) {
      return Types.NULL;
    } else if ("BPCHAR".equalsIgnoreCase(nativeType)) {
      return Types.CHAR;
    } else if ("VARCHAR".equalsIgnoreCase(nativeType)) {
      return Types.VARCHAR;
    } else if ("NUMERIC".equalsIgnoreCase(nativeType)) {
      return Types.DECIMAL;
    } else if ("TIMESTAMP".equalsIgnoreCase(nativeType)) {
      return Types.TIMESTAMP;
    } else if ("TEXT".equalsIgnoreCase(nativeType)) {
      return Types.CLOB;
    } else if ("BYTEA".equalsIgnoreCase(nativeType)) {
      return Types.BLOB;
    } else {
      return Types.OTHER;
    }
  }

  @Override
  protected int translateParamType(String nativeType) {

    if (nativeType == null) {
      return Types.NULL;
    } else if ("VOID".equalsIgnoreCase(nativeType)) {
      return Types.NULL;
    } else if ("CHARACTER".equalsIgnoreCase(nativeType)) {
      return Types.CHAR;
    } else if ("BPCHAR".equalsIgnoreCase(nativeType)) {
      return Types.CHAR;
    } else if ("VARCHAR".equalsIgnoreCase(nativeType)) {
      return Types.VARCHAR;
    } else if ("NUMERIC".equalsIgnoreCase(nativeType)) {
      return Types.NUMERIC;
    } else if ("TIMESTAMP".equalsIgnoreCase(nativeType)) {
      return Types.TIMESTAMP;
    } else if (nativeType.startsWith("timestamp")) {
      return Types.TIMESTAMP;
    } else if ("TEXT".equalsIgnoreCase(nativeType)) {
      return Types.CLOB;
    } else if ("BYTEA".equalsIgnoreCase(nativeType)) {
      return Types.BLOB;
    } else if ("OID".equalsIgnoreCase(nativeType)) {
      return Types.OTHER;
    } else {
      return Types.VARCHAR;
    }
  }

  @Override
  protected int translateFKEvent(String fkevent) {
    if ("C".equalsIgnoreCase(fkevent)) {
      return DatabaseMetaData.importedKeyCascade;
    } else if ("N".equalsIgnoreCase(fkevent)) {
      return DatabaseMetaData.importedKeySetNull;
    } else if ("R".equalsIgnoreCase(fkevent)) {
      return DatabaseMetaData.importedKeyRestrict;
    } else {
      return DatabaseMetaData.importedKeyNoAction;
    }
  }

  private static class FinalBoolean {
    private boolean b = false;

    public boolean get() {
      return b;
    }

    public void set(boolean value) {
      b = value;
    }
  }

  @Override
  protected Index readIndex(ResultSet rs) throws SQLException {
    String indexRealName = rs.getString(1);
    String indexName = indexRealName.toUpperCase();
    String indexWhereClause = rs.getString(4);

    final Index inx = new Index();

    inx.setName(indexName);
    inx.setUnique(translateUniqueness(rs.getString(2)));
    if (indexWhereClause != null && !indexWhereClause.isEmpty()) {
      inx.setWhereClause(transformIndexExpression(indexWhereClause));
    }

    _stmt_indexcolumns.setString(1, indexRealName);
    fillList(_stmt_indexcolumns, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {
        IndexColumn indexColumn;
        if (r.getInt(2) == FUNCTION_BASED_COLUMN_INDEX_POSITION) {
          indexColumn = getIndexColumnFromExpression(r.getString(1));
        } else {
          indexColumn = new IndexColumn(r.getString(1).toUpperCase());
        }
        inx.addColumn(indexColumn);
      }
    });

    // Obtain the operatorClassOids of each index column
    String[] operatorClassOids = rs.getString(3).split(" ");
    int i = 0;
    int trgmOperators = 0;
    for (IndexColumn indexColumn : inx.getColumns()) {
      String operatorClassName = getOperatorClassName(operatorClassOids[i++]);
      String operatorClassNameUpperCased = operatorClassName.toUpperCase();
      if (operatorClassNameUpperCased.contains("PATTERN")) {
        indexColumn.setOperatorClass(operatorClassName);
      } else if (operatorClassNameUpperCased.equals("GIN_TRGM_OPS")) {
        trgmOperators++;
      }
    }
    inx.setContainsSearch(trgmOperators == inx.getColumns().length);
    return inx;
  }

  /**
   * Given the OID of an operator class, returns its name
   * 
   * @param operatorClassOid
   *          the OID of an operator class
   * @return the name of the operator class
   */
  private String getOperatorClassName(String operatorClassOid) {
    String operatorClassName = null;
    try {
      PreparedStatement st = _connection
          .prepareStatement("select opcname from PG_OPCLASS where oid = ?");
      st.setInt(1, Integer.parseInt(operatorClassOid));
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        operatorClassName = rs.getString(1);
      }
    } catch (SQLException e) {
      _log.error("Error while getting the name of the operator class with oid " + operatorClassOid,
          e);
    }
    return operatorClassName;
  }

  private IndexColumn getIndexColumnFromExpression(String indexExpression) {
    String transformedExpression = transformIndexExpression(indexExpression);
    IndexColumn inxcol = new IndexColumn();
    inxcol.setName("functionBasedColumn");
    inxcol.setFunctionExpression(transformedExpression);
    return inxcol;
  }

  /**
   * Remove the ::type, uppercase everything that is not between quotes and remove the whitespaces
   * 
   * @param indexExpression
   *          the index expression to be transformed
   * @return the transformed index expression
   */
  private String transformIndexExpression(String indexExpression) {
    String transformedIndexExpression = removeNewLines(indexExpression);
    transformedIndexExpression = removeCastExpressions(transformedIndexExpression);
    transformedIndexExpression = transformUnquotedPartOfExpression(transformedIndexExpression);
    transformedIndexExpression = removeParentheses(transformedIndexExpression);
    return transformedIndexExpression;
  }

  private String removeNewLines(String indexExpression) {
    String transformedExpression = "";
    for (String line : indexExpression.split("\n")) {
      transformedExpression += line.trim() + " ";
    }

    return transformedExpression.trim();
  }

  private String removeCastExpressions(String indexExpression) {
    // casts will start with :: and end either with ')', ',' or ' '
    // we replace all characters between '::' and one of these ending characters

    String castExpressionRegExp = "(:{2}[a-zA-Z0-9]+)([ \\),])?";
    int replacementGroup = 2;
    return replaceRegularExpressionMatchings(indexExpression, castExpressionRegExp,
        replacementGroup);
  }

  /**
   * In some occasions, PG adds extra parenthesis to beginning and end of the expression, remove
   * them as they will be always added when the index is created. In this way, the expression gets
   * identically exported in ORA and PG.
   */
  private String removeParentheses(String expr) {
    if (expr.startsWith("(") && expr.endsWith(")")) {
      return expr.substring(1, expr.length() - 1);
    }
    return expr;
  }

  private String replaceRegularExpressionMatchings(String string, String regExp, int groupIndex) {
    Pattern pattern = Pattern.compile(regExp);
    Matcher matcher = pattern.matcher(string);
    StringBuffer buffer = new StringBuffer();
    while (matcher.find()) {
      String replacement = matcher.group(groupIndex) != null ? matcher.group(groupIndex) : "";
      matcher.appendReplacement(buffer, replacement);
    }
    matcher.appendTail(buffer);
    return buffer.toString();
  }

  /**
   * Uppercases and remove whitespaces from the unquoted parts of the expression
   * 
   */
  private String transformUnquotedPartOfExpression(String indexExpression) {
    String transformedIndexExpression = indexExpression;
    int openingQuoteIndex = indexExpression.indexOf("'");
    if (openingQuoteIndex == -1) {
      // there is no text between quotes, leave arguments separated just by commas (by removing
      // whitespaces) and capitalize all the string
      transformedIndexExpression = removeExtraWhiteSpaces(transformedIndexExpression).toUpperCase();
    } else {
      // capitalize and remove whitespaces only from the text that is not between quotes
      StringBuilder expressionBuilder = new StringBuilder();
      int closingQuoteIndex = -1;
      while (openingQuoteIndex != -1) {
        String unquotedExpression = transformedIndexExpression.substring(closingQuoteIndex + 1,
            openingQuoteIndex).toUpperCase();
        expressionBuilder.append(removeExtraWhiteSpaces(unquotedExpression));
        closingQuoteIndex = indexExpression.indexOf("'", openingQuoteIndex + 1);
        expressionBuilder.append(transformedIndexExpression.substring(openingQuoteIndex,
            closingQuoteIndex + 1));
        openingQuoteIndex = indexExpression.indexOf("'", closingQuoteIndex + 1);
      }
      expressionBuilder.append(transformedIndexExpression.substring(closingQuoteIndex + 1));
      transformedIndexExpression = expressionBuilder.toString();
    }
    return transformedIndexExpression;
  }

  private String removeExtraWhiteSpaces(String expression) {
    // we replace all the white spaces present before and/or after a comma or a database operator
    // like =, >, < etc.

    String operatorBetweenSpacesRegExp = "(\\s?)([^a-zA-Z0-9\\s]+)(\\s?)";
    int replacementGroup = 2;
    return replaceRegularExpressionMatchings(expression, operatorBetweenSpacesRegExp,
        replacementGroup);
  }

  @Override
  protected Unique readUnique(ResultSet rs) throws SQLException {
    // similar to readTable, see there for definition of both (regarding case)
    String constraintRealName = rs.getString(1);
    String constraintName = constraintRealName.toUpperCase();

    final Unique uni = new Unique();

    uni.setName(constraintName);

    /*
     * Note: only element 0 of this list will ever be used.
     * 
     * Contains a ','-separated strings of the indices of the index-column into the positions of the
     * corresponding columns in the table. Example if an index has the fourth,second,third column of
     * a table this contains the string "4,2,3"
     */
    final ArrayList<String> apositions = new ArrayList<String>();

    /*
     * This maps the columnIndex of an index-column (in the list of columns of the table) to its
     * IndexColumn object. Example: If an IndexColumn is defined on the second column in the table
     * this map is <2,IndexColumn>
     */
    final HashMap<String, IndexColumn> colMap = new HashMap<String, IndexColumn>();

    _stmt_uniquecolumns.setString(1, constraintRealName);
    fillList(_stmt_uniquecolumns, new RowFiller() {
      public void fillRow(ResultSet r) throws SQLException {
        apositions.add(r.getString(3));
        IndexColumn inxcol = new IndexColumn();
        inxcol.setName(r.getString(1));
        colMap.put(r.getString(2), inxcol);
      }
    });

    /*
     * Re-order the IndexColumn into the correct order (based on index-definition) as they are read
     * without ordering them from the metadata
     */
    if (apositions.size() > 0) {
      for (String pos : (apositions.get(0).split(","))) {
        uni.addColumn(colMap.get(pos));
      }
    }
    return uni;
  }
}
