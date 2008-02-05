/*
************************************************************************************
* Copyright (C) 2001-2006 Openbravo S.L.
* Licensed under the Apache Software License version 2.0
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
* Unless required by applicable law or agreed to  in writing,  software  distributed
* under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
* CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
* specific language governing permissions and limitations under the License.
************************************************************************************
*/

package org.apache.ddlutils.platform.postgresql;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Parameter;
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
    protected Translation _checkTranslation = new PostgreSqlCheckTranslation();
    
    protected Map<Integer, Integer> _paramtypes =  new HashMap<Integer, Integer>();
    
    /** Creates a new instance of PostgreSqlModelLoader */
    public PostgreSqlModelLoader() {
    }    
    
    protected String readName() {
        return "PostgreSql server";
    }
    
    protected void initMetadataSentences() throws SQLException {

        try {
            PreparedStatement s = _connection.prepareStatement(
                    "CREATE OR REPLACE FUNCTION temp_findinarray(conkey _int4, attnum int4)  RETURNS int4 AS \n" +
                    "$BODY$\n" +
                    " DECLARE i integer; " +
                    "begin " +
                    "for i in 1..array_upper(conkey,1)" +
                    "  loop     " +
                    "     IF (conkey[i] = attnum) THEN" +
                    "	RETURN i;" +
                    "     END IF;" +
                    "  end loop;  " +
                    "  return 0;" +
                    " end; \n" +
                    "$BODY$\n" +
                    " LANGUAGE 'plpgsql' VOLATILE");
            s.executeUpdate();
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        if (_filter.getExcludedTables().length == 0) {
            _stmt_listtables = _connection.prepareStatement(
                    "SELECT UPPER(TABLENAME) FROM PG_TABLES WHERE SCHEMANAME = CURRENT_SCHEMA()" +
                    " ORDER BY UPPER(TABLENAME)");
        } else {
            _stmt_listtables = _connection.prepareStatement(
                    "SELECT UPPER(TABLENAME) FROM PG_TABLES WHERE SCHEMANAME = CURRENT_SCHEMA()" +
                    " AND UPPER(TABLENAME) NOT IN (" + getListObjects(_filter.getExcludedTables()) + ")" +
                    " ORDER BY UPPER(TABLENAME)");
        }
        _stmt_pkname = _connection.prepareStatement(
                "SELECT UPPER(PG_CONSTRAINT.CONNAME::TEXT)FROM PG_CONSTRAINT JOIN PG_CLASS ON PG_CLASS.OID = PG_CONSTRAINT.CONRELID WHERE PG_CONSTRAINT.CONTYPE::TEXT = 'p' AND UPPER(PG_CLASS.RELNAME::TEXT) =  ?");
        _stmt_listcolumns = _connection.prepareStatement(
                "SELECT UPPER(PG_ATTRIBUTE.ATTNAME::TEXT), UPPER(PG_TYPE.TYPNAME::TEXT), " +
                " CASE PG_TYPE.TYPNAME" +
                "     WHEN 'varchar'::name THEN pg_attribute.atttypmod - 4" +
                "     WHEN 'bpchar'::name THEN pg_attribute.atttypmod - 4" +
                "     ELSE NULL::integer" +
                " END," +
                " CASE PG_TYPE.TYPNAME" +
                "     WHEN 'bytea'::name THEN 4000" +
                "     WHEN 'text'::name THEN 4000" +
                "     WHEN 'oid'::name THEN 4000" +
                "     ELSE CASE PG_ATTRIBUTE.ATTLEN " +
                "              WHEN -1 THEN PG_ATTRIBUTE.ATTTYPMOD - 4 " +
                "              ELSE PG_ATTRIBUTE.ATTLEN " +
                "          END" +
                " END," +
                " CASE pg_type.typname" +
                "     WHEN 'bytea'::name THEN 4000" +
                "     WHEN 'text'::name THEN 4000" +
                "     WHEN 'oid'::name THEN 4000" +
                "     ELSE" +
                "         CASE atttypmod" +
                "             WHEN -1 THEN 0" +
                "             ELSE 10" +
                "         END" +
                " END," +
                " 0, not pg_attribute.attnotnull," +
                " CASE pg_attribute.atthasdef" +
                "     WHEN true THEN ( SELECT pg_attrdef.adsrc FROM pg_attrdef WHERE pg_attrdef.adrelid = pg_class.oid AND pg_attrdef.adnum = pg_attribute.attnum)" +
                "     ELSE NULL::text" +
                " END" +
                " FROM pg_class, pg_namespace, pg_attribute, pg_type" +
                " WHERE pg_attribute.attrelid = pg_class.oid AND pg_attribute.atttypid = pg_type.oid AND pg_class.relnamespace = pg_namespace.oid AND pg_namespace.nspname = current_schema() AND pg_attribute.attnum > 0 " +
                " AND upper(pg_class.relname::text) = ? " +
                " ORDER BY pg_attribute.attnum");
        _stmt_pkcolumns = _connection.prepareStatement(
                "SELECT upper(pg_attribute.attname::text)" +
                " FROM pg_constraint, pg_class, pg_attribute" +
                " WHERE pg_constraint.conrelid = pg_class.oid AND pg_attribute.attrelid = pg_constraint.conrelid AND (pg_attribute.attnum = ANY (pg_constraint.conkey))" +
                " AND upper(pg_constraint.conname::text) = ?" +
                " ORDER BY pg_attribute.attnum::integer");
        _stmt_listchecks = _connection.prepareStatement(
                "SELECT upper(pg_constraint.conname::text), pg_constraint.consrc" +
                " FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid" +
                " WHERE pg_constraint.contype::text = 'c' and upper(pg_class.relname::text) = ?" +
                " ORDER BY upper(pg_constraint.conname::text)");
        _stmt_listfks = _connection.prepareStatement(
                "SELECT upper(pg_constraint.conname::text) AS constraint_name, upper(fk_table.relname::text), upper(pg_constraint.confdeltype::text), 'A'" +
                " FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid LEFT JOIN pg_class fk_table ON fk_table.oid = pg_constraint.confrelid" +
                " WHERE pg_constraint.contype::text = 'f' and upper(pg_class.relname::text) = ?" +
                " ORDER BY upper(pg_constraint.conname::text)");
        _stmt_fkcolumns = _connection.prepareStatement(
                "SELECT upper(pa1.attname), upper(pa2.attname)" +
                " FROM pg_constraint pc, pg_class pc1, pg_attribute pa1, pg_class pc2, pg_attribute pa2" +
                " WHERE pc.contype='f' and pc.conrelid= pc1.oid and upper(pc.conname) = upper(?) and pa1.attrelid = pc1.oid and pa1.attnum = ANY(pc.conkey)" +
                " and pc.confrelid = pc2.oid and pa2.attrelid = pc2.oid and pa2.attnum = ANY(pc.confkey)");
        _stmt_listindexes = _connection.prepareStatement(
                "SELECT UPPER(PG_CLASS.RELNAME), CASE PG_INDEX.indisunique WHEN true THEN 'UNIQUE' ELSE 'NONUNIQUE' END" +
                " FROM PG_INDEX, PG_CLASS, PG_CLASS PG_CLASS1, PG_NAMESPACE" +
                " WHERE PG_INDEX.indexrelid = PG_CLASS.OID" +
                " AND PG_INDEX.indrelid = PG_CLASS1.OID" +
                " AND PG_CLASS.RELNAMESPACE = PG_NAMESPACE.OID" +
                " AND PG_CLASS1.RELNAMESPACE = PG_NAMESPACE.OID" +
                " AND PG_NAMESPACE.NSPNAME = CURRENT_SCHEMA()" +
                " AND PG_INDEX.INDISPRIMARY ='f'" +
                " AND UPPER(PG_CLASS1.RELNAME) = ?" +
                " AND PG_CLASS.RELNAME NOT IN (SELECT pg_constraint.conname::text " +
                "    FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid" +
                "    WHERE pg_constraint.contype::text = 'u')" +
                " ORDER BY UPPER(PG_CLASS.RELNAME)");
        _stmt_indexcolumns = _connection.prepareStatement("SELECT upper(pg_attribute.attname::text) " +
                "FROM pg_index, pg_class, pg_namespace, pg_attribute" +
                " WHERE pg_index.indexrelid = pg_class.oid" +
                " AND pg_attribute.attrelid = pg_index.indrelid" +
                " AND pg_attribute.attnum = ANY (indkey)" +
                " AND pg_class.relnamespace = pg_namespace.oid" +
                " AND pg_namespace.nspname = current_schema()" +
                " AND upper(pg_class.relname::text) = ?" +
                " ORDER BY temp_findinarray(pg_index.indkey, pg_attribute.attnum)");
        _stmt_listuniques = _connection.prepareStatement(
                "SELECT upper(pg_constraint.conname::text)" +
                " FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid" +
                " WHERE pg_constraint.contype::text = 'u' AND upper(pg_class.relname::text) = ?" +
                " ORDER BY upper(pg_constraint.conname::text)");
        _stmt_uniquecolumns = _connection.prepareStatement(
                "SELECT upper(pg_attribute.attname::text)" +
                " FROM pg_constraint, pg_class, pg_attribute" +
                " WHERE pg_constraint.conrelid = pg_class.oid AND pg_attribute.attrelid = pg_constraint.conrelid AND (pg_attribute.attnum = ANY (pg_constraint.conkey))" +
                " AND upper(pg_constraint.conname::text) = ?" +
                " ORDER BY temp_findinarray(pg_constraint.conkey, pg_attribute.attnum)");
        
        if (_filter.getExcludedViews().length == 0) {
            _stmt_listviews = _connection.prepareStatement("SELECT upper(viewname), definition FROM pg_views " +
                    "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND viewname !~ '^pg_'");
        } else {
            _stmt_listviews = _connection.prepareStatement("SELECT upper(viewname), definition FROM pg_views " +
                    "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND viewname !~ '^pg_' " +
                    "AND upper(viewname) NOT IN (" + getListObjects(_filter.getExcludedViews()) + ")"); 
        }
        
        if (_filter.getExcludedSequences().length == 0) {
            _stmt_listsequences = _connection.prepareStatement("SELECT upper(relname), 1, 1 FROM pg_class WHERE relkind = 'S'");
        } else {
            _stmt_listsequences = _connection.prepareStatement("SELECT upper(relname), 1, 1 FROM pg_class WHERE relkind = 'S' AND upper(relname) NOT IN (" + getListObjects(_filter.getExcludedSequences()) + ")");
        }

        if (_filter.getExcludedTriggers().length == 0) {
            _stmt_listtriggers = _connection.prepareStatement("SELECT upper(trg.tgname) AS trigger_name, upper(tbl.relname) AS table_name, " +
                    "CASE trg.tgtype & cast(3 as int2) " +
                    "WHEN 0 THEN 'AFTER EACH STATEMENT' " +
                    "WHEN 1 THEN 'AFTER EACH ROW' " +
                    "WHEN 2 THEN 'BEFORE EACH STATEMENT' " +
                    "WHEN 3 THEN 'BEFORE EACH ROW' " +
                    "END AS trigger_type, " +
                    "CASE trg.tgtype & cast(28 as int2) WHEN 16 THEN 'UPDATE' " +
                    "WHEN  8 THEN 'DELETE' " +
                    "WHEN  4 THEN 'INSERT' " +
                    "WHEN 20 THEN 'INSERT, UPDATE' " +
                    "WHEN 28 THEN 'INSERT, UPDATE, DELETE' " +
                    "WHEN 24 THEN 'UPDATE, DELETE' " +
                    "WHEN 12 THEN 'INSERT, DELETE' " +
                    "END AS trigger_event, " +
                    "p.prosrc AS function_code " +
                    "FROM pg_trigger trg, pg_class tbl, pg_proc p " +
                    "WHERE trg.tgrelid = tbl.oid AND trg.tgfoid = p.oid AND tbl.relname !~ '^pg_' AND trg.tgname !~ '^RI'");
        } else {
            _stmt_listtriggers = _connection.prepareStatement("SELECT upper(trg.tgname) AS trigger_name, upper(tbl.relname) AS table_name, " +
                    "CASE trg.tgtype & cast(3 as int2) " +
                    "WHEN 0 THEN 'AFTER EACH STATEMENT' " +
                    "WHEN 1 THEN 'AFTER EACH ROW' " +
                    "WHEN 2 THEN 'BEFORE EACH STATEMENT' " +
                    "WHEN 3 THEN 'BEFORE EACH ROW' " +
                    "END AS trigger_type, " +
                    "CASE trg.tgtype & cast(28 as int2) WHEN 16 THEN 'UPDATE' " +
                    "WHEN  8 THEN 'DELETE' " +
                    "WHEN  4 THEN 'INSERT' " +
                    "WHEN 20 THEN 'INSERT, UPDATE' " +
                    "WHEN 28 THEN 'INSERT, UPDATE, DELETE' " +
                    "WHEN 24 THEN 'UPDATE, DELETE' " +
                    "WHEN 12 THEN 'INSERT, DELETE' " +
                    "END AS trigger_event, " +
                    "p.prosrc AS function_code " +
                    "FROM pg_trigger trg, pg_class tbl, pg_proc p " +
                    "WHERE trg.tgrelid = tbl.oid AND trg.tgfoid = p.oid AND tbl.relname !~ '^pg_' AND trg.tgname !~ '^RI' AND upper(trg.tgname) NOT IN (" + getListObjects(_filter.getExcludedTriggers()) + ")");
        }
        
        if (_filter.getExcludedFunctions().length == 0) {
            _stmt_listfunctions = _connection.prepareStatement(
                    "select distinct upper(proname) from pg_proc p, pg_namespace n " +
                    "where  pronamespace = n.oid " +
                    "and n.nspname=current_schema() " +
                    "and p.oid not in (select tgfoid " +
                    "from pg_trigger) " +
                    "and p.proname <> 'temp_findinarray'");
        } else {
            _stmt_listfunctions = _connection.prepareStatement(
                    "select distinct upper(proname) from pg_proc p, pg_namespace n " +
                    "where  pronamespace = n.oid " +
                    "and n.nspname=current_schema() " +
                    "and p.oid not in (select tgfoid " +
                    "from pg_trigger) " +
                    "and p.proname <> 'temp_findinarray' " +
                    "AND upper(p.proname) NOT IN (" + getListObjects(_filter.getExcludedFunctions()) + ")");
        }
        
        _stmt_functioncode = _connection.prepareStatement("select p.prosrc FROM pg_proc p WHERE UPPER(p.proname) = ?"); // dummy sentence        

        _stmt_functionparams = _connection.prepareStatement(
                "  SELECT " +
                "         pg_proc.prorettype," +
                "         pg_proc.proargtypes," +
                "         pg_proc.proallargtypes," +
                "         pg_proc.proargmodes," +
                "         pg_proc.proargnames," +
                "         pg_proc.prosrc" +
                "    FROM pg_catalog.pg_proc" +
                "         JOIN pg_catalog.pg_namespace" +
                "         ON (pg_proc.pronamespace = pg_namespace.oid)" +
                "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype" +
                "     AND (pg_proc.proargtypes[0] IS NULL" +
                "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)" +
                "     AND NOT pg_proc.proisagg" +
                "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)" +
                "     AND upper(pg_proc.proname) = ?" +
                "         ORDER BY pg_proc.proargtypes DESC");

        _stmt_functiondefaults = _connection.prepareStatement(
                "  SELECT " +
                "         pg_proc.proname," +
                "         pg_proc.proargtypes," +
                "         pg_proc.proallargtypes," +
                "         pg_proc.prosrc" +
                "    FROM pg_catalog.pg_proc" +
                "         JOIN pg_catalog.pg_namespace" +
                "         ON (pg_proc.pronamespace = pg_namespace.oid)" +
                "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype" +
                "     AND (pg_proc.proargtypes[0] IS NULL" +
                "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)" +
                "     AND NOT pg_proc.proisagg" +
                "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)" +
                "     AND (upper(pg_proc.proname) = ? )" +
                "         ORDER BY pg_proc.proargtypes ASC");
        
        _stmt_functiondefaults0 = _connection.prepareStatement(
                "  SELECT " +
                "         pg_proc.proname," +
                "         pg_proc.proargtypes," +
                "         pg_proc.proallargtypes," +
                "         pg_proc.prosrc" +
                "    FROM pg_catalog.pg_proc" +
                "         JOIN pg_catalog.pg_namespace" +
                "         ON (pg_proc.pronamespace = pg_namespace.oid)" +
                "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype" +
                "     AND (pg_proc.proargtypes[0] IS NULL" +
                "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)" +
                "     AND NOT pg_proc.proisagg" +
                "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)" +
                "     AND (upper(pg_proc.proname) = ? )" +
                "         ORDER BY pg_proc.proargtypes ASC");
        
        
        _stmt_paramtypes = _connection.prepareStatement("SELECT pg_catalog.format_type(?, NULL)");        

    }
    
    protected void closeMetadataSentences() throws SQLException {
        super.closeMetadataSentences();
        _stmt_functionparams.close();
        _stmt_paramtypes.close();
        
        Statement s = _connection.createStatement();
        s.executeUpdate(
                "DROP FUNCTION temp_findinarray(conkey _int4, attnum int4)");
        s.close();        
    }
    
    int numDefaults;
    int numDefaultsDif;
    int numRemDefaults;
    protected Function readFunction(String name) throws SQLException {
        
        final Function f = new Function();
        f.setName(name);
        
        final FinalBoolean firststep = new FinalBoolean();

        _stmt_functionparams.setString(1, name);
        _stmt_functiondefaults.setString(1, name);
        _stmt_functiondefaults0.setString(1, name+"0");
        numDefaults=0;
        numDefaultsDif=0;
        
        numRemDefaults=0;
        
        fillList(_stmt_functionparams, new RowFiller() { public void fillRow(ResultSet r) throws SQLException {

            if (firststep.get()) {
                // just set defaults
                Integer[] atypes = getIntArray(r, 2);
                Integer[] aalltypes = getIntArray2(r, 3);
                if (aalltypes != null) {    
                    atypes = aalltypes;
                }  
                
                /*
                for (int i = atypes.length; i < f.getParameterCount(); i++) {
                    f.getParameter(i).setDefaultValue("0"); // a dummy default value
                }*/
                numDefaults++;
                
            } else {
                int ireturn = r.getInt(1);
                Integer[] atypes = getIntArray(r, 2);
                Integer[] aalltypes = getIntArray2(r, 3);
                String[] modes = getStringArray(r, 4);
                String[] names = getStringArray(r, 5);

                if (aalltypes == null) {
                    f.setTypeCode(getParamType(ireturn));                  
                } else {
                    f.setTypeCode(Types.NULL);    
                    atypes = aalltypes;
                }
                

                for (int i = 0; i < atypes.length; i++) {
                    Parameter p = new Parameter();
                    p.setTypeCode(getParamType(atypes[i]));
                    if (modes == null) {
                        p.setModeCode(Parameter.MODE_IN);
                    } else {
                        p.setModeCode("i".equals(modes[i]) 
                                ? Parameter.MODE_IN
                                : Parameter.MODE_OUT);
                    }
                    if (names != null) {
                        p.setName(names[i]);
                    }

                    f.addParameter(p);
                }          
                firststep.set(true);
                

            	f.setBody(translatePLSQLBody(r.getString(6)));

            	numDefaults=0;
            }
        }});    
        firststep.set(false);

        numRemDefaults=numDefaults;


        
        fillList(_stmt_functiondefaults, new RowFiller() { public void fillRow(ResultSet r) throws SQLException {

        	//int numParams=f.getParameterCount();
        	if(numRemDefaults>0)
        	{
                Integer[] types = getIntArray(r, 2);
                Integer[] alltypes=getIntArray2(r,3);
        		int numParamsMin=numRemDefaults;//types.length;
        		//if(alltypes!=null && alltypes.length>numParamsMin) numParamsMin=alltypes.length;
        		//System.out.println(r.getString(1)+": "+numParams+";;;"+numParamsMin);
        		try
        		{
		        	String bodyMin=r.getString(4);
		        	String patternSearched=""+f.getName().toUpperCase()+"(.*)\\)";
		        	Pattern pattern=Pattern.compile(patternSearched);
		        	Matcher matcher=pattern.matcher(bodyMin);
		        	if(matcher.find())
		        	{
			        	String defaults=matcher.group(1).trim();
		        		//System.out.println("hemos encontrado: "+defaults);
			        	defaults=defaults.substring(1, defaults.length());
			        	//System.out.println("default: "+defaults);
			        	StringTokenizer sT=new StringTokenizer(defaults);
			        	
			        	Vector<String> strs=new Vector<String>();
			        	while(sT.hasMoreTokens())
				        	strs.add(sT.nextToken());
			        	
		        		String pvalue=strs.lastElement().replaceAll("'", "");
		        		//System.out.println("intento meter: "+pvalue);
		        		f.getParameter(f.getParameterCount()-numRemDefaults).setDefaultValue(pvalue);

		        		numRemDefaults--;
		        	}
		        	else
		        	{
	        			System.out.println("Error reading default values for parameters in function "+r.getString(1)+" (pattern not found)");
		        	}
        		}catch(Exception e)
        		{
        			System.out.println("Error reading default values for parameters in function "+r.getString(1)+": "+e.toString());
        			//System.out.println("We'll try to find them in function "+r.getString(1)+"0");


        		}
        	}
        
    	
        }});
        
        if(numRemDefaults!=0)
        {
			System.out.println("Error reading default values for parameters in function "+name);
        }
        
                
        return f;
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
        
        String s = r.getString(iposition);
        if (s == null) {
            return null;
        } else {
            ArrayList<Integer> list = new ArrayList<Integer>();        
            if (s.length() > 1 && s.charAt(0) == '{' && s.charAt(s.length() - 1) == '}') {
                s = s.substring(1, s.length() - 1);
            }
        
            StringTokenizer st = new StringTokenizer(s, ",");

            while (st.hasMoreTokens()) {
                list.add(Integer.parseInt(st.nextToken()));
            }        

            return list.toArray(new Integer[list.size()]);
        }
    }
    
    protected String[] getStringArray(ResultSet r, int iposition) throws SQLException {
        
        String s = r.getString(iposition);
        if (s == null) {
            return null;
        } else {
            ArrayList<String> list = new ArrayList<String>();
            if (s.length() > 1 && s.charAt(0) == '{' && s.charAt(s.length() - 1) == '}') {
                s = s.substring(1, s.length() - 1);
            }
        
            StringTokenizer st = new StringTokenizer(s, ",");

            while (st.hasMoreTokens()) {
                list.add(st.nextToken());
            }        

            return list.toArray(new String[list.size()]);
        }
    }
    
    protected int getParamType(int pgtype) throws SQLException {
        
        if (!_paramtypes.containsKey(pgtype)) {

            _stmt_paramtypes.setInt(1, pgtype);
            String stype = (String) readRow(_stmt_paramtypes, new RowConstructor() { public Object getRow(ResultSet r) throws SQLException {
                return r.getString(1);
            }});

            _paramtypes.put(pgtype, translateParamType(stype));    
        }
        
        return _paramtypes.get(pgtype);
    }
    
    protected String translateCheckCondition(String code) {
        return _checkTranslation.exec(code);
    }
    
    protected boolean translateRequired(String required) {
        return "f".equals(required);
    }
    
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
                    if(sunescaped.length() == 0) return null;
                    else return sunescaped.toString();
                } else {
                    return value;
                }
            case Types.TIMESTAMP:
                if ("now()".equalsIgnoreCase(value)) {
                    return "SYSDATE";
                } else {
                    return value;
                }
            default: return value;
        }
    }
    
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
        } else if ("OID".equalsIgnoreCase(nativeType)) {
            return Types.BLOB;
        } else {
            return Types.VARCHAR;
        }
    }
    
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
            return Types.BLOB;
        } else {
            return Types.VARCHAR;
        }
    }    
    
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
    
}
