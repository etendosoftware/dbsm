package org.apache.ddlutils.platform.postgresql;

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
import java.sql.Types;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.AddColumnChange;
import org.apache.ddlutils.alteration.RemoveColumnChange;
import org.apache.ddlutils.alteration.TableChange;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.SqlBuilder;
import org.apache.ddlutils.translation.CommentFilter;
import org.apache.ddlutils.translation.LiteralFilter;
import org.apache.ddlutils.translation.Translation;
import org.apache.ddlutils.util.ExtTypes;

/**
 * The SQL Builder for PostgresSql.
 * 
 * @version $Revision: 504014 $
 */
public class PostgreSqlBuilder extends SqlBuilder
{
    
    private Translation plsqltranslation = null;
    private Translation sqltranslation = null;
    
    /**
     * Creates a new builder instance.
     * 
     * @param platform The plaftform this builder belongs to
     */
    public PostgreSqlBuilder(Platform platform)
    {
        super(platform);
        // we need to handle the backslash first otherwise the other
        // already escaped sequences would be affected
        addEscapedCharSequence("\\", "\\\\");
        addEscapedCharSequence("'",  "\\'");
        addEscapedCharSequence("\b", "\\b");
        addEscapedCharSequence("\f", "\\f");
        addEscapedCharSequence("\n", "\\n");
        addEscapedCharSequence("\r", "\\r");
        addEscapedCharSequence("\t", "\\t");
    }

    /**
     * {@inheritDoc}
     */
    public void dropTable(Table table) throws IOException
    { 
        printStartOfStatement("TABLE", getStructureObjectName(table));
        print("DROP TABLE ");
        printIdentifier(getStructureObjectName(table));
        print(" CASCADE");
        printEndOfStatement(getStructureObjectName(table));

        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++)
        {
            dropAutoIncrementSequence(table, columns[idx]);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void writeExternalIndexDropStmt(Table table, Index index) throws IOException
    {
        print("DROP INDEX ");
        printIdentifier(getConstraintObjectName(index));
        printEndOfStatement();
    }

    /**
     * {@inheritDoc}
     */
    public void createTable(Database database, Table table, Map parameters) throws IOException
    {
        for (int idx = 0; idx < table.getColumnCount(); idx++)
        {
            Column column = table.getColumn(idx);

            if (column.isAutoIncrement())
            {
                createAutoIncrementSequence(table, column);
            }
        }
        super.createTable(database, table, parameters);
        
        //Add comments for NVARCHAR types

        for (int idx = 0; idx < table.getColumnCount(); idx++)
        {
            Column column = table.getColumn(idx);
            String comment="";

            if (column.getTypeCode()==ExtTypes.NVARCHAR)
            {
                comment+="--OBTG:NVARCHAR--";
            }

            if (column.getTypeCode()==ExtTypes.NCHAR)
            {
                comment+="--OBTG:NCHAR--";
            }
            if(column.getOnCreateDefault()!=null && !column.getOnCreateDefault().equals(""))
            {
            	String oncreatedefaultp=column.getOnCreateDefault();
            	String oncreatedefault="";
            	int lengthoncreate=oncreatedefaultp.length();
            	//Parse oncreatedefault
            	for(int i=0;i<lengthoncreate;i++)
            	{
            		String tchar=oncreatedefaultp.substring(0,1);
            		oncreatedefaultp=oncreatedefaultp.substring(1);
            		if(tchar.equals("'"))
            			oncreatedefault+="''";
            		else
            			oncreatedefault+=tchar;
            	}
            	comment+="--OBTG:ONCREATEDEFAULT:"+oncreatedefault+"--";
            }
            if(!comment.equals(""))
            	println("COMMENT ON COLUMN "+table.getName()+"."+column.getName()+" IS '"+comment+"';");
        }
    }

    /**
     * {@inheritDoc}
     */
    protected String getDelimitedIdentifier(String identifier)
    {
        if ("OFFSET".equalsIgnoreCase(identifier) ||
            "NOW".equalsIgnoreCase(identifier) ||
            "WHEN".equalsIgnoreCase(identifier)) {
            return getPlatformInfo().getDelimiterToken() + identifier + getPlatformInfo().getDelimiterToken();
        } else {
            return super.getDelimitedIdentifier(identifier);
        }
    }

    
    /**
     * Creates the auto-increment sequence that is then used in the column.
     *  
     * @param table  The table
     * @param column The column
     */
    private void createAutoIncrementSequence(Table table, Column column) throws IOException
    {
        print("CREATE SEQUENCE ");
        printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
        printEndOfStatement();
    }


    protected void disableNOTNULLColumns(Vector<AddColumnChange> newColumns) throws IOException
    {
    	for(int i=0;i<newColumns.size();i++)
    	{
    		Column column=newColumns.get(i).getNewColumn();
    		if(column.isRequired())
    		{
    			println("ALTER TABLE "+newColumns.get(i).getChangedTable().getName()+" ALTER "+getColumnName(column)+" DROP NOT NULL");
        		printEndOfStatement();
    		}
    		
    	}
    }
    protected void disableTempNOTNULLColumns(Vector<AddColumnChange> newColumns) throws IOException
    {
    	for(int i=0;i<newColumns.size();i++)
    	{
    		Column column=newColumns.get(i).getNewColumn();
    		if(column.isRequired())
    		{
    			println("ALTER TABLE "+newColumns.get(i).getChangedTable().getName()+"_ ALTER "+getColumnName(column)+" DROP NOT NULL");
        		printEndOfStatement();
    		}
    		
    	}
    }
    
    protected void enableNOTNULLColumns(Vector<AddColumnChange> newColumns) throws IOException
    {
    	for(int i=0;i<newColumns.size();i++)
    	{
    		Column column=newColumns.get(i).getNewColumn();
    		if(column.isRequired())
    		{
    			println("ALTER TABLE "+newColumns.get(i).getChangedTable().getName()+" ALTER "+getColumnName(column)+" SET NOT NULL");
        		printEndOfStatement();
    		}
    		
    	}
    }
    
    
    /**
     * Creates the auto-increment sequence that is then used in the column.
     *  
     * @param table  The table
     * @param column The column
     */
    private void dropAutoIncrementSequence(Table table, Column column) throws IOException
    {
        print("DROP SEQUENCE ");
        printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
        printEndOfStatement();
    }

    /**
     * {@inheritDoc}
     */
    protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException
    {
        print("UNIQUE DEFAULT nextval('");
        print(getConstraintName(null, table, column.getName(), "seq"));
        print("')");
    }

    /**
     * {@inheritDoc}
     */
    public String getSelectLastIdentityValues(Table table)
    {
        Column[] columns = table.getAutoIncrementColumns();

        if (columns.length == 0)
        {
            return null;
        }
        else
        {
            StringBuffer result = new StringBuffer();
    
            result.append("SELECT ");
            for (int idx = 0; idx < columns.length; idx++)
            {
                if (idx > 0)
                {
                    result.append(", ");
                }
                result.append("currval('");
                result.append(getConstraintName(null, table, columns[idx].getName(), "seq"));
                result.append("') AS ");
                result.append(getDelimitedIdentifier(columns[idx].getName()));
            }
            return result.toString();
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void processTableStructureChanges(Database currentModel,
                                                Database desiredModel,
                                                Table    sourceTable,
                                                Table    targetTable,
                                                Map      parameters,
                                                List     changes) throws IOException
    {
    	/*
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof AddColumnChange)
            {
                AddColumnChange addColumnChange = (AddColumnChange)change;

                // We can only use PostgreSQL-specific SQL if
                // * the column is not set to NOT NULL (the constraint would be applied immediately
                //   which will not work if there is already data in the table)
                // * the column has no default value (it would be applied after the change which
                //   means that PostgreSQL would behave differently from other databases where the
                //   default is applied to every column)
                // * the column is added at the end of the table (PostgreSQL does not support
                //   insertion of a column)
                if (!addColumnChange.getNewColumn().isRequired() &&
                    (addColumnChange.getNewColumn().getDefaultValue() == null) &&
                    (addColumnChange.getNextColumn() == null))
                {
                    processChange(currentModel, desiredModel, addColumnChange);
                    changeIt.remove();
                }
            }
            else if (change instanceof RemoveColumnChange)
            {
                processChange(currentModel, desiredModel, (RemoveColumnChange)change);
                changeIt.remove();
            }
        }*/ //We will try to insert data even in this case
        super.processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable, parameters, changes);
    }

    /**
     * Processes the addition of a column to a table.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database        currentModel,
                                 Database        desiredModel,
                                 AddColumnChange change) throws IOException
    {
        print("ALTER TABLE ");
        printlnIdentifier(getStructureObjectName(change.getChangedTable()));
        printIndent();
        print("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn());
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /**
     * Processes the removal of a column from a table.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database           currentModel,
                                 Database           desiredModel,
                                 RemoveColumnChange change) throws IOException
    {
        print("ALTER TABLE ");
        printlnIdentifier(getStructureObjectName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        if (change.getColumn().isAutoIncrement())
        {
            dropAutoIncrementSequence(change.getChangedTable(), change.getColumn());
        }
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /**
     * {@inheritDoc}
     */
    protected void writeCreateFunctionStmt(Function function) throws IOException {
        print("CREATE FUNCTION ");
        printIdentifier(getStructureObjectName(function));
    }

    /**
     * {@inheritDoc}
     */
    protected void writeDropFunctionStmt(Function function) throws IOException {
        print("DROP FUNCTION ");
        printIdentifier(getStructureObjectName(function));

        print("(");
        for (int idx = 0; idx < function.getParameterCount(); idx ++) {
            if (idx > 0) {
                print(", ");
            }
            writeParameter(function.getParameter(idx));
        }
        print(")");            
    }

    /**
     * {@inheritDoc}
     */
    protected String getNoParametersDeclaration() {
        return "()";
    }
    
    /**
     * {@inheritDoc}
     */
    protected String getFunctionReturn(Function function) {
        
        if (function.getTypeCode() == Types.NULL) {
            if (isProcedure(function)) {
                return "";
            } else {
                return "RETURNS VOID";
            }
        } else {
            return "RETURNS " + getSqlType(function.getTypeCode());
        }
    }   
    
    private boolean isProcedure(Function function) {
        for (int i = 0; i < function.getParameterCount(); i++){
            if (function.getParameter(i).getModeCode() == Parameter.MODE_OUT) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    protected String getFunctionBeginBody() {                
        return "AS $BODY$ DECLARE ";
    }
    
    /**
     * {@inheritDoc}
     */
    protected String getFunctionEndBody() {
        return "; $BODY$ LANGUAGE plpgsql;";
    }
    
    /**
     * {@inheritDoc}
     */    
    protected void createFunction(Function function) throws IOException {
        
        super.createFunction(function);
        /*System.out.println("Funcion: "+function.getName());
        for(int i=0;i<function.getParameterCount();i++)
        	System.out.println("    Param: "+function.getParameter(i).getName()+"   default: "+function.getParameter(i).getDefaultValue());
        */
        //We'll add a comment to save the NVARCHAR, VARCHAR2, and similar types of data which don't have
        //a corresponding type in PostgreSQL
        String comment="--OBTG:";
        boolean b=false;
        if(function.getTypeCode()==ExtTypes.NVARCHAR)
        {
        	comment+=function.getName()+"func="+"NVARCHAR";
        	b=true;
        }
        for(int i=0;i<function.getParameterCount();i++)
        {
        	Parameter p=function.getParameter(i);
        	if(p.getTypeCode()==ExtTypes.NVARCHAR)
        	{
        		if(b)
        			comment+=",";
        		comment+=p.getName()+"="+"NVARCHAR";
        		b=true;
        	}
        }
        if(!comment.equals("--OBTG:"))
        {
            print("COMMENT ON FUNCTION "+function.getName()+" ");

            if (function.getParameterCount() == 0) {
                print(getNoParametersDeclaration());
            } else {
                print("(");
                for (int idx = 0; idx < function.getParameterCount(); idx ++) {
                    if (idx > 0) {
                        print(", ");
                    }
                    writeParameter(function.getParameter(idx));
                }
                print(")");
            }
            println(" IS '"+comment+"--';");
        }
        String sLastDefault = function.getParameterCount() == 0 ? null : getDefaultValue(function.getParameter(function.getParameterCount() - 1));
        if (sLastDefault != null && !sLastDefault.equals("")) {
            try {
                Function f = (Function) function.clone();
                f.removeParameter(function.getParameterCount() - 1);
                StringBuffer sBody = new StringBuffer();
                sBody.append("BEGIN\n");
                sBody.append(function.getTypeCode() == Types.NULL ? " " : "RETURN ");
                sBody.append(getStructureObjectName(function));
                sBody.append(" (");
                for(int i = 0; i < f.getParameterCount(); i++) {
                    sBody.append("$");
                    sBody.append(i + 1);
                    sBody.append(", ");
                }
                sBody.append(sLastDefault);
                sBody.append(");\n");
                sBody.append("END");                        
                f.setBody(sBody.toString());
                createFunction(f);
            } catch (CloneNotSupportedException e) {
                // Will not happen
            }            
        }
        else
        {
        	//System.out.println("funcion "+function.getName()+" doesn't have defaults");
        }
    } 

    
    /**
     * {@inheritDoc}
     */
    protected void dropFunction(Function function) throws IOException {
        
        String sLastDefault = function.getParameterCount() == 0 ? null : function.getParameter(function.getParameterCount() - 1).getDefaultValue();
        if (sLastDefault != null && !sLastDefault.equals("")) {
            try {
                Function f = (Function) function.clone();
                f.removeParameter(function.getParameterCount() - 1);
                dropFunction(f);
            } catch (CloneNotSupportedException e) {
                // Will not happen
            }            
        }
        
        super.dropFunction(function);
        
    }
    
    /**
     * {@inheritDoc}
     */
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
        
        // Postgre does not support default values...
        // writeDefaultValueStmt(parameter); 
    }    

    public void writeCreateTriggerFunction(Trigger trigger) throws IOException {
        
        printStartOfStatement("FUNCTION FOR TRIGGER", getStructureObjectName(trigger));     
        
        print("CREATE FUNCTION ");
        printIdentifier(getStructureObjectName(trigger));
        print("()");
        println();
        print("RETURNS trigger");
        println();
        
        print(getFunctionBeginBody());        
        println();
        

        String body=trigger.getBody();
        
        LiteralFilter litFilter=new LiteralFilter();
        CommentFilter comFilter=new CommentFilter();
        body=litFilter.removeLiterals(body);
        body=comFilter.removeComments(body);
        

        body=getPLSQLTriggerTranslation().exec(body);

        body=comFilter.restoreComments(body);
		body=litFilter.restoreLiterals(body);

        print(body);
        println();
        print(getFunctionEndBody());  
        
        printEndOfStatement(getStructureObjectName(trigger));
    }
    
    public void writeTriggerExecuteStmt(Trigger trigger) throws IOException {             
        print("EXECUTE PROCEDURE ");    
        printIdentifier(getStructureObjectName(trigger));
        print("()");
    }
    
    
    protected void writeDropTriggerEndStatement(Database database, Trigger trigger) throws IOException {
        print(" ON ");
        print(getStructureObjectName(database.findTable(trigger.getTable())));
        print(" CASCADE");
    }
    
    protected void writeDropTriggerFunction(Trigger trigger) throws IOException {
        
        printStartOfStatement("FUNCTION FOR TRIGGER", getStructureObjectName(trigger));     

        print("DROP FUNCTION ");
        printIdentifier(getStructureObjectName(trigger));
        print("()");
        printEndOfStatement(getStructureObjectName(trigger));
    }
    
    protected void writeCreateViewStatement(View view) throws IOException {  
        
        printScriptOptions("FORCE = TRUE");
        print("CREATE VIEW ");
        printIdentifier(getStructureObjectName(view));
        print(" AS ");
        print(getSQLTranslation().exec(view.getStatement()));        
    }
    
    protected void createUpdateRules(View view) throws IOException {
        
        RuleProcessor rule = new RuleProcessor(view.getStatement());
        
        if (rule.isUpdatable()) {

            // INSERT RULE
            print("CREATE OR REPLACE RULE ");
            printIdentifier(shortenName(view.getName() + "_INS", getMaxTableNameLength()));
            print(" AS ON INSERT TO ");
            printIdentifier(getStructureObjectName(view));
            print(" DO INSTEAD INSERT INTO ");
            printIdentifier(shortenName(rule.getViewTable(), getMaxTableNameLength()));
            print(" ( ");
            for(int i = 0; i < rule.getViewFields().size(); i++) {
                RuleProcessor.ViewField field = rule.getViewFields().get(i);
                if (i > 0) {
                    print (", ");
                }
                print(field.getField());
            }
            print(" ) VALUES ( ");
            for(int i = 0; i < rule.getViewFields().size(); i++) {
                RuleProcessor.ViewField field = rule.getViewFields().get(i);
                if (i > 0) {
                    print (", ");
                }
                print("NEW.");
                print(field.getFieldas());
            }       
            print(")");
            printEndOfStatement(getStructureObjectName(view));

            // UPDATE RULE
            print("CREATE OR REPLACE RULE ");
            printIdentifier(shortenName(view.getName() + "_UPD", getMaxTableNameLength()));
            print(" AS ON UPDATE TO ");
            printIdentifier(getStructureObjectName(view));
            print(" DO INSTEAD UPDATE ");
            printIdentifier(shortenName(rule.getViewTable(), getMaxTableNameLength()));
            print(" SET ");

            for(int i = 0; i < rule.getViewFields().size(); i++) {
                RuleProcessor.ViewField field = rule.getViewFields().get(i);
                if (i > 0) {
                    print (", ");
                }
                print(field.getField());
                print(" = NEW.");
                print(field.getFieldas());
            }
            print(" WHERE ");
            print(rule.getViewFields().get(0).getField());
            print(" = NEW.");
            print(rule.getViewFields().get(0).getFieldas());            
            printEndOfStatement(getStructureObjectName(view));   

            // DELETE RULE
            print("CREATE OR REPLACE RULE ");
            printIdentifier(shortenName(view.getName() + "_DEL", getMaxTableNameLength()));
            print(" AS ON DELETE TO ");
            printIdentifier(getStructureObjectName(view));
            print(" DO INSTEAD DELETE FROM ");
            printIdentifier(shortenName(rule.getViewTable(), getMaxTableNameLength()));
            print(" WHERE ");
            print(rule.getViewFields().get(0).getField());
            print(" = OLD.");
            print(rule.getViewFields().get(0).getFieldas());            
            printEndOfStatement(getStructureObjectName(view));  
        }
    }    
    
    
    protected void dropUpdateRules(View view) throws IOException {
        
        RuleProcessor rule = new RuleProcessor(view.getStatement());
        
        if (rule.isUpdatable()) {
            // INSERT RULE
            print("DROP RULE IF EXISTS ");
            printIdentifier(shortenName(view.getName() + "_INS", getMaxTableNameLength()));
            print(" ON ");
            printIdentifier(getStructureObjectName(view));
            printEndOfStatement(getStructureObjectName(view));  

            // UPDATE RULE
            print("DROP RULE IF EXISTS ");
            printIdentifier(shortenName(view.getName() + "_UPD", getMaxTableNameLength()));
            print(" ON ");
            printIdentifier(getStructureObjectName(view));
            printEndOfStatement(getStructureObjectName(view));  

            // DELETE RULE
            print("DROP RULE IF EXISTS ");
            printIdentifier(shortenName(view.getName() + "_DEL", getMaxTableNameLength()));
            print(" ON ");
            printIdentifier(getStructureObjectName(view));
            printEndOfStatement(getStructureObjectName(view));  
        }
    }
    
    
    protected Translation createPLSQLFunctionTranslation(Database database) {
        return new PostgrePLSQLFunctionTranslation(database);
    }    
    
    public Translation createPLSQLTriggerTranslation(Database database) {
        return new PostgrePLSQLTriggerTranslation(database);
    }    
    
    protected Translation createSQLTranslation(Database database) {
        return new PostgreSQLTranslation();
    }    
    
    /**
     * {@inheritDoc}
     */
    protected String getNativeFunction(String neutralFunction, int typeCode) throws IOException {
        switch (typeCode) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.FLOAT:
                return neutralFunction;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                if ("SYSDATE".equals(neutralFunction.toUpperCase())) {
                    return "now()";
                } else {
                    return neutralFunction;
                }
            case Types.BIT:
            default:
                return neutralFunction;
        }
    }
    
}
