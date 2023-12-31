package org.apache.ddlutils.task;

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

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;

/**
 * Task for getting structural info and data from a live database. Eg. it has sub tasks for writing
 * the schema of the live database or the data currently in it to an XML file, for creating the DTDs
 * for these data files, and for generating SQL to creating a schema in the database to a file.
 * <br/>
 * Example:<br/>
 * 
 * <pre>
 * &lt;taskdef classname=&quot;org.apache.ddlutils.task.DatabaseToDdlTask&quot;
 *          name=&quot;databaseToDdl&quot;
 *          classpathref=&quot;project-classpath&quot; /&gt;
 * 
 * &lt;databaseToDdl usedelimitedsqlidentifiers=&quot;true&quot;
 *                modelname=&quot;example&quot;&gt;
 *   &lt;database driverclassname=&quot;org.apache.derby.jdbc.ClientDriver&quot;
 *             url=&quot;jdbc:derby://localhost/ddlutils&quot;
 *             username=&quot;ddlutils&quot;
 *             password=&quot;ddlutils&quot;/&gt; 
 * 
 *   &lt;writeschematofile outputfile=&quot;schema.xml&quot;/&gt; 
 *   &lt;writedatatofile outputfile=&quot;data.xml&quot;
 *                    encoding=&quot;ISO-8859-1&quot;/&gt; 
 * &lt;/databaseToDdl&gt;
 * </pre>
 * 
 * This reads the schema and data from the database and writes them to XML files.
 * 
 * @version $Revision: 289996 $
 * @ant.task name="databaseToDdl"
 */
public class DatabaseToDdlTask extends DatabaseTaskBase {
  /** The table types to recognize when reading the model from the database. */
  private String _tableTypes;
  /** The name of the model read from the database. */
  private String _modelName = "unnamed";

  /**
   * Specifies the table types to be processed. More precisely, all tables that are of a type not in
   * this list, will be ignored by the task and its sub tasks. For details and typical table types
   * see <a href=
   * "http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html#getTables(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String[])"
   * >java.sql.DatabaseMetaData#getTables</a>.
   * 
   * @param tableTypes
   *          The table types as a comma-separated list
   * @ant.not-required By default, only tables of type <code>TABLE</code> are used by the task.
   */
  public void setTableTypes(String tableTypes) {
    _tableTypes = tableTypes;
  }

  /**
   * Specifies the name of the model that is read from the database. This is mostly useful for the
   * the <code>writeSchemaToFile</code> sub-task as it ensures that the generated XML defines a
   * valid model.
   * 
   * @param modelName
   *          The model name. Use <code>null</code> or an empty string for the default name
   * @ant.not-required By default, DldUtils uses the schema name returned from the database or
   *                   <code>"default"</code> if no schema name was returned by the database.
   */
  public void setModelName(String modelName) {
    _modelName = modelName;
  }

  /**
   * Adds the "create dtd"-command.
   * 
   * @param command
   *          The command
   */
  public void addWriteDtdToFile(WriteDtdToFileCommand command) {
    addCommand(command);
  }

  /**
   * Adds the "write schema to file"-command.
   * 
   * @param command
   *          The command
   */
  public void addWriteSchemaToFile(WriteSchemaToFileCommand command) {
    addCommand(command);
  }

  /**
   * Adds the "write schema sql to file"-command.
   * 
   * @param command
   *          The command
   */
  public void addWriteSchemaSqlToFile(WriteSchemaSqlToFileCommand command) {
    addCommand(command);
  }

  /**
   * Adds the "write data into database"-command.
   * 
   * @param command
   *          The command
   */
  public void addWriteDataToDatabase(WriteDataToDatabaseCommand command) {
    addCommand(command);
  }

  /**
   * Adds the "write data into file"-command.
   * 
   * @param command
   *          The command
   */
  public void addWriteDataToFile(WriteDataToFileCommand command) {
    addCommand(command);
  }

  /**
   * Returns the table types to recognize.
   * 
   * @return The table types
   */
  private String[] getTableTypes() {
    if ((_tableTypes == null) || (_tableTypes.length() == 0)) {
      return new String[0];
    }

    StringTokenizer tokenizer = new StringTokenizer(_tableTypes, ",");
    ArrayList result = new ArrayList();

    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken().trim();

      if (token.length() > 0) {
        result.add(token);
      }
    }
    return (String[]) result.toArray(new String[result.size()]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Database readModel() {
    if (getDataSource() == null) {
      throw new BuildException("No database specified.");
    }

    try {
      return getPlatform().readModelFromDatabase(_modelName,
          getPlatformConfiguration().getCatalogPattern(),
          getPlatformConfiguration().getSchemaPattern(), getTableTypes());
    } catch (Exception ex) {
      throw new BuildException(
          "Could not read the schema from the specified database: " + ex.getLocalizedMessage(), ex);
    }
  }
}
