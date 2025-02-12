package org.apache.ddlutils.io;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
 * Provides basic live database data <-> XML functionality.
 * 
 * @version $Revision: $
 */
public class DatabaseDataIO implements DataSetTableExporter {
  /**
   * The converters to use for converting between data and its XML representation.
   */
  private ArrayList _converters = new ArrayList();
  /** Whether we should continue when an error was detected. */
  private boolean _failOnError = true;
  /**
   * Whether foreign key order shall be followed when inserting data into the database.
   */
  private boolean _ensureFKOrder = true;
  /** Whether we should use batch mode. */
  private boolean _useBatchMode;
  /** The maximum number of objects to insert in one batch. */
  private Integer _batchSize;

  /** Whether DdlUtils should search for the schema of the tables. @deprecated */
  private boolean _determineSchema;
  /**
   * The schema pattern for finding tables when reading data from a live database. @deprecated
   */
  private String _schemaPattern;

  protected boolean _writePrimaryKeyComment = true;

  private DataSetTableQueryGenerator queryGenerator;

  private final Log _log = LogFactory.getLog(DatabaseDataIO.class);

  public DatabaseDataIO() {
    this.queryGenerator = new DataSetTableQueryGenerator();
  }

  public DatabaseDataIO(DataSetTableQueryGenerator queryGenerator) {
    this.queryGenerator = queryGenerator;
  }

  /**
   * Registers a converter.
   * 
   * @param converterRegistration
   *          The registration info
   */
  public void registerConverter(DataConverterRegistration converterRegistration) {
    _converters.add(converterRegistration);
  }

  /**
   * Determines whether data io is stopped when an error happens.
   * 
   * @return Whether io is stopped when an error was detected (true by default)
   */
  public boolean isFailOnError() {
    return _failOnError;
  }

  /**
   * Specifies whether data io shall be stopped when an error happens.
   * 
   * @param failOnError
   *          Whether io should stop when an error was detected
   */
  public void setFailOnError(boolean failOnError) {
    _failOnError = failOnError;
  }

  /**
   * Determines whether batch mode is used for inserting data into the database.
   * 
   * @return <code>true</code> if batch mode is used
   */
  public boolean getUseBatchMode() {
    return _useBatchMode;
  }

  /**
   * Specifies whether batch mode should be used for inserting data into the database.
   * 
   * @param useBatchMode
   *          <code>true</code> if batch mode shall be used
   */
  public void setUseBatchMode(boolean useBatchMode) {
    _useBatchMode = useBatchMode;
  }

  /**
   * Returns the batch size override.
   * 
   * @return The batch size if different from the default, <code>null</code> otherwise
   */
  public Integer getBatchSize() {
    return _batchSize;
  }

  /**
   * Sets the batch size to be used by this object.
   * 
   * @param batchSize
   *          The batch size if different from the default, or <code>null</code> if the default
   *          shall be used
   */
  public void setBatchSize(Integer batchSize) {
    _batchSize = batchSize;
  }

  /**
   * Determines whether the sink delays the insertion of beans so that the beans referenced by it
   * via foreignkeys are already inserted into the database.
   * 
   * @return <code>true</code> if beans are inserted after its foreignkey-references
   */
  public boolean isEnsureFKOrder() {
    return _ensureFKOrder;
  }

  /**
   * Specifies whether the sink shall delay the insertion of beans so that the beans referenced by
   * it via foreignkeys are already inserted into the database.<br/>
   * Note that you should careful with setting <code>haltOnErrors</code> to false as this might
   * result in beans not inserted at all. The sink will then throw an appropriate exception at the
   * end of the insertion process (method {@link DataSink#end()}).
   * 
   * @param ensureFKOrder
   *          <code>true</code> if beans shall be inserted after its foreignkey-references
   */
  public void setEnsureFKOrder(boolean ensureFKOrder) {
    _ensureFKOrder = ensureFKOrder;
  }

  public boolean isWritePrimaryKeyComment() {
    return _writePrimaryKeyComment;
  }

  /**
   * Specifies if the output should contain <!-- primaryKeyValue --> in each line. Default value is
   * true
   */
  public void setWritePrimaryKeyComment(boolean _writePrimaryKeyComment) {
    this._writePrimaryKeyComment = _writePrimaryKeyComment;
  }

  /**
   * Kept as org.openbravo.ezattributes module in at least version 2.0.5 (for 3.0) is still calling
   * it. The method is changed to do nothing, as in case of that module the existing call was
   * useless.
   */
  @Deprecated
  public void setDatabaseFilter(DatabaseFilter value) {
  }

  /**
   * Specifies whether DdlUtils should try to find the schema of the tables when reading data from a
   * live database.
   * 
   * @param determineSchema
   *          Whether to try to find the table's schemas
   */
  public void setDetermineSchema(boolean determineSchema) {
    _determineSchema = determineSchema;
  }

  /**
   * Sets the schema pattern to find the schemas of tables when reading data from a live database.
   * 
   * @param schemaPattern
   *          The schema pattern
   */
  public void setSchemaPattern(String schemaPattern) {
    _schemaPattern = schemaPattern;
  }

  /**
   * Registers the converters at the given configuration.
   * 
   * @param converterConf
   *          The converter configuration
   */
  protected void registerConverters(ConverterConfiguration converterConf) throws DdlUtilsException {
    for (Iterator it = _converters.iterator(); it.hasNext();) {
      DataConverterRegistration registrationInfo = (DataConverterRegistration) it.next();

      if (registrationInfo.getTypeCode() != Integer.MIN_VALUE) {
        converterConf.registerConverter(registrationInfo.getTypeCode(),
            registrationInfo.getConverter());
      } else {
        if ((registrationInfo.getTable() == null) || (registrationInfo.getColumn() == null)) {
          throw new DdlUtilsException(
              "Please specify either the jdbc type or a table/column pair for which the converter shall be defined");
        }
        converterConf.registerConverter(registrationInfo.getTable(), registrationInfo.getColumn(),
            registrationInfo.getConverter());
      }
    }
  }

  /**
   * Returns a data writer instance configured to write to the indicated file in the specified
   * encoding.
   * 
   * @param path
   *          The path to the output XML data file
   * @param xmlEncoding
   *          The encoding to use for writing the XML
   * @return The writer
   */
  public DataWriter getConfiguredDataWriter(String path, String xmlEncoding)
      throws DdlUtilsException {
    try {
      DataWriter writer = new DataWriter(new FileOutputStream(path), xmlEncoding);

      registerConverters(writer.getConverterConfiguration());
      return writer;
    } catch (IOException ex) {
      throw new DdlUtilsException(ex);
    }
  }

  /**
   * Returns a data writer instance configured to write to the given output stream in the specified
   * encoding.
   * 
   * @param output
   *          The output stream
   * @param xmlEncoding
   *          The encoding to use for writing the XML
   * @return The writer
   */
  public DataWriter getConfiguredDataWriter(OutputStream output, String xmlEncoding)
      throws DdlUtilsException {
    DataWriter writer = new DataWriter(output, xmlEncoding);

    registerConverters(writer.getConverterConfiguration());
    return writer;
  }

  /**
   * Returns a data writer instance configured to write to the given output writer in the specified
   * encoding.
   * 
   * @param output
   *          The output writer; needs to be configured with the specified encoding
   * @param xmlEncoding
   *          The encoding to use for writing the XML
   * @return The writer
   */
  public DataWriter getConfiguredDataWriter(Writer output, String xmlEncoding)
      throws DdlUtilsException {
    DataWriter writer = new DataWriter(output, xmlEncoding);

    registerConverters(writer.getConverterConfiguration());
    return writer;
  }

  /**
   * Sorts the given table according to their foreign key order.
   * 
   * @param tables
   *          The tables
   * @return The sorted tables
   */
  private List sortTables(Table[] tables) {
    ArrayList result = new ArrayList();
    HashSet processed = new HashSet();
    ListOrderedMap pending = new ListOrderedMap();

    for (int idx = 0; idx < tables.length; idx++) {
      Table table = tables[idx];

      if (table.getForeignKeyCount() == 0) {
        result.add(table);
        processed.add(table);
      } else {
        HashSet waitedFor = new HashSet();

        for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
          Table waitedForTable = table.getForeignKey(fkIdx).getForeignTable();

          if (!table.equals(waitedForTable)) {
            waitedFor.add(waitedForTable);
          }
        }
        pending.put(table, waitedFor);
      }
    }

    HashSet newProcessed = new HashSet();

    while (!processed.isEmpty() && !pending.isEmpty()) {
      newProcessed.clear();
      for (Iterator it = pending.entrySet().iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        Table table = (Table) entry.getKey();
        HashSet waitedFor = (HashSet) entry.getValue();

        waitedFor.removeAll(processed);
        if (waitedFor.isEmpty()) {
          it.remove();
          result.add(table);
          newProcessed.add(table);
        }
      }
      processed.clear();

      HashSet tmp = processed;

      processed = newProcessed;
      newProcessed = tmp;
    }
    // the remaining are within circular dependencies
    for (Iterator it = pending.keySet().iterator(); it.hasNext();) {
      result.add(it.next());
    }
    return result;
  }

  public void readRowsIntoDatabaseData(Platform platform, Database model, DatabaseData databaseData,
      OBDataset dataset, String moduleID) {
    for (OBDatasetTable dsTable : dataset.getTableList()) {
      Connection con = platform.borrowConnection();
      Table table = model.findTable(dsTable.getName());
      Vector<DynaBean> rows = this.readRowsFromTableList(con, platform, model, table, dsTable,
          moduleID);
      if (rows != null) {
        for (DynaBean row : rows) {
          databaseData.addRow(table, row, false);
        }
      }
      platform.returnConnection(con);
    }
    databaseData.reorderAllTables();
  }

  @Override
  public boolean exportDataSet(Database model, OBDatasetTable dsTable, OutputStream output,
      String moduleId, Map<String, Object> customParams, boolean orderByTableId) {
    String xmlEncoding = (String) customParams.get("xmlEncoding");
    Platform platform = (Platform) customParams.get("platform");
    boolean anyRecordsHaveBeenExported = false;
    if (orderByTableId) {
      anyRecordsHaveBeenExported = writeDataForTableToXML(platform, model, dsTable, output,
          xmlEncoding, moduleId);
    } else {
      // no need to store the objects in a vector in order to sort them, we write them to the file
      // as they are being read
      anyRecordsHaveBeenExported = streamDataForTableToXML(platform, model, dsTable, output,
          xmlEncoding, moduleId);
    }
    return anyRecordsHaveBeenExported;
  }

  private boolean streamDataForTableToXML(Platform platform, Database model, OBDatasetTable dsTable,
      OutputStream output, String xmlEncoding, String moduleID) {
    DataWriter writer = getConfiguredDataWriter(output, xmlEncoding);
    writer.setWritePrimaryKeyComment(_writePrimaryKeyComment);
    registerConverters(writer.getConverterConfiguration());
    writer.writeDocumentStart();
    long nExportedRows = 0;
    Table table = model.findTable(dsTable.getName());
    Connection con = platform.borrowConnection();

    Table[] atables = { table };
    DataSetTableQueryGeneratorExtraProperties extraProperties = new DataSetTableQueryGeneratorExtraProperties();
    extraProperties.setModuleId(moduleID);
    dsTable.setName(table.getName());
    String sqlstatement = queryGenerator.generateQuery(dsTable, extraProperties);
    try (Statement statement = con.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlstatement);
      Iterator<DynaBean> iterator = platform.createResultSetIterator(model, resultSet, atables);
      while (iterator.hasNext()) {
        DynaBean row = (DynaBean) iterator.next();
        writer.write(model, dsTable, row);
        nExportedRows++;
      }
      if (nExportedRows > 0) {
        _log.info(
            "  " + nExportedRows + " records have been exported from table " + table.getName());
      }
    } catch (SQLException ex) {
      _log.error("SQL command to read rows from table failed: " + sqlstatement, ex);
    } finally {
      platform.returnConnection(con);
      writer.writeDocumentEnd();
    }

    return nExportedRows > 0;
  }

  public boolean writeDataForTableToXML(Platform platform, Database model, OBDatasetTable dsTable,
      OutputStream output, String xmlEncoding, String moduleID) {
    DataWriter writer = getConfiguredDataWriter(output, xmlEncoding);
    writer.setWritePrimaryKeyComment(_writePrimaryKeyComment);
    registerConverters(writer.getConverterConfiguration());
    writer.writeDocumentStart();
    boolean anyRecordsHaveBeenExported = false;
    Table table = model.findTable(dsTable.getName());
    Connection con = platform.borrowConnection();
    Vector<DynaBean> rows = readRowsFromTableList(con, platform, model, table, dsTable, moduleID);
    for (DynaBean row : rows) {
      writer.write(model, dsTable, row);
      anyRecordsHaveBeenExported = true;
    }
    if (rows.size() > 0) {
      _log.info("  " + rows.size() + " records have been exported from table " + table.getName());
    }
    platform.returnConnection(con);
    writer.writeDocumentEnd();
    return anyRecordsHaveBeenExported;

  }

  public int writeDataForTableToXML(Platform platform, Database model, DatabaseData databaseData,
      OBDatasetTable dsTable, OutputStream output, String xmlEncoding, String moduleID) {
    DataWriter writer = getConfiguredDataWriter(output, xmlEncoding);
    writer.setWritePrimaryKeyComment(_writePrimaryKeyComment);
    registerConverters(writer.getConverterConfiguration());
    writer.writeDocumentStart();
    Vector<DynaBean> rows = databaseData.getRowsFromTable(dsTable.getName().toUpperCase());
    if (rows != null) {
      for (DynaBean row : rows) {
        writer.write(model, dsTable, row);
      }
    }
    writer.writeDocumentEnd();
    return rows == null ? 0 : rows.size();
  }

  /**
   * Returns a data reader instance configured for the given platform (which needs to be connected
   * to a live database) and model.
   * 
   * @param platform
   *          The database
   * @param model
   *          The model
   * @return The data reader
   */
  public DataReader getConfiguredDataReader(Platform platform, Database model)
      throws DdlUtilsException {
    DataToDatabaseSink sink = new DataToDatabaseSink(platform, model);
    DataReader reader = new DataReader();

    sink.setHaltOnErrors(_failOnError);
    sink.setEnsureForeignKeyOrder(_ensureFKOrder);
    sink.setUseBatchMode(_useBatchMode);
    if (_batchSize != null) {
      sink.setBatchSize(_batchSize.intValue());
    }

    reader.setModel(model);
    reader.setSink(sink);
    registerConverters(reader.getConverterConfiguration());
    return reader;
  }

  public DataReader getConfiguredCompareDataReader(Database database) {
    DataReader reader = new DataReader();
    reader.setSink(new DataToArraySink());
    reader.setModel(database);
    return reader;
  }

  /**
   * Reads the data from the specified files and writes it to the database to which the given
   * platform is connected.
   * 
   * @param platform
   *          The platform, must be connected to a live database
   * @param files
   *          The XML data files
   */
  public void writeDataToDatabase(Platform platform, String[] files) throws DdlUtilsException {
    writeDataToDatabase(platform, platform.readModelFromDatabase("unnamed"), files);
  }

  /**
   * Reads the data from the given input streams and writes it to the database to which the given
   * platform is connected.
   * 
   * @param platform
   *          The platform, must be connected to a live database
   * @param inputs
   *          The input streams for the XML data
   */
  public void writeDataToDatabase(Platform platform, InputStream[] inputs)
      throws DdlUtilsException {
    writeDataToDatabase(platform, platform.readModelFromDatabase("unnamed"), inputs);
  }

  /**
   * Reads the data from the given input readers and writes it to the database to which the given
   * platform is connected.
   * 
   * @param platform
   *          The platform, must be connected to a live database
   * @param inputs
   *          The input readers for the XML data
   */
  public void writeDataToDatabase(Platform platform, Reader[] inputs) throws DdlUtilsException {
    writeDataToDatabase(platform, platform.readModelFromDatabase("unnamed"), inputs);
  }

  /**
   * Reads the data from the indicated files and writes it to the database to which the given
   * platform is connected. Only data that matches the given model will be written.
   * 
   * @param platform
   *          The platform, must be connected to a live database
   * @param model
   *          The model to which to constrain the written data
   * @param files
   *          The XML data files
   */
  public void writeDataToDatabase(Platform platform, Database model, String[] files)
      throws DdlUtilsException {
    DataReader dataReader = getConfiguredDataReader(platform, model);

    dataReader.getSink().start();
    for (int idx = 0; (files != null) && (idx < files.length); idx++) {
      writeDataToDatabase(dataReader, files[idx]);
    }
    dataReader.getSink().end();
  }

  /**
   * Reads the data from the given input streams and writes it to the database to which the given
   * platform is connected. Only data that matches the given model will be written.
   * 
   * @param platform
   *          The platform, must be connected to a live database
   * @param model
   *          The model to which to constrain the written data
   * @param inputs
   *          The input streams for the XML data
   */
  public void writeDataToDatabase(Platform platform, Database model, InputStream[] inputs)
      throws DdlUtilsException {
    DataReader dataReader = getConfiguredDataReader(platform, model);

    dataReader.getSink().start();
    for (int idx = 0; (inputs != null) && (idx < inputs.length); idx++) {
      writeDataToDatabase(dataReader, inputs[idx]);
    }
    dataReader.getSink().end();
  }

  /**
   * Reads the data from the given input readers and writes it to the database to which the given
   * platform is connected. Only data that matches the given model will be written.
   * 
   * @param platform
   *          The platform, must be connected to a live database
   * @param model
   *          The model to which to constrain the written data
   * @param inputs
   *          The input readers for the XML data
   */
  public void writeDataToDatabase(Platform platform, Database model, Reader[] inputs)
      throws DdlUtilsException {
    DataReader dataReader = getConfiguredDataReader(platform, model);

    dataReader.getSink().start();
    for (int idx = 0; (inputs != null) && (idx < inputs.length); idx++) {
      writeDataToDatabase(dataReader, inputs[idx]);
    }
    dataReader.getSink().end();
  }

  /**
   * Reads the data from the specified files and writes it to the database via the given data
   * reader. Note that the sink that the data reader is configured with, won't be started or ended
   * by this method. This has to be done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param files
   *          The XML data files
   */
  public void writeDataToDatabase(DataReader dataReader, String[] files) throws DdlUtilsException {
    for (int idx = 0; (files != null) && (idx < files.length); idx++) {
      writeDataToDatabase(dataReader, files[idx]);
    }
  }

  /**
   * Reads the data from the given input stream and writes it to the database via the given data
   * reader. Note that the input stream won't be closed by this method. Note also that the sink that
   * the data reader is configured with, won't be started or ended by this method. This has to be
   * done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param inputs
   *          The input streams for the XML data
   */
  public void writeDataToDatabase(DataReader dataReader, InputStream[] inputs)
      throws DdlUtilsException {
    for (int idx = 0; (inputs != null) && (idx < inputs.length); idx++) {
      writeDataToDatabase(dataReader, inputs[idx]);
    }
  }

  /**
   * Reads the data from the given input stream and writes it to the database via the given data
   * reader. Note that the input stream won't be closed by this method. Note also that the sink that
   * the data reader is configured with, won't be started or ended by this method. This has to be
   * done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param inputs
   *          The input readers for the XML data
   */
  public void writeDataToDatabase(DataReader dataReader, Reader[] inputs) throws DdlUtilsException {
    for (int idx = 0; (inputs != null) && (idx < inputs.length); idx++) {
      writeDataToDatabase(dataReader, inputs[idx]);
    }
  }

  /**
   * Reads the data from the given input stream and writes it to the database via the given data
   * reader. Note that the input stream won't be closed by this method. Note also that the sink that
   * the data reader is configured with, won't be started or ended by this method. This has to be
   * done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param files
   *          The input readers for the XML data
   */
  public void writeDataToDatabase(DataReader dataReader, File[] files) throws DdlUtilsException {
    for (int idx = 0; (files != null) && (idx < files.length); idx++) {
      writeDataToDatabase(dataReader, files[idx]);
    }
  }

  /**
   * Reads the data from the indicated XML file and writes it to the database via the given data
   * reader. Note that the sink that the data reader is configured with, won't be started or ended
   * by this method. This has to be done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param path
   *          The path to the XML data file
   */
  public void writeDataToDatabase(DataReader dataReader, String path) throws DdlUtilsException {
    try {
      dataReader.parse(path);
    } catch (Exception ex) {
      throw new DdlUtilsException(ex);
    }
  }

  /**
   * Reads the data from the indicated XML file and writes it to the database via the given data
   * reader. Note that the sink that the data reader is configured with, won't be started or ended
   * by this method. This has to be done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param path
   *          The path to the XML data file
   */
  public void writeDataToDatabase(DataReader dataReader, File file) throws DdlUtilsException {
    try {
      dataReader.parse(file);
    } catch (Exception ex) {
      throw new DdlUtilsException(ex);
    }
  }

  /**
   * Reads the data from the given input stream and writes it to the database via the given data
   * reader. Note that the input stream won't be closed by this method. Note also that the sink that
   * the data reader is configured with, won't be started or ended by this method. This has to be
   * done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param input
   *          The input stream for the XML data
   */
  public void writeDataToDatabase(DataReader dataReader, InputStream input)
      throws DdlUtilsException {
    try {
      dataReader.parse(input);
    } catch (Exception ex) {
      throw new DdlUtilsException(ex);
    }
  }

  /**
   * Reads the data from the given input stream and writes it to the database via the given data
   * reader. Note that the input stream won't be closed by this method. Note also that the sink that
   * the data reader is configured with, won't be started or ended by this method. This has to be
   * done by the code using this method.
   * 
   * @param dataReader
   *          The data reader
   * @param input
   *          The input reader for the XML data
   */
  public void writeDataToDatabase(DataReader dataReader, Reader input) throws DdlUtilsException {
    try {
      dataReader.parse(input);
    } catch (Exception ex) {
      throw new DdlUtilsException(ex);
    }
  }

  public Vector<DynaBean> readRowsFromTableList(Connection connection, Platform platform,
      Database model, Table table, OBDatasetTable dsTable, String moduleId) {
    Table[] atables = { table };
    Statement statement = null;
    ResultSet resultSet = null;
    String sqlstatement = "";
    try {
      statement = connection.createStatement();
      DataSetTableQueryGeneratorExtraProperties extraProperties = new DataSetTableQueryGeneratorExtraProperties();
      extraProperties.setModuleId(moduleId);
      extraProperties.setOrderByClause(queryGenerator.buildOrderByClauseUsingKeyColumns(table));
      dsTable.setName(table.getName());
      sqlstatement = queryGenerator.generateQuery(dsTable, extraProperties);
      resultSet = statement.executeQuery(sqlstatement);
      Iterator it = platform.createResultSetIterator(model, resultSet, atables);
      Vector<DynaBean> dbs = new Vector<DynaBean>();
      while (it.hasNext()) {
        dbs.add((DynaBean) it.next());
      }
      Collections.sort(dbs,
          new BaseDynaBeanIDHexComparator(table.getPrimaryKeyColumns()[0].getName()));
      return dbs;
    } catch (SQLException ex) {
      _log.error("SQL command to read rows from table failed: " + sqlstatement);
      return null;
    }
  }

  private class BaseDynaBeanIDHexComparator implements Comparator<Object> {
    String pkName;

    public BaseDynaBeanIDHexComparator(String pkName) {
      this.pkName = pkName;
    }

    @Override
    public int compare(Object o1, Object o2) {
      if (!(o1 instanceof DynaBean) || !(o2 instanceof DynaBean)) {
        return 0;
      }
      final DynaBean bob1 = (DynaBean) o1;
      final DynaBean bob2 = (DynaBean) o2;
      try {
        final BigInteger bd1 = new BigInteger(bob1.get(pkName).toString(), 32);
        final BigInteger bd2 = new BigInteger(bob2.get(pkName).toString(), 32);
        return bd1.compareTo(bd2);
      } catch (final NumberFormatException n) {
        System.out.println("problem: " + n.getMessage());
        return 0;
      }
    }
  }

  /**
   * Reads the database model from the specified XML file.
   * 
   * @param file
   *          The XML file
   * @return The database model
   * @throws DdlUtilsException
   *           If an error occurs while reading the XML file
   */
  public Database readDatabaseFromFile(File file) throws DdlUtilsException {
    try {
      JAXBContext context = JAXBContext.newInstance(Database.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      return (Database) unmarshaller.unmarshal(file);
    } catch (JAXBException ex) {
      throw new DdlUtilsException("Error reading database model from file: " + file.getPath(), ex);
    }
  }

  /**
   * Writes the database model to the specified XML file.
   * 
   * @param database
   *          The database model
   * @param file
   *          The XML file
   * @throws DdlUtilsException
   *           If an error occurs while writing the XML file
   */
  public void writeDatabaseToFile(Database database, File file) throws DdlUtilsException {
    try {
      JAXBContext context = JAXBContext.newInstance(Database.class);
      Marshaller marshaller = context.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      marshaller.marshal(database, file);
    } catch (JAXBException ex) {
      throw new DdlUtilsException("Error writing database model to file: " + file.getPath(), ex);
    }
  }

  /**
   * Reads the database model from the specified XML input stream.
   * 
   * @param inputStream
   *          The XML input stream
   * @return The database model
   * @throws DdlUtilsException
   *           If an error occurs while reading the XML input stream
   */
  public Database readDatabaseFromInputStream(InputStream inputStream) throws DdlUtilsException {
    try {
      JAXBContext context = JAXBContext.newInstance(Database.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      return (Database) unmarshaller.unmarshal(inputStream);
    } catch (JAXBException ex) {
      throw new DdlUtilsException("Error reading database model from input stream", ex);
    }
  }

  /**
   * Writes the database model to the specified XML output stream.
   * 
   * @param database
   *          The database model
   * @param outputStream
   *          The XML output stream
   * @throws DdlUtilsException
   *           If an error occurs while writing the XML output stream
   */
  public void writeDatabaseToOutputStream(Database database, OutputStream outputStream)
      throws DdlUtilsException {
    try {
      JAXBContext context = JAXBContext.newInstance(Database.class);
      Marshaller marshaller = context.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      marshaller.marshal(database, outputStream);
    } catch (JAXBException ex) {
      throw new DdlUtilsException("Error writing database model to output stream", ex);
    }
  }

  /**
   * Reads the database model from the specified XML reader.
   * 
   * @param reader
   *          The XML reader
   * @return The database model
   * @throws DdlUtilsException
   *           If an error occurs while reading the XML reader
   */
  public Database readDatabaseFromReader(Reader reader) throws DdlUtilsException {
    try {
      JAXBContext context = JAXBContext.newInstance(Database.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      return (Database) unmarshaller.unmarshal(reader);
    } catch (JAXBException ex) {
      throw new DdlUtilsException("Error reading database model from reader", ex);
    }
  }

  /**
   * Writes the database model to the specified XML writer.
   * 
   * @param database
   *          The database model
   * @param writer
   *          The XML writer
   * @throws DdlUtilsException
   *           If an error occurs while writing the XML writer
   */
  public void writeDatabaseToWriter(Database database, Writer writer) throws DdlUtilsException {
    try {
      JAXBContext context = JAXBContext.newInstance(Database.class);
      Marshaller marshaller = context.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      marshaller.marshal(database, writer);
    } catch (JAXBException ex) {
      throw new DdlUtilsException("Error writing database model to writer", ex);
    }
  }
}
