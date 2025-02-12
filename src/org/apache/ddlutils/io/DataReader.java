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

import java.io.InputStream;
import java.io.Reader;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.ddlutils.model.Database;

/**
 * Reads data XML into dyna beans matching a specified database model. Note that the data sink won't
 * be started or ended by the data reader, this has to be done in the code that uses the data
 * reader.
 * 
 * @version $Revision: 289996 $
 */
public class DataReader {
  /** The database model. */
  private Database _model;
  /** The object to receive the read beans. */
  private DataSink _sink;
  /** The converters. */
  private ConverterConfiguration _converterConf = new ConverterConfiguration();

  /**
   * Returns the converter configuration of this data reader.
   * 
   * @return The converter configuration
   */
  public ConverterConfiguration getConverterConfiguration() {
    return _converterConf;
  }

  /**
   * Returns the database model.
   * 
   * @return The model
   */
  public Database getModel() {
    return _model;
  }

  /**
   * Sets the database model.
   * 
   * @param model
   *          The model
   */
  public void setModel(Database model) {
    _model = model;
  }

  /**
   * Returns the data sink.
   * 
   * @return The sink
   */
  public DataSink getSink() {
    return _sink;
  }

  /**
   * Sets the data sink.
   * 
   * @param sink
   *          The sink
   */
  public void setSink(DataSink sink) {
    _sink = sink;
  }

  /**
   * Parses the XML data from the given input stream.
   * 
   * @param input
   *          The input stream
   */
  public void parse(InputStream input) throws JAXBException {
    JAXBContext context = JAXBContext.newInstance(Database.class);
    Unmarshaller unmarshaller = context.createUnmarshaller();
    Database database = (Database) unmarshaller.unmarshal(input);
    processDatabase(database);
  }

  /**
   * Parses the XML data from the given reader.
   * 
   * @param reader
   *          The reader
   */
  public void parse(Reader reader) throws JAXBException {
    JAXBContext context = JAXBContext.newInstance(Database.class);
    Unmarshaller unmarshaller = context.createUnmarshaller();
    Database database = (Database) unmarshaller.unmarshal(reader);
    processDatabase(database);
  }

  /**
   * Processes the unmarshalled database and sends the data to the sink.
   * 
   * @param database
   *          The unmarshalled database
   */
  private void processDatabase(Database database) {
    List<Table> tables = database.getTables();
    for (Table table : tables) {
      List<DynaBean> rows = table.getRows();
      for (DynaBean row : rows) {
        _sink.addBean(row);
      }
    }
  }
}
