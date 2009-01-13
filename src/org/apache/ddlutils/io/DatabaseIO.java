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

import java.beans.IntrospectionException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.commons.betwixt.io.BeanReader;
import org.apache.commons.betwixt.io.BeanWriter;
import org.apache.commons.betwixt.strategy.HyphenatedNameMapper;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.View;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This class provides functions to read and write database models from/to XML.
 * 
 * @version $Revision: 481151 $
 */
public class DatabaseIO {
    /**
     * The name of the XML attribute use to denote that teh content of a data
     * XML element uses Base64 encoding.
     */
    public static final String BASE64_ATTR_NAME = "base64";

    /** Whether to validate the XML. */
    private boolean _validateXml = true;
    /** Whether to use the internal dtd that comes with DdlUtils. */
    private boolean _useInternalDtd = true;

    /**
     * Returns whether XML is validated upon reading it.
     * 
     * @return <code>true</code> if read XML is validated
     */
    public boolean isValidateXml() {
        return _validateXml;
    }

    /**
     * Specifies whether XML shall be validated upon reading it.
     * 
     * @param validateXml
     *            <code>true</code> if read XML shall be validated
     */
    public void setValidateXml(boolean validateXml) {
        _validateXml = validateXml;
    }

    /**
     * Returns whether the internal dtd that comes with DdlUtils is used.
     * 
     * @return <code>true</code> if parsing uses the internal dtd
     */
    public boolean isUseInternalDtd() {
        return _useInternalDtd;
    }

    /**
     * Specifies whether the internal dtd is to be used.
     * 
     * @param useInternalDtd
     *            Whether to use the internal dtd
     */
    public void setUseInternalDtd(boolean useInternalDtd) {
        _useInternalDtd = useInternalDtd;
    }

    /**
     * Returns the commons-betwixt mapping file as an
     * {@link org.xml.sax.InputSource} object. Per default, this will be
     * classpath resource under the path <code>/mapping.xml</code>.
     * 
     * @return The input source for the mapping
     */
    protected InputSource getBetwixtMapping() {
        return new InputSource(getClass().getResourceAsStream("/mapping.xml"));
    }

    /**
     * Returns a new bean reader configured to read database models.
     * 
     * @return The reader
     */
    protected BeanReader getReader() throws IntrospectionException,
            SAXException, IOException {
        BeanReader reader = new BeanReader();

        reader.getXMLIntrospector().getConfiguration()
                .setAttributesForPrimitives(true);
        reader.getXMLIntrospector().getConfiguration()
                .setWrapCollectionsInElement(false);
        reader.getXMLIntrospector().getConfiguration().setElementNameMapper(
                new HyphenatedNameMapper());
        reader.setValidating(isValidateXml());
        if (isUseInternalDtd()) {
            reader.setEntityResolver(new LocalEntityResolver());
        }
        reader.registerMultiMapping(getBetwixtMapping());

        return reader;
    }

    /**
     * Returns a new bean writer configured to writer database models.
     * 
     * @param output
     *            The target output writer
     * @return The writer
     */
    protected BeanWriter getWriter(OutputStream output)
            throws DdlUtilsException {
        try {
            BeanWriter writer = new BeanWriter(output, "UTF8");

            writer.getXMLIntrospector().register(getBetwixtMapping());
            writer.getXMLIntrospector().getConfiguration()
                    .setAttributesForPrimitives(true);
            writer.getXMLIntrospector().getConfiguration()
                    .setWrapCollectionsInElement(false);
            writer.getXMLIntrospector().getConfiguration()
                    .setElementNameMapper(new HyphenatedNameMapper());
            writer.getBindingConfiguration().setMapIDs(false);
            writer.enablePrettyPrint();

            return writer;
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
    }

    /**
     * Returns a new bean writer configured to writer database models.
     * 
     * @param output
     *            The target output writer
     * @return The writer
     */
    protected BeanWriter getWriter(Writer writer_) throws DdlUtilsException {
        try {
            BeanWriter writer = new BeanWriter(writer_);

            writer.getXMLIntrospector().register(getBetwixtMapping());
            writer.getXMLIntrospector().getConfiguration()
                    .setAttributesForPrimitives(true);
            writer.getXMLIntrospector().getConfiguration()
                    .setWrapCollectionsInElement(false);
            writer.getXMLIntrospector().getConfiguration()
                    .setElementNameMapper(new HyphenatedNameMapper());
            writer.getBindingConfiguration().setMapIDs(false);
            writer.enablePrettyPrint();

            return writer;
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
    }

    /**
     * Reads the database model contained in the specified file.
     * 
     * @param filename
     *            The model file name
     * @return The database model
     */
    public Database read(String filename) throws DdlUtilsException {
        Database model = null;

        try {
            model = (Database) getReader().parse(filename);
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
        model.initialize();
        return model;
    }

    /**
     * Reads the database model contained in the specified file but does not
     * initialize the database
     * 
     * @param file
     *            The model file
     * @return The database model
     */
    public Database readplain(File file) throws DdlUtilsException {
        Database model = null;

        try {
            model = (Database) getReader().parse(file);
        } catch (Exception ex) {
            throw new DdlUtilsException(ex.getMessage() + " : "
                    + file.getPath(), ex);
        }
        return model;
    }

    /**
     * Reads the database model contained in the specified file.
     * 
     * @param file
     *            The model file
     * @return The database model
     */
    public Database read(File file) throws DdlUtilsException {
        Database model = null;

        try {
            model = (Database) getReader().parse(file);
        } catch (Exception ex) {
            throw new DdlUtilsException(ex.getMessage() + " : "
                    + file.getPath(), ex);
        }
        model.initialize();
        return model;
    }

    /**
     * Reads the database model given by the reader.
     * 
     * @param reader
     *            The reader that returns the model XML
     * @return The database model
     */
    public Database read(Reader reader) throws DdlUtilsException {
        Database model = null;

        try {
            model = (Database) getReader().parse(reader);
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
        model.initialize();
        return model;
    }

    /**
     * Reads the database model from the given input source.
     * 
     * @param source
     *            The input source
     * @return The database model
     */
    public Database read(InputSource source) throws DdlUtilsException {
        Database model = null;

        try {
            model = (Database) getReader().parse(source);
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
        model.initialize();
        return model;
    }

    /**
     * 
     * Read recursively all the xml files of the database of a given directory
     * 
     * @param f
     *            The directory that contains the database model in xml format
     * @return Array of the files contained in the directory
     */
    public static File[] readFileArray(File f) {

        if (f.isDirectory()) {

            ArrayList<File> fileslist = new ArrayList<File>();

            File[] directoryfiles = f.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return !pathname.isHidden()
                            && (pathname.isDirectory() || (pathname.isFile() && pathname
                                    .getName().endsWith(".xml")));
                }
            });

            for (File file : directoryfiles) {
                File[] ff = readFileArray(file);
                for (File fileint : ff) {
                    fileslist.add(fileint);
                }
            }

            return fileslist.toArray(new File[fileslist.size()]);
        } else {
            return new File[] { f };
        }
    }

    /**
     * 
     * Writes the database model to the specified directory.
     * 
     * @param model
     *            The database model
     * @param dir
     *            The root directory for output
     * @throws DdlUtilsException
     */
    public void writeToDir(Database model, File dir) throws DdlUtilsException {

        Database d;
        File subdir;

        // remove all .xml files
        dir.mkdirs();
        File[] filestodelete = readFileArray(dir);
        for (File filedelete : filestodelete) {
            filedelete.delete();
        }

        // Write tables
        subdir = new File(dir, "tables");
        subdir.mkdirs();

        for (int i = 0; i < model.getTableCount(); i++) {
            Table t = model.getTable(i);
            d = new Database();
            d.setName("TABLE " + t.getName());
            d.addTable(t);
            write(d, new File(subdir, t.getName() + ".xml"));
        }
        // Write modified tables
        subdir = new File(dir, "modifiedTables");
        if (model.getModifiedTableCount() > 0) {
            subdir.mkdirs();

            for (int i = 0; i < model.getModifiedTableCount(); i++) {
                Table t = model.getModifiedTable(i);
                d = new Database();
                d.setName("MODIFIED TABLE " + t.getName());
                d.addTable(t);
                write(d, new File(subdir, t.getName() + ".xml"));
            }
        }

        // Write views
        subdir = new File(dir, "views");
        subdir.mkdirs();

        for (int i = 0; i < model.getViewCount(); i++) {
            View v = model.getView(i);
            d = new Database();
            d.setName("VIEW " + v.getName());
            d.addView(v);
            write(d, new File(subdir, v.getName() + ".xml"));
        }

        // Write sequences
        subdir = new File(dir, "sequences");
        subdir.mkdirs();

        for (int i = 0; i < model.getSequenceCount(); i++) {
            Sequence s = model.getSequence(i);
            d = new Database();
            d.setName("SEQUENCE " + s.getName());
            d.addSequence(s);
            write(d, new File(subdir, s.getName() + ".xml"));
        }

        // Write functions
        subdir = new File(dir, "functions");
        subdir.mkdirs();

        for (int i = 0; i < model.getFunctionCount(); i++) {
            Function f = model.getFunction(i);
            d = new Database();
            d.setName("FUNCTION " + f.getName());
            d.addFunction(f);
            write(d, new File(subdir, f.getName() + ".xml"));
        }

        // Write trigger
        subdir = new File(dir, "triggers");
        subdir.mkdirs();

        for (int i = 0; i < model.getTriggerCount(); i++) {
            Trigger t = model.getTrigger(i);
            d = new Database();
            d.setName("TRIGGER " + t.getName());
            d.addTrigger(t);
            write(d, new File(subdir, t.getName() + ".xml"));
        }
    }

    /**
     * Writes the database model to the specified file.
     * 
     * @param model
     *            The database model
     * @param file
     *            The model file
     */
    public void write(Database model, File file) throws DdlUtilsException {
        try {
            FileOutputStream writer = null;

            try {
                writer = new FileOutputStream(file);

                write(model, writer);
                writer.flush();
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
    }

    /**
     * Writes the database model to the specified file.
     * 
     * @param model
     *            The database model
     * @param filename
     *            The model file name
     */
    public void write(Database model, String filename) throws DdlUtilsException {
        try {
            FileOutputStream writer = null;

            try {
                writer = new FileOutputStream(filename);

                write(model, writer);
                writer.flush();
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
    }

    /**
     * Writes the database model to the given output stream. Note that this
     * method does not flush the stream.
     * 
     * @param model
     *            The database model
     * @param output
     *            The output stream
     */
    public void write(Database model, OutputStream output)
            throws DdlUtilsException {
        write(model, getWriter(output));
    }

    /**
     * Writes the database model to the given output writer. Note that this
     * method does not flush the writer.
     * 
     * @param model
     *            The database model
     * @param output
     *            The output writer
     */
    public void write(Database model, Writer output) throws DdlUtilsException {
        write(model, getWriter(output));
    }

    /**
     * Internal method that writes the database model using the given bean
     * writer.
     * 
     * @param model
     *            The database model
     * @param writer
     *            The bean writer
     */
    private void write(Database model, BeanWriter writer)
            throws DdlUtilsException {
        try {
            // writer.writeXmlDeclaration("<?xml version=\"1.0\"?>\n<!DOCTYPE
            // database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">");
            writer.writeXmlDeclaration("<?xml version=\"1.0\"?>");
            writer.write(model);
        } catch (Exception ex) {
            throw new DdlUtilsException(ex);
        }
    }

    public void write(File file, Vector<Change> changes) {
        try {
            BeanWriter writer = getWriter(new FileOutputStream(file));
            writer.writeXmlDeclaration("<?xml version=\"1.0\"?>");
            writer.flush();

            // writer.write(new ChangeVector(changes));
            // writer.write(changes);

            for (Change change : changes) {
                writer.write(change);
                writer.flush();
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Vector<Change> readChanges(File file) {
        try {
            BeanReader reader = getReader();
            reader.setValidating(false);
            Vector<Change> changes = new Vector<Change>();
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String header = br.readLine();
            String line = "";
            while ((line = br.readLine()) != null) {
              Change change = (Change) reader.parse(new ByteArrayInputStream(
                      (line).getBytes()));
              changes.add(change);
            }

            return changes;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
