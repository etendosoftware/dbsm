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

package org.openbravo.ddlutils.task;

import java.io.File;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Task;

/**
 *
 * @author Adrian
 */
public class CreateDatabase extends Task {
    
    private String driver;
    private String url;
    private String user;
    private String password;
    
    private File prescript = null;
    private File postscript = null;
    
    private File model;   
    private boolean dropfirst = false;
    private boolean failonerror = false;
    
    private String object = null;

    protected Log _log;
    private VerbosityLevel _verbosity = null;
    
    private String basedir;
    private String dirFilter;

    /** Creates a new instance of CreateDatabase */
    public CreateDatabase() {
    }
    
    /**
     * Initializes the logging.
     */
    private void initLogging() {
        // For Ant, we're forcing DdlUtils to do logging via log4j to the console
        Properties props = new Properties();
        String     level = (_verbosity == null ? Level.INFO.toString() : _verbosity.getValue()).toUpperCase();

        props.setProperty("log4j.rootCategory", level + ",A");
        props.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        props.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.A.layout.ConversionPattern", "%m%n");
        // we don't want debug logging from Digester/Betwixt
        props.setProperty("log4j.logger.org.apache.commons", "WARN");

        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);

        _log = LogFactory.getLog(getClass());
    }
    
    @Override
	public void execute() {
       
        initLogging();
    
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(getDriver());
        ds.setUrl(getUrl());
        ds.setUsername(getUser());
        ds.setPassword(getPassword());    
        
        Platform platform = PlatformFactory.createNewPlatformInstance(ds);
        // platform.setDelimitedIdentifierModeOn(true);
		
        
        try {  
            
            // execute the pre-script
            if (getPrescript() == null) {
                // try to execute the default prescript
                File fpre = new File(getModel(), "prescript-" + platform.getName() + ".sql");
                if (fpre.exists()) {
                    _log.info("Executing default prescript");
                    platform.evaluateBatch(DatabaseUtils.readFile(fpre), !isFailonerror());
                }
            } else {
                platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()), !isFailonerror());
            }

            Database db =null;
            if(basedir==null)
            {
              _log.info("Basedir for additional files not specified. Creating database with just Core.");
              db = DatabaseUtils.readDatabase(getModel());
            }
            else
            {
              //We read model files using the filter, obtaining a file array. The models will be merged
              //to create a final target model.
              Vector<File> dirs=new Vector<File>();
              dirs.add(model);
              DirectoryScanner dirScanner=new DirectoryScanner();
              dirScanner.setBasedir(new File(basedir));
              String[] dirFilterA = {dirFilter};
              dirScanner.setIncludes(dirFilterA);
              dirScanner.scan();
              String[] incDirs=dirScanner.getIncludedDirectories();
              for(int j=0;j<incDirs.length;j++)
              {
                File dirF=new File(basedir, incDirs[j]);
                dirs.add(dirF);
              }
              File[] fileArray=new File[dirs.size()];
              for(int i=0;i<dirs.size();i++)
              {
                fileArray[i]=dirs.get(i);
              }
              db = DatabaseUtils.readDatabase(fileArray);
            }
            
            // Create database
            _log.info("Executing creation script");
            // crop database if needed
            if (object != null) {
                Database empty = new Database();
                empty.setName("empty");
                db = DatabaseUtils.cropDatabase(empty, db, object);
                _log.info("for database object " + object);                
            } else {
                _log.info("for the complete database");                
            }             
            
            
            
            platform.createTables(db, isDropfirst(), !isFailonerror());   
            
            // execute the post-script
            if (getPostscript() == null) {
                // try to execute the default prescript
                File fpost = new File(getModel(), "postscript-" + platform.getName() + ".sql");
                if (fpost.exists()) {
                    _log.info("Executing default postscript");
                    platform.evaluateBatch(DatabaseUtils.readFile(fpost), !isFailonerror());
                }                
            } else {
                platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), !isFailonerror());
            }            
            
            
        } catch (Exception e) {
            throw new BuildException(e);
        }   
    }
    
    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public File getModel() {
        return model;
    }

    public void setModel(File model) {
        this.model = model;
    }
    
    public String getBasedir() {
        return basedir;
    }
    
    public void setBasedir(String basedir)
    {
      this.basedir=basedir;
    }
    
    public String getDirFilter() {
      return dirFilter;
    }
    
    public void setDirFilter(String dirFilter) {
      this.dirFilter=dirFilter;
    }

    public boolean isDropfirst() {
        return dropfirst;
    }

    public void setDropfirst(boolean dropfirst) {
        this.dropfirst = dropfirst;
    }

    public boolean isFailonerror() {
        return failonerror;
    }

    public void setFailonerror(boolean failonerror) {
        this.failonerror = failonerror;
    }    

    public File getPrescript() {
        return prescript;
    }

    public void setPrescript(File prescript) {
        this.prescript = prescript;
    }

    public File getPostscript() {
        return postscript;
    }

    public void setPostscript(File postscript) {
        this.postscript = postscript;
    }    
    
    public void setObject(String object) {
        if (object == null || object.trim().startsWith("$") || object.trim().equals("")) {
            this.object = null;
        } else {            
            this.object = object;
        }
    }
    
    public String getObject() {
        return object;
    }
    
    /**
     * Specifies the verbosity of the task's debug output.
     * 
     * @param level The verbosity level
     * @ant.not-required Default is <code>INFO</code>.
     */
    public void setVerbosity(VerbosityLevel level)
    {
        _verbosity = level;
    }
    /*
    public void addDirset(DirSet set) throws BuildException
    {
      _log.info(set.toString());
      filesets.addElement(set);
    }*/
}
