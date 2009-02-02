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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.postgresql.PostgrePLSQLFunctionTranslation;
import org.apache.ddlutils.platform.postgresql.PostgrePLSQLTriggerTranslation;
import org.apache.ddlutils.translation.NullTranslation;
import org.apache.ddlutils.translation.Translation;
import org.apache.tools.ant.BuildException;

/**
 * 
 * @author adrian
 */
public class CompareDatabase extends BaseDatabaseTask {

  private String excludeobjects = "com.openbravo.db.OpenbravoExcludeFilter";// "org.apache.ddlutils.platform.ExcludeFilter";

  private File model;

  /** Creates a new instance of ExportDatabase */
  public CompareDatabase() {
  }

  @Override
  public void doExecute() {

    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(getDriver());
    ds.setUrl(getUrl());
    ds.setUsername(getUser());
    ds.setPassword(getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // platform.setDelimitedIdentifierModeOn(true);

    try {

      // Load database
      final Database db1 = platform.loadModelFromDatabase(DatabaseUtils
          .getExcludeFilter(excludeobjects));
      // if (db1 == null) {
      // db1 = DatabaseUtils.loadCurrentDatabase(ds);
      // }
      getLog().info("Platform database");
      getLog().info(db1.toString());

      // Load database
      final Database db2 = DatabaseUtils.readDatabase(getModel());
      getLog().info("Model database");
      getLog().info(db2.toString());

      // Compare tables
      for (int i = 0; i < db1.getTableCount(); i++) {
        final Table t1 = db1.getTable(i);
        final Table t2 = db2.findTable(t1.getName());

        if (t2 == null) {
          getLog().info("DIFF: TABLE NOT EXISTS " + t1.getName());
        } else {
          if (!t1.equals(t2)) {
            getLog().info("DIFF: TABLES DIFFERENTS " + t1.getName());
          }
        }
      }

      // Compare views
      for (int i = 0; i < db1.getViewCount(); i++) {
        final View w1 = db1.getView(i);
        final View w2 = db2.findView(w1.getName());

        if (w2 == null) {
          getLog().info("DIFF: VIEW NOT EXISTS " + w1.getName());
        } else {
          if (!w1.equals(w2)) {
            getLog().info("DIFF: VIEWS DIFFERENTS " + w1.getName());
          }
        }
      }

      // Compare sequences
      for (int i = 0; i < db1.getSequenceCount(); i++) {
        final Sequence s1 = db1.getSequence(i);
        final Sequence s2 = db2.findSequence(s1.getName());

        if (s2 == null) {
          getLog().info("DIFF: SEQUENCE NOT EXISTS " + s1.getName());
        } else {
          if (!s1.equals(s2)) {
            getLog().info("DIFF: SEQUENCES DIFFERENTS " + s1.getName());
          }
        }
      }

      Translation triggerTranslation = new NullTranslation();
      Translation functionTranslation = null;
      if (platform.getName().contains("Postgre")) {
        triggerTranslation = new PostgrePLSQLTriggerTranslation(db2);
        functionTranslation = new PostgrePLSQLFunctionTranslation(db2);
      } else if (platform.getName().contains("Oracle")) {
        triggerTranslation = new NullTranslation();
        functionTranslation = new NullTranslation();

      }
      /*
       * for(int i=0;i<db2.getFunctionCount();i++) System.out.println(db2.getFunction(i));
       */
      // Compare functions
      for (int i = 0; i < db1.getFunctionCount(); i++) {
        final Function f1 = db1.getFunction(i);
        Function f2 = null;
        if (platform.getName().contains("Postgre"))
          f2 = db2.findFunctionWithParams(f1.getName(), f1.getParameters());
        else
          f2 = db2.findFunction(f1.getName());

        if (f2 == null) {
          f2 = db2.findFunction(f1.getName());
          if (f2 != null) {
            getLog().info("DIFF: FUNCTION DIFFERENT " + f1.getName() + " (different parameters)");
            /*
             * System.out.println(f1.getName()); Parameter[] parameters=f1.getParameters(); for(int
             * ind=0;ind<parameters.length;ind++) System.out.println(parameters[ind]);
             * parameters=f2.getParameters(); for(int ind=0;ind<parameters.length;ind++)
             * System.out.println(parameters[ind]); System.out.println(f1.getTypeCode());
             * System.out.println(f2.getTypeCode());
             */
          } else
            getLog().info("DIFF: FUNCTION NOT EXISTS " + f1.getName());
        } else {
          f2.setTranslation(functionTranslation);
          if (!f1.equals(f2)) {
            getLog().info("DIFF: FUNCTION DIFFERENT " + f1.getName());

          }
        }
      }

      // Compare TRIGGERS
      for (int i = 0; i < db1.getTriggerCount(); i++) {
        final Trigger t1 = db1.getTrigger(i);
        final Trigger t2 = db2.findTrigger(t1.getName());

        if (t2 == null) {
          getLog().info("DIFF: TRIGGER NOT EXISTS " + t1.getName());
        } else {
          t2.setTranslation(triggerTranslation);
          if (!t1.equals(t2)) {
            getLog().info("DIFF: TRIGGERS DIFFERENTS " + t1.getName());
            System.out.println(t1.getBody());
            System.out.println(t2.getBody());
            System.out.println(t1.getBody().equals(t2.getBody()));
            System.exit(1);
          }
        }
      }

    } catch (final Exception e) {
      // log(e.getLocalizedMessage());
      throw new BuildException(e);
    }
  }

  public String getExcludeobjects() {
    return excludeobjects;
  }

  public void setExcludeobjects(String excludeobjects) {
    this.excludeobjects = excludeobjects;
  }

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
  }
}
