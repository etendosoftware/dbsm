/*
 *************************************************************************
 * The contents of this file are subject to the Openbravo  Public  License
 * Version  1.1  (the  "License"),  being   the  Mozilla   Public  License
 * Version 1.1  with a permitted attribution clause; you may not  use this
 * file except in compliance with the License. You  may  obtain  a copy of
 * the License at http://www.openbravo.com/legal/license.html 
 * Software distributed under the License  is  distributed  on  an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific  language  governing  rights  and  limitations
 * under the License. 
 * The Original Code is Openbravo ERP. 
 * The Initial Developer of the Original Code is Openbravo SLU 
 * All portions are Copyright (C) 2009-2018 Openbravo SLU
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */

package org.openbravo.ddlutils.task;

import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Logger;
import org.openbravo.dal.core.DalInitializingTask;

/**
 * This is the base class for the database ant tasks. It provides logging and other base
 * functionality.
 * 
 * @author mtaal
 */
public abstract class BaseDalInitializingTask extends DalInitializingTask {

  private String driver;
  private String url;
  private String user;
  private String password;

  protected Logger log;
  private VerbosityLevel verbosity = null;

  /** Creates a new instance of CreateDatabase */
  public BaseDalInitializingTask() {
  }

  /**
   * Initializes the logging.
   */
  protected void initLogging() {
    log = Logger.getLogger(getClass());
  }

  /**
   * Specifies the verbosity of the task's debug output.
   * 
   * @param level
   *          The verbosity level
   * @ant.not-required Default is <code>INFO</code>.
   */
  public void setVerbosity(VerbosityLevel level) {
    verbosity = level;
  }

  public Logger getLog() {
    if (log == null) {
      initLogging();
    }
    return log;
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

  public void setLog(Logger log) {
    this.log = log;
  }

  public VerbosityLevel getVerbosity() {
    return verbosity;
  }
}
