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

import org.apache.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.tools.ant.Task;

/**
 * This is the base class for the database ant tasks. It provides logging and other base
 * functionality.
 * 
 * @author mtaal
 */
public abstract class BaseDatabaseTask extends Task {

  private static final String OB_REBUILD_APPENDER_NAME = "OBRebuildAppender";

  private String driver;
  private String url;
  private String user;
  private String password;
  private String systemUser;
  private String systemPassword;

  protected Logger log;
  protected boolean doOBRebuildAppender = false;

  public BaseDatabaseTask() {
  }

  /**
   * Initializes the logging.
   */
  protected void initLogging() {
    setupRebuildAppender();
    log = Logger.getLogger(getClass());
  }

  private void setupRebuildAppender() {
    if (!doOBRebuildAppender) {
      final LoggerContext context = LoggerContext.getContext(false);
      final Configuration config = context.getConfiguration();

      Appender appender = config.getAppender(OB_REBUILD_APPENDER_NAME);

      if (appender != null) {
        LoggerConfig rootLoggerConfig = config.getRootLogger();
        rootLoggerConfig.removeAppender(OB_REBUILD_APPENDER_NAME);

        rootLoggerConfig.addAppender(appender, Level.OFF, null);
        context.updateLoggers();
      }
    }
  }

  @Override
  public void execute() {
    initLogging();
    doExecute();
  }

  // Needs to be implemented subclass, does the real execute
  protected abstract void doExecute();

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

  public String getSystemUser() {
    return systemUser;
  }

  public void setSystemUser(String systemUser) {
    this.systemUser = systemUser;
  }

  public String getSystemPassword() {
    return systemPassword;
  }

  public void setSystemPassword(String systemPassword) {
    this.systemPassword = systemPassword;
  }

  public void setLog(Logger log) {
    this.log = log;
  }
}
