package org.openbravo.ddlutils.task;

import java.io.File;

import org.apache.ddlutils.task.VerbosityLevel;

public class AlterDatabaseJava {

  /**
   * @param args
   */
  public static void main(String[] args) {

    final AlterDatabaseDataAll ada = new AlterDatabaseDataAll();
    ada.setDriver(args[0]);
    ada.setUrl(args[1]);
    ada.setUser(args[2]);
    ada.setPassword(args[3]);
    ada.setExcludeobjects(args[4]);
    ada.setModel(new File(args[5]));
    ada.setFilter(args[6]);
    ada.setInput(new File(args[7]));
    ada.setObject(args[8]);
    ada.setFailonerror(new Boolean(args[9]).booleanValue());
    ada.setVerbosity(new VerbosityLevel(args[10]));
    ada.setBasedir(args[11]);
    ada.setDirFilter(args[12]);
    ada.setDatadir(args[13]);
    ada.setDatafilter(args[14]);
    ada.setUserId(args[15]);
    ada.setPropertiesFile(args[16]);
    String force = args[17];
    if (force.equalsIgnoreCase("yes"))
      force = "true";
    ada.setForce(new Boolean(force).booleanValue());
    ada.execute();

  }
}
