package org.openbravo.ddlutils.task;

import java.io.File;

public class AlterDatabaseModJava {

  /**
   * @param args
   */
  public static void main(String[] args) {

    final AlterDatabaseDataMod ada = new AlterDatabaseDataMod();
    ada.setDriver(args[0]);
    ada.setUrl(args[1]);
    ada.setUser(args[2]);
    ada.setPassword(args[3]);
    ada.setExcludeobjects(args[4]);
    ada.setModel(new File(args[5]));
    // args[6] was 'filter' now unused
    ada.setInput(new File(args[7]));
    ada.setObject(args[8]);
    ada.setFailonerror(new Boolean(args[9]).booleanValue());
    ada.setBasedir(args[10]);
    ada.setBaseConfig(args[11]);
    ada.setDirFilter(args[12]);
    ada.setDatadir(args[13]);
    ada.setDatafilter(args[14]);
    ada.setModule(args[15]);
    String force = args[16];
    if (force.equalsIgnoreCase("yes"))
      force = "true";
    ada.setForce(new Boolean(force).booleanValue());
    String strict = args[17];
    if (strict.equalsIgnoreCase("yes"))
      strict = "true";
    ada.setStrict(new Boolean(strict).booleanValue());
    ada.execute();

  }
}
