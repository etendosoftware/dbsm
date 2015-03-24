package org.openbravo.dbsm.test.model.recreation;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.openbravo.dbsm.test.model.data.CreateDefault;
import org.openbravo.dbsm.test.model.data.OtherDefaults;

@RunWith(Suite.class)
@SuiteClasses({ //
AddDropColumn.class, //
    CreateDefault.class, //
    OtherDefaults.class, //
    SQLCommands.class })
public class RecreationTestSuite {

}
