package org.openbravo.dbsm.test.model.recreation;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ //
    ColumnSizeChange.class, //
    ColumnTypeChange.class, //
    CombinedTypeChanges.class })
public class TypeChangeSuite {

}
