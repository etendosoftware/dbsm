/*
 ************************************************************************************
 * Copyright (C) 2015-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.model;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.openbravo.dbsm.test.configscript.ConfigScriptSuite;
import org.openbravo.dbsm.test.model.data.CreateDefault;
import org.openbravo.dbsm.test.model.data.DefaultValuesTest;
import org.openbravo.dbsm.test.model.data.OtherDefaults;
import org.openbravo.dbsm.test.model.recreation.AddDropColumn;
import org.openbravo.dbsm.test.model.recreation.AddDropConstraints;
import org.openbravo.dbsm.test.model.recreation.OtherRecreations;
import org.openbravo.dbsm.test.model.recreation.PLCode;
import org.openbravo.dbsm.test.model.recreation.SQLCommands;
import org.openbravo.dbsm.test.model.recreation.TypeChangeSuite;

/**
 * Test suite grouping all cases for database model
 * 
 * @author alostale
 *
 */
@RunWith(Suite.class)
@SuiteClasses({ //
CheckConstraints.class, //
    Pg95SqlStandardization.class, //
    Sequences.class, //
    NumericScaleChanges.class, //
    AddDropColumn.class, //
    DefaultValuesTest.class, //
    CreateDefault.class, //
    FunctionBasedIndexes.class, //
    CheckIndexFunctionInPrescripts.class, //
    OperatorClassIndexes.class, //
    PartialIndexes.class, //
    ContainsSearchIndexes.class, //
    IndexParallelization.class, //
    CheckExcludeFilter.class, //
    CheckPlSqlStandardizationOnModelLoad.class, //
    CheckDisableAndEnableForeignKeysAndConstraints.class, //
    CheckFollowsClauseCanBeDefinedInOracleTriggers.class, //
    Functions.class,//
    Views.class,//

    AddDropConstraints.class,//
    TypeChangeSuite.class, //
    OtherDefaults.class, //
    SQLCommands.class, //
    OtherRecreations.class, //
    PreventConstraintDeletion.class, //
    PreventCascadeRowDeletion.class,//
    PLCode.class,

    ColumnSizeChangesWithDependentViews.class,//
    ConfigScriptSuite.class })
public class ModelSuite {

}
