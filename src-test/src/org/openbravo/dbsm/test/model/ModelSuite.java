/*
 ************************************************************************************
 * Copyright (C) 2015-2016 Openbravo S.L.U.
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
import org.openbravo.dbsm.test.model.data.CreateDefault;
import org.openbravo.dbsm.test.model.recreation.AddDropColumn;

import com.google.common.base.Functions;

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
    CreateDefault.class, //
    FunctionBasedIndexes.class, //
    OperatorClassIndexes.class, //
    CheckExcludeFilter.class, //
    CheckPlSqlStandardizationOnModelLoad.class, //
    CheckDisableAndEnableForeignKeysAndConstraints.class, //
    CheckFollowsClauseCanBeDefinedInOracleTriggers.class, //
    Functions.class })
public class ModelSuite {

}
