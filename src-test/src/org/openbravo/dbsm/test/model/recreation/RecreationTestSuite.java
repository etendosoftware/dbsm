/*
 ************************************************************************************
 * Copyright (C) 2015 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.model.recreation;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.openbravo.dbsm.test.model.data.CreateDefault;
import org.openbravo.dbsm.test.model.data.OtherDefaults;

@RunWith(Suite.class)
@SuiteClasses({ //
AddDropColumn.class, //
    AddDropConstraints.class,//
    DataTypeChanges.class, //
    CreateDefault.class, //
    OtherDefaults.class, //
    SQLCommands.class, //
    OtherRecreations.class })
public class RecreationTestSuite {

}
