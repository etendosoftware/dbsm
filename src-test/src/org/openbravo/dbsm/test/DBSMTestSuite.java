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

package org.openbravo.dbsm.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.openbravo.dbsm.test.configscript.ConfigScriptSuite;
import org.openbravo.dbsm.test.model.ModelSuite;
import org.openbravo.dbsm.test.sourcedata.SourceDataSuite;

/**
 * Test suite including all tests to be executed for DBSM
 * 
 * @author alostale
 *
 */
@RunWith(Suite.class)
@SuiteClasses({ ModelSuite.class, ConfigScriptSuite.class, SourceDataSuite.class })
public class DBSMTestSuite {

}
