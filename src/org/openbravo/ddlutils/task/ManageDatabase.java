/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.task;

import org.apache.tools.ant.BuildException;

/**
 * 
 * @author adrian
 */
public class ManageDatabase extends BaseDatabaseTask {

    /** Creates a new instance of CreateDatabase */
    public ManageDatabase() {
    }

    @Override
    public void doExecute() {

        throw new BuildException("ant task not valid.");

        // BasicDataSource ds = new BasicDataSource();
        // ds.setDriverClassName(getDriver());
        // ds.setUrl(getUrl());
        // ds.setUsername(getUser());
        // ds.setPassword(getPassword());
        //        
        // // Check if already managed
        // try {
        // Database db = DatabaseUtils.loadCurrentDatabase(ds);
        // throw new BuildException("Database already managed.");
        // } catch (SQLException ex) {
        // }
        //               
        // try {
        // DatabaseUtils.manageDatabase(ds);
        // } catch (Exception e) {
        // // log(e.getLocalizedMessage());
        // throw new BuildException(e);
        // }
    }

}
