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

package org.apache.ddlutils.platform;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.ddlutils.model.Database;

/**
 * 
 * @author adrian
 */
public interface ModelLoader {

    public Database getDatabase(Connection connection, ExcludeFilter filter)
            throws SQLException;

    public Database getDatabase(Connection connection, ExcludeFilter filter,
            String prefix, boolean loadCompleteTables, String moduleId)
            throws SQLException;

    public Database getDatabase(Connection connection, ExcludeFilter filter,
            String datasetName) throws SQLException;

    public void setLog(Log log);

    public Log getLog();
}
