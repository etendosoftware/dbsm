/*
 ************************************************************************************
 * Copyright (C) 2018 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.apache.ddlutils.platform.postgresql;

import java.sql.SQLException;

/**
 * PostgreSQL 16 replaces pg_proc.proisagg boolean column with prokind which is a flag for different
 * types of procedures
 */
public class PostgreSql16ModelLoader extends PostgreSql11ModelLoader {

}
