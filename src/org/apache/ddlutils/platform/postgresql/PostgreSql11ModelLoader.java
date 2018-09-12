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
 * PostgreSQL 11 replaces pg_proc.proisagg boolean column with prokind which is a flag for different
 * types of procedures
 */
public class PostgreSql11ModelLoader extends PostgreSql10ModelLoader {

  @Override
  protected void initMetadataSentences() throws SQLException {
    super.initMetadataSentences();

    _stmt_functionparams.close();
    _stmt_functiondefaults.close();
    _stmt_functiondefaults0.close();

    _stmt_functionparams = _connection.prepareStatement("  SELECT "
        + "         pg_proc.prorettype," + "         pg_proc.proargtypes,"
        + "         pg_proc.proallargtypes," + "         pg_proc.proargmodes,"
        + "         pg_proc.proargnames," + "         pg_proc.prosrc"
        + "    FROM pg_catalog.pg_proc" + "         JOIN pg_catalog.pg_namespace"
        + "         ON (pg_proc.pronamespace = pg_namespace.oid)"
        + "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype"
        + "     AND (pg_proc.proargtypes[0] IS NULL"
        + "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)"
        + "     AND pg_proc.prokind <> 'a'"
        + "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)"
        + "     AND pg_proc.proname = ?"
        + "         ORDER BY pg_proc.pronargs DESC, length(pg_proc.prosrc) DESC");

    _stmt_functiondefaults = _connection.prepareStatement("  SELECT " + "         pg_proc.proname,"
        + "         pg_proc.proargtypes," + "         pg_proc.proallargtypes,"
        + "         pg_proc.prosrc" + "    FROM pg_catalog.pg_proc"
        + "         JOIN pg_catalog.pg_namespace"
        + "         ON (pg_proc.pronamespace = pg_namespace.oid)"
        + "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype"
        + "     AND (pg_proc.proargtypes[0] IS NULL"
        + "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)"
        + "     AND pg_proc.prokind <> 'a'"
        + "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)"
        + "     AND pg_proc.proname = ? ORDER BY pg_proc.pronargs ASC");

    _stmt_functiondefaults0 = _connection.prepareStatement("  SELECT "
        + "         pg_proc.proname," + "         pg_proc.proargtypes,"
        + "         pg_proc.proallargtypes," + "         pg_proc.prosrc"
        + "    FROM pg_catalog.pg_proc" + "         JOIN pg_catalog.pg_namespace"
        + "         ON (pg_proc.pronamespace = pg_namespace.oid)"
        + "   WHERE pg_proc.prorettype <> 'pg_catalog.cstring'::pg_catalog.regtype"
        + "     AND (pg_proc.proargtypes[0] IS NULL"
        + "      OR pg_proc.proargtypes[0] <> 'pg_catalog.cstring'::pg_catalog.regtype)"
        + "     AND pg_proc.prokind <> 'a'"
        + "     AND pg_catalog.pg_function_is_visible(pg_proc.oid)"
        + "     AND pg_proc.proname = ? ORDER BY pg_proc.proargtypes ASC");
  }

}
