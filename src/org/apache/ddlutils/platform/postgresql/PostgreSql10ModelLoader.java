/*
 ************************************************************************************
 * Copyright (C) 2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.postgresql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.platform.RowConstructor;

/**
 * Class used by the {@link org.apache.ddlutils.platform.postgresql.PostgreSql10Platform} platform
 * to load the database model. It differs from the loader of previous PostgreSQL versions in the way
 * sequences are loaded.
 */
public class PostgreSql10ModelLoader extends PostgreSqlModelLoader {

  @SuppressWarnings("rawtypes")
  @Override
  protected Collection readSequences() throws SQLException {
    return readList(_stmt_listsequences, new RowConstructor() {
      @Override
      public Object getRow(ResultSet r) throws SQLException {
        String sequenceName = r.getString(1);
        Object oid = r.getObject(2);
        _log.debug("Sequence " + sequenceName);
        final Sequence sequence = new Sequence();
        sequence.setName(sequenceName);

        // Since PG 10 sequence details are stored in catalog (pg_sequence table)
        String sql = "select seqstart, seqincrement from pg_sequence where seqrelid = ?";
        try (PreparedStatement stmtCurrentSequence = _connection.prepareStatement(sql)) {
          stmtCurrentSequence.setObject(1, oid);
          readList(stmtCurrentSequence, new RowConstructor() {
            @Override
            public Object getRow(ResultSet seqDetails) throws SQLException {
              sequence.setStart(seqDetails.getInt(1));
              sequence.setIncrement(seqDetails.getInt(2));
              return sequence;
            }
          });
        }
        return sequence;
      }
    });
  }

}
