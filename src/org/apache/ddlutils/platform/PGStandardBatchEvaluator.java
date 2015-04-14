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

package org.apache.ddlutils.platform;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.Platform;

public class PGStandardBatchEvaluator extends StandardBatchEvaluator {

  public PGStandardBatchEvaluator(Platform platform) {
    super(platform);
  }

  @Override
  protected int handleFailedBatchExecution(Connection connection, List<String> sql,
      boolean continueOnError, long indexFailedStatement) {
    try {
      connection.rollback();
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      _log.error("Error while handling a failed batch execution ins postgreSql.");
    }
    _log.info("Batch statement failed. Rolling back and retrying all the statements in a non-batched connection.");
    // The batch failed. We will execute all commands again using the old method
    return evaluateBatch(connection, sql, continueOnError, 0);
  }

  @Override
  public int evaluateBatchRealBatch(Connection connection, List<String> sql, boolean continueOnError)
      throws DatabaseOperationException {
    try {
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      // will not happen
    }
    return super.evaluateBatchRealBatch(connection, sql, continueOnError);
  }

}
