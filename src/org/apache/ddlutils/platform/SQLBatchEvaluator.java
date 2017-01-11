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

package org.apache.ddlutils.platform;

import java.sql.Connection;
import java.util.List;

import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.Platform;

/**
 * Interface to implement to evaluate sql batches.
 * 
 * @author alostale
 *
 */
public interface SQLBatchEvaluator {
  public void setPlatform(Platform p);

  public int evaluateBatch(Connection connection, List<String> sql, boolean continueOnError,
      long firstSqlCommandIndex) throws DatabaseOperationException;

  public int evaluateBatchRealBatch(Connection connection, List<String> sql, boolean continueOnError)
      throws DatabaseOperationException;

  /** Should successful commands be logged */
  public void setLogInfoSucessCommands(boolean logInfoSucessCommands);

  /** Should successful commands be logged */
  public boolean isLogInfoSucessCommands();
}
