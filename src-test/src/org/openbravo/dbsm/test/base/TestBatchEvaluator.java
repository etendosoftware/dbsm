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

package org.openbravo.dbsm.test.base;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.platform.SQLBatchEvaluator;

/**
 * Batch evaluator for test cases. It does not execute any SQL statement, but it stores them in a
 * list which can later be retrieved with getSQLStatements, in this way asserts on what is executed
 * at DB level can be performed.
 * 
 * @author alostale
 *
 */
public class TestBatchEvaluator implements SQLBatchEvaluator {
  List<String> statements = new ArrayList<String>();

  @Override
  public void setPlatform(Platform p) {
    // platform not needed
  }

  @Override
  public int evaluateBatch(Connection connection, List<String> sql, boolean continueOnError,
      long firstSqlCommandIndex) throws DatabaseOperationException {
    statements.addAll(sql);
    return 0;
  }

  @Override
  public int evaluateBatchRealBatch(Connection connection, List<String> sql,
      boolean continueOnError) throws DatabaseOperationException {
    return evaluateBatch(connection, sql, continueOnError, 0);
  }

  public List<String> getSQLStatements() {
    return statements;
  }

  @Override
  public void setLogInfoSucessCommands(boolean logInfoSucessCommands) {

  }

  @Override
  public boolean isLogInfoSucessCommands() {
    return false;
  }

  @Override
  public boolean isDBEvaluator() {
    return false;
  }
}
