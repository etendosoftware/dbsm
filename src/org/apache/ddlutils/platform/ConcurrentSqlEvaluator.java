/*
 ************************************************************************************
 * Copyright (C) 2016 Openbravo S.L.U.
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
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.util.JdbcSupport;

/**
 * Wrapper for SQLBatchEvaluator that allows concurrent executions. It allows to execute a single
 * sql statement handling DB connection acquisition and return from pool.
 * 
 * @author alostale
 *
 */
public class ConcurrentSqlEvaluator implements Callable<Integer> {
  private final static Log log = LogFactory.getLog(ConcurrentSqlEvaluator.class);

  private SQLBatchEvaluator evaluator;
  private String sql;
  private JdbcSupport dbConPool;
  private boolean continueOnError;

  /**
   * 
   * @param evaluator
   *          SQLBatchEvaluator to process the sql
   * @param sql
   *          A single sql statement to execute
   * @param dbConPool
   *          DB connection pool to borrow the connection from
   * @param continueOnError
   *          should an exception be thrown if sql fails
   */
  public ConcurrentSqlEvaluator(SQLBatchEvaluator evaluator, String sql, JdbcSupport dbConPool,
      boolean continueOnError) {
    this.evaluator = evaluator;
    this.sql = sql;
    this.dbConPool = dbConPool;
    this.continueOnError = continueOnError;
  }

  /**
   * Executes the sql borrowing the connection from pool and returning it afterwards.
   * 
   * @return number of errors the sql execution caused
   */
  @Override
  public Integer call() {
    Connection con = null;
    try {
      con = dbConPool.borrowConnection();
      log.debug("[" + Thread.currentThread().getName() + "] - executing " + sql);
      return evaluator.evaluateBatch(con, Arrays.asList(sql), continueOnError, 0);
    } catch (Exception e) {
      log.error("Error while executing " + sql, e);
      return 1;
    } finally {
      dbConPool.returnConnection(con);
    }
  }
}
