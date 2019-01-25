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

package org.apache.ddlutils.platform;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.util.JdbcSupport;

/**
 * Default batch evaluator. Receives SQL batches and executes them in DB
 * 
 * @author alostale
 *
 */
public class StandardBatchEvaluator extends JdbcSupport implements SQLBatchEvaluator {
  private static final int MAX_LOOPS_OF_FORCED = 5;
  protected final Log _log = LogFactory.getLog(getClass());
  private Platform platform;
  private boolean logInfoSucessCommands = true;

  public StandardBatchEvaluator(Platform platform) {
    this.platform = platform;
  }

  @Override
  public void setPlatform(Platform p) {
    this.platform = p;
  }

  @Override
  public int evaluateBatch(Connection connection, List<String> sql, boolean continueOnError,
      long firstSqlCommandIndex) throws DatabaseOperationException {
    Statement statement = null;
    int errors = 0;
    int commandCount = 0;
    long t = System.currentTimeMillis();
    boolean _ignoreWarns = platform.areWarnsIgnored();

    ArrayList<String> aForcedCommands = new ArrayList<String>();
    ArrayList<String> iteratedCommands = new ArrayList<String>();
    // we tokenize the SQL along the delimiters, and we also make sure that
    // only delimiters
    // at the end of a line or the end of the string are used (row mode)
    try {
      statement = connection.createStatement();

      List<String> sqlToRetry = sql.subList((int) firstSqlCommandIndex, sql.size());
      for (String command : sqlToRetry) {
        // ignore whitespace
        command = command.trim();
        if (command.length() == 0) {
          continue;
        }

        commandCount++;

        if (_log.isDebugEnabled()) {
          _log.debug("About to execute SQL " + command);
        }
        if (command.contains("ITERATE = TRUE")) {
          iteratedCommands.add(command);
          continue;
        }
        try {
          // int results = statement.executeUpdate(command);
          boolean result = statement.execute(command);
          // int results;
          // if (statement.execute(command)) {
          // results = 0; // is a result set
          // } else {
          // results = statement.getUpdateCount();
          // }

          /*
           * if (_log.isDebugEnabled()) { _log.debug("After execution, " + results +
           * " row(s) have been changed"); }
           */
        } catch (SQLException ex) {
          if (command.contains("SCRIPT OPTIONS (CRITICAL = TRUE)")) {
            throw new DatabaseOperationException(
                "Error while executing a critical SQL to recreate a database table: " + command
                    + ".\nYou should recover a backup of the database if possible. If it's not, take into account that there is an auxiliary table which still contains the original data, which can be recovered from it. If a update.database or smartbuild is done, this auxiliary table will be deleted, and all its data will be lost forever. For more information, visit the page: http://wiki.openbravo.com/wiki/Update_Tips",
                ex);
          }

          if (continueOnError) {
            // Since the user deciced to ignore this error, we log
            // the error
            // on level warn, and the exception itself on level
            // debug
            if (!command.contains("SCRIPT OPTIONS (FORCE = TRUE)")) {
              _log.warn("SQL Command failed with: " + ex.getMessage());
              _log.warn(command);
              if (_log.isDebugEnabled()) {
                _log.debug(ex);
              }
              errors++;
            }

            // It is a "forced" command ?
            if (command.contains("SCRIPT OPTIONS (FORCE = TRUE)")) {
              aForcedCommands.add(command);
            }
          } else {
            throw new DatabaseOperationException("Error while executing SQL " + command, ex);
          }
        }

        // lets display any warnings
        SQLWarning warning = connection.getWarnings();

        while (warning != null) {
          _log.warn(warning.toString());
          warning = warning.getNextWarning();
        }
        connection.clearWarnings();
      }
      boolean changedSomething = false;
      do {
        changedSomething = false;
        for (String command : iteratedCommands) {
          int changedRecords = statement.executeUpdate(command);
          if (changedRecords != 0) {
            _log.debug("changed: " + changedRecords + " executed: " + command);
            changedSomething = true;
          }
        }
      } while (changedSomething);

      logCommands(errors, commandCount, t);
      t = System.currentTimeMillis();

      // execute the forced commands
      int loops = 0;
      HashMap<String, String> errorMap = new HashMap<String, String>();
      int previousErrors = errors;
      while (loops < MAX_LOOPS_OF_FORCED && !aForcedCommands.isEmpty()) {

        loops++;
        commandCount = 0;
        errors = previousErrors;

        for (Iterator<String> it = aForcedCommands.iterator(); it.hasNext();) {

          String command = it.next();
          commandCount++;

          if (_log.isDebugEnabled()) {
            _log.debug("About to execute SQL " + command);
          }
          try {
            int results = statement.executeUpdate(command);
            if (_log.isDebugEnabled()) {
              _log.debug("After execution, " + results + " row(s) have been changed");
            }
            it.remove();
            if (errorMap.containsKey(command)) {
              errorMap.remove(command);
            }
          } catch (SQLException ex) {
            String error = "SQL Command failed with: " + ex.getMessage();
            errorMap.put(command, error);
            if (_log.isDebugEnabled()) {
              _log.debug(ex);
            }
            errors++;
          }
        }
        String errorNumber2 = "";
        if (errors > 0) {
          errorNumber2 = " with " + errors + " error(s)";
        } else {
          errorNumber2 = " successfully";
        }
        errorNumber2 += " in " + (System.currentTimeMillis() - t + " ms");
        if (commandCount > 0) {
          _log.info("Executed " + commandCount + " forced SQL command(s)" + errorNumber2);
        }
      }
      Iterator it = errorMap.keySet().iterator();
      while (it.hasNext()) {
        String error = errorMap.get(it.next());
        if (!_ignoreWarns) {
          _log.warn(error);
        } else {
          _log.info(error);
        }
      }
      if (!aForcedCommands.isEmpty()) {
        String error = "There are still " + aForcedCommands.size()
            + " forced commands not executed sucessfully (likely related to failed view statements).";
        if (_ignoreWarns) {
          _log.info(error);
        } else {
          _log.warn(error);
        }
      }

    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while executing SQL", ex);
    } finally {
      closeStatement(statement);
    }

    return errors;
  }

  private void logCommands(int errors, int commandCount, long startTime) {
    String errorNumber = "";
    if (errors > 0) {
      errorNumber = " with " + errors + " error(s)";
    } else {
      errorNumber = " successfully";
    }

    errorNumber += " in " + (System.currentTimeMillis() - startTime + " ms");
    if (commandCount > 0) {
      if (errors == 0 && !logInfoSucessCommands) {
        _log.debug("Executed " + commandCount + " SQL command(s)" + errorNumber);
      } else {
        _log.info("Executed " + commandCount + " SQL command(s)" + errorNumber);
      }
    }
  }

  @Override
  public int evaluateBatchRealBatch(Connection connection, List<String> sql,
      boolean continueOnError) throws DatabaseOperationException {
    Statement statement = null;
    int errors = 0;
    int commandCount = 0;
    long t = System.currentTimeMillis();

    // we tokenize the SQL along the delimiters, and we also make sure that
    // only delimiters
    // at the end of a line or the end of the string are used (row mode)
    try {

      statement = connection.createStatement();

      for (String command : sql) {

        // ignore whitespace
        command = command.trim();
        if (command.length() == 0) {
          continue;
        }

        commandCount++;

        if (_log.isDebugEnabled()) {
          _log.debug("About to execute SQL " + command);
        }

        try {
          statement.addBatch(command);

        } catch (SQLException ex) {

          if (continueOnError) {
            // Since the user deciced to ignore this error, we log
            // the error
            // on level warn, and the exception itself on level
            // debug
            _log.warn("SQL Command failed with: " + ex.getMessage());
            _log.warn(command);
            if (_log.isDebugEnabled()) {
              _log.debug(ex);
            }
            errors++;

          } else {
            throw new DatabaseOperationException("Error while executing SQL " + command, ex);
          }
        }

        // lets display any warnings
        SQLWarning warning = connection.getWarnings();

        while (warning != null) {
          _log.warn(warning.toString());
          warning = warning.getNextWarning();
        }
        connection.clearWarnings();
      }

      // everything added to batch
      try {
        statement.executeBatch();
      } catch (BatchUpdateException e) {
        errors++;
        long indexFailedStatement = e.getUpdateCounts().length;
        return handleFailedBatchExecution(connection, sql, continueOnError, indexFailedStatement);
      }

    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while executing SQL", ex);
    } finally {
      try {
        connection.setAutoCommit(true);
      } catch (SQLException e) {
        // won't happen
      }

      if (statement != null) {
        closeStatement(statement);
      }
    }

    logCommands(errors, commandCount, t);

    return errors;
  }

  protected int handleFailedBatchExecution(Connection connection, List<String> sql,
      boolean continueOnError, long indexFailedStatement) {
    return evaluateBatch(connection, sql, continueOnError, 0);
  }

  @Override
  public void setLogInfoSucessCommands(boolean logInfoSucessCommands) {
    this.logInfoSucessCommands = logInfoSucessCommands;
  }

  @Override
  public boolean isLogInfoSucessCommands() {
    return logInfoSucessCommands;
  }

  @Override
  public boolean isDBEvaluator() {
    return true;
  }
}
