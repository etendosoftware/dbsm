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

package org.apache.ddlutils.platform.postgresql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.translation.CommentFilter;
import org.apache.ddlutils.translation.LiteralFilter;

/**
 * Allows to concurrently standardize PL triggers.
 * 
 * @see PostgreSqlModelLoader#standardizePlSql
 * 
 * @author alostale
 */
class PostgrePLSQLTriggerConcurrentStandardization implements Runnable {
  private static final Log log = LogFactory
      .getLog(PostgrePLSQLTriggerConcurrentStandardization.class);

  private Database db;
  private int idx;

  PostgrePLSQLTriggerConcurrentStandardization(Database db, int idx) {
    this.db = db;
    this.idx = idx;
  }

  @Override
  public void run() {
    Trigger trg = db.getTrigger(idx);
    log.debug("Standardizing trigger: " + trg.getName());
    trg.setOriginalBody(trg.getBody());
    PostgrePLSQLTriggerStandarization triggerStandarization = new PostgrePLSQLTriggerStandarization(
        db, idx);
    String body = trg.getBody();

    LiteralFilter litFilter = new LiteralFilter();
    CommentFilter comFilter = new CommentFilter();

    body = litFilter.removeLiterals(body);
    body = comFilter.removeComments(body);

    String standardizedBody = triggerStandarization.exec(body);

    standardizedBody = comFilter.restoreComments(standardizedBody);
    standardizedBody = litFilter.restoreLiterals(standardizedBody);

    while (standardizedBody.charAt(standardizedBody.length() - 1) == '\n'
        || standardizedBody.charAt(standardizedBody.length() - 1) == ' ')
      standardizedBody = standardizedBody.substring(0, standardizedBody.length() - 1);
    trg.setBody(standardizedBody + '\n');
    log.debug("  ...standardized trigger: " + trg.getName());
  }

}
