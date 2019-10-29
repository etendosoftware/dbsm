/*
 ************************************************************************************
 * Copyright (C) 2016-2019 Openbravo S.L.U.
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
import org.apache.ddlutils.model.View;

/**
 * Allows to concurrently standardize DB views.
 * 
 * @see PostgreSqlModelLoader#standardizePlSql
 * 
 * @author alostale
 */
class PostgrePLSQLViewConcurrentStandardization implements Runnable {
  private static final Log log = LogFactory.getLog(PostgrePLSQLViewConcurrentStandardization.class);

  protected Database db;
  protected int idx;

  PostgrePLSQLViewConcurrentStandardization(Database db, int idx) {
    this.db = db;
    this.idx = idx;
  }

  @Override
  public void run() {
    View view = getView();
    log.debug("Standardizing view: " + view.getName());
    PostgreSQLStandarization viewStandarization = new PostgreSQLStandarization();
    String body = getView().getStatement();

    String standardizedBody = viewStandarization.exec(body);
    if (standardizedBody.endsWith("\n")) {
      standardizedBody = standardizedBody.substring(0, standardizedBody.length() - 1);
    }
    standardizedBody = standardizedBody.trim();
    getView().setStatement(standardizedBody);

    log.debug("  ...standardized view: " + view.getName());
  }

  protected View getView() {
    return db.getView(idx);
  }

}
