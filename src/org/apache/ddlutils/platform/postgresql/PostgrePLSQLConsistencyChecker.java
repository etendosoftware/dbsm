/*
 ************************************************************************************
 * Copyright (C) 2016-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.postgresql;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.platform.PlatformImplBase;
import org.apache.ddlutils.translation.CommentFilter;
import org.apache.ddlutils.translation.LiteralFilter;

/**
 * Checks consistency of exported PL code in PostgreSQL. It compares the body of a PL object
 * (Function or Trigger) to ensure after importing it will remain as it was exported.
 * 
 * It allows parallel execution.
 * 
 * @see PostgreSqlPlatform#checkTranslationConsistency
 * 
 * @author alostale
 *
 */
class PostgrePLSQLConsistencyChecker implements Callable<StructureObject> {
  private static final Log log = LogFactory.getLog(PostgrePLSQLConsistencyChecker.class);
  private Database fullDatabase;
  private StructureObject plObject;

  PostgrePLSQLConsistencyChecker(Database fullDatabase, StructureObject plObject) {
    this.fullDatabase = fullDatabase;
    this.plObject = plObject;
  }

  /** Checks PL consistency returning {@code null} if it is consistent or the object if it is not */
  @Override
  public StructureObject call() throws Exception {
    PostgrePLSQLTranslation translator;
    String originalBody;
    String body;
    if (plObject instanceof Function) {
      originalBody = ((Function) plObject).getOriginalBody();
      body = ((Function) plObject).getBody();
      translator = new PostgrePLSQLFunctionTranslation(fullDatabase);
    } else if (plObject instanceof Trigger) {
      originalBody = ((Trigger) plObject).getOriginalBody();
      body = ((Trigger) plObject).getBody();
      translator = new PostgrePLSQLTriggerTranslation(fullDatabase);
    } else {
      throw new IllegalArgumentException(
          "Expected a Function or a Trigger got a " + plObject.getClass().getName());
    }

    if (originalBody == null) {
      return null;
    }
    LiteralFilter litFilter1 = new LiteralFilter();
    CommentFilter comFilter1 = new CommentFilter();
    String s1 = litFilter1.removeLiterals(body);
    s1 = comFilter1.removeComments(s1);
    s1 = translator.exec(s1);
    s1 = comFilter1.restoreComments(s1);
    s1 = litFilter1.restoreLiterals(s1);
    String s1r = s1.replaceAll("\\s", "");
    String s2 = originalBody;
    String s2r = s2.replaceAll("\\s", "");
    if (!s1r.equals(s2r)) {
      log.warn("Found differences in " + plObject.getName());
      PlatformImplBase.printDiff(s1, s2);
      return plObject;
    }
    return null;
  }
}
