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

package org.openbravo.dbsm.test.base.matchers;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.openbravo.dbsm.test.base.DbsmTest;
import org.openbravo.dbsm.test.base.DbsmTest.Row;

/**
 * Matchers specific for testing dbsm
 * 
 * @author alostale
 *
 */
public class DBSMMatchers {

  @Factory
  public static Matcher<Row> rowEquals(Row row) {
    return new RowComparator(row);
  }

  private static class RowComparator extends TypeSafeMatcher<DbsmTest.Row> {
    private Row compareToRow;
    private String failureDesc = "";

    @Override
    public void describeTo(Description description) {
      description.appendText("row values should be identical but differences found " + failureDesc);

    }

    public RowComparator(DbsmTest.Row compareToRow) {
      this.compareToRow = compareToRow;
    }

    @Override
    protected boolean matchesSafely(DbsmTest.Row row) {
      for (String colName : row.getColumnNames()) {
        String newValue = row.getValue(colName);
        String origValue = compareToRow.getValue(colName);
        boolean fail = false;
        if (newValue == null && origValue != null) {
          fail = true;
        } else if (newValue == null && origValue == null) {
          continue;
        } else if (!newValue.equals(origValue)) {
          fail = true;
        }
        if (fail) {
          failureDesc += !failureDesc.isEmpty() ? " ," : "";
          failureDesc += "<" + colName + " [" + origValue + " -> " + newValue + "]>";
        }
      }

      return failureDesc.isEmpty();
    }
  }
}
