/* ************************************************************************************
 * Copyright (C) 2015 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;
import java.util.ArrayList;

import org.apache.ddlutils.platform.postgresql.PostgreSqlCheckTranslation;
import org.apache.ddlutils.translation.Translation;
import org.junit.Test;

/**
 * Test cases to validate DB check constraints
 * 
 * @author alostale
 *
 */
public class CheckConstraints {
  private static final String EXPECTED_RESULT = "TYPE IN ('M', 'P', 'T')";

  /**
   * Test case for issue #28684
   * 
   * Checks constraint formatted as in pg9.3 after install sources is properly translated
   */
  @Test
  public void installSourceIsProperlyTranlsated() throws NoSuchFieldException,
      IllegalAccessException {
    String case1 = "((type)::text = ANY ((ARRAY['M'::character varying, 'P'::character varying, 'T'::character varying])::text[]))";
    assertThat(translate(case1), is(equalTo(EXPECTED_RESULT)));
  }

  /**
   * Test case for issue #28684
   * 
   * Checks constraint formatted as in pg9.3 after pg_dump + pg_restore is properly translated
   */
  @Test
  public void dumpedAndRestoredPgDbIsProperlyTranslated() throws NoSuchFieldException,
      IllegalAccessException {
    String case2 = "((type)::text = ANY (ARRAY[('M'::character varying)::text, ('P'::character varying)::text, ('T'::character varying)::text]))";
    assertThat(translate(case2), is(equalTo(EXPECTED_RESULT)));
  }

  private String translate(String checkConstraint) throws NoSuchFieldException,
      IllegalAccessException {
    PostgreSqlCheckTranslation trl = new PostgreSqlCheckTranslation();
    Field f = trl.getClass().getSuperclass().getDeclaredField("_translations");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    ArrayList<Translation> trls = (ArrayList<Translation>) f.get(trl);
    int i = 0;
    String translatedCheck = checkConstraint;
    System.out.println(translatedCheck);
    for (Translation t : trls) {
      translatedCheck = t.exec(translatedCheck);
      System.out.println(++i + " " + translatedCheck);
    }
    return translatedCheck;
  }
}
