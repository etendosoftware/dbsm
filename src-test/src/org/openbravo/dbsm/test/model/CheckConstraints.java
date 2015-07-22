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
import java.util.List;

import org.apache.ddlutils.platform.postgresql.PostgreSqlCheckTranslation;
import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.Translation;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test cases to validate DB check constraints
 * 
 * @author alostale
 *
 */
@Ignore("These tests are not applicable anymore after fix for issue #30397 "
    + "because constraints are read from db already beautified so extra required modifications "
    + "are much more limited.")
public class CheckConstraints {

  private static List<CheckConstraintType> contrains;

  static {
    contrains = new ArrayList<CheckConstraintType>();

    contrains
        .add(new CheckConstraintType(
            "AD_MODULE_TYPE_CHK",
            "TYPE IN ('M', 'P', 'T')",
            "((type)::text = ANY ((ARRAY['M'::character varying, 'P'::character varying, 'T'::character varying])::text[]))",
            "((type)::text = ANY (ARRAY[('M'::character varying)::text, ('P'::character varying)::text, ('T'::character varying)::text]))"));
    contrains
        .add(new CheckConstraintType(
            "S_TIMEEXPENSE_PROCESSED_CHK",
            "PROCESSED IN ('Y', 'N')",
            "((processed)::text = ANY ((ARRAY['Y'::character varying, 'N'::character varying])::text[]))",
            "((processed)::text = ANY (ARRAY[('Y'::character varying)::text, ('N'::character varying)::text]))"));
    contrains
        .add(new CheckConstraintType(
            "MA_WRPHASEPRODUCT_PRODUCTI_CHK",
            "PRODUCTIONTYPE IN ('+', '-')",
            "((productiontype)::text = ANY ((ARRAY['+'::character varying, '-'::character varying])::text[]))",
            "((productiontype)::text = ANY (ARRAY[('+'::character varying)::text, ('-'::character varying)::text]))"));

    // following cases didn't change after export + dump, testing them just because are more complex
    contrains
        .add(new CheckConstraintType(
            "C_INVOICELINE_CHECK3",
            "(((((ISDEFERRED = 'Y') AND (C_PERIOD_ID IS NOT NULL)) AND (DEFPLANTYPE IS NOT NULL)) AND (M_PRODUCT_ID IS NOT NULL)) AND (PERIODNUMBER IS NOT NULL)) OR (ISDEFERRED = 'N')",
            "((((((isdeferred = 'Y'::bpchar) AND (c_period_id IS NOT NULL)) AND (defplantype IS NOT NULL)) AND (m_product_id IS NOT NULL)) AND (periodnumber IS NOT NULL)) OR (isdeferred = 'N'::bpchar))",
            "((((((isdeferred = 'Y'::bpchar) AND (c_period_id IS NOT NULL)) AND (defplantype IS NOT NULL)) AND (m_product_id IS NOT NULL)) AND (periodnumber IS NOT NULL)) OR (isdeferred = 'N'::bpchar))"));
    contrains
        .add(new CheckConstraintType(
            "C_DEBT_PAYMENT_ISVALID_CHK1",
            "(((ISVALID = 'Y') AND (((C_SETTLEMENT_GENERATE_ID IS NULL) AND (GENERATE_PROCESSED = 'N')) OR ((C_SETTLEMENT_GENERATE_ID IS NOT NULL) AND (GENERATE_PROCESSED = 'Y')))) OR (((ISVALID = 'N') AND (C_SETTLEMENT_GENERATE_ID IS NOT NULL)) AND (GENERATE_PROCESSED = 'N'))) OR ((ISVALID = 'N') AND (ISAUTOMATICGENERATED = 'N'))",
            "((((isvalid = 'Y'::bpchar) AND (((c_settlement_generate_id IS NULL) AND (generate_processed = 'N'::bpchar)) OR ((c_settlement_generate_id IS NOT NULL) AND (generate_processed = 'Y'::bpchar)))) OR (((isvalid = 'N'::bpchar) AND (c_settlement_generate_id IS NOT NULL)) AND (generate_processed = 'N'::bpchar))) OR ((isvalid = 'N'::bpchar) AND (isautomaticgenerated = 'N'::bpchar)))",
            "((((isvalid = 'Y'::bpchar) AND (((c_settlement_generate_id IS NULL) AND (generate_processed = 'N'::bpchar)) OR ((c_settlement_generate_id IS NOT NULL) AND (generate_processed = 'Y'::bpchar)))) OR (((isvalid = 'N'::bpchar) AND (c_settlement_generate_id IS NOT NULL)) AND (generate_processed = 'N'::bpchar))) OR ((isvalid = 'N'::bpchar) AND (isautomaticgenerated = 'N'::bpchar)))"));

  }

  /**
   * Test case for issue #28684
   * 
   * Checks constraint formatted as in pg9.3 after install sources is properly translated
   */
  @Test
  public void installSourceIsProperlyTranslated() throws NoSuchFieldException,
      IllegalAccessException {
    for (CheckConstraintType contraint : contrains) {
      assertThat(contraint.contraintName + " is not properly tranlsated",
          translate(contraint.installSource), is(equalTo(contraint.translated)));
    }
  }

  /**
   * Test case for issue #28684
   * 
   * Checks constraint formatted as in pg9.3 after pg_dump + pg_restore is properly translated
   */
  @Test
  public void dumpedAndRestoredPgDbIsProperlyTranslated() throws NoSuchFieldException,
      IllegalAccessException {
    for (CheckConstraintType contraint : contrains) {
      assertThat(contraint.contraintName + " is not properly tranlsated",
          translate(contraint.dumpRestore), is(equalTo(contraint.translated)));
    }
  }

  public static String translate(String checkConstraint) throws NoSuchFieldException,
      IllegalAccessException {
    return translate(checkConstraint, new PostgreSqlCheckTranslation());
  }

  public static String translate(String checkConstraint, CombinedTranslation trl)
      throws NoSuchFieldException, IllegalAccessException {
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

  private static class CheckConstraintType {
    String contraintName;
    String translated;
    String installSource;
    String dumpRestore;

    public CheckConstraintType(String contraintName, String translated, String installSource,
        String dumpRestore) {
      this.contraintName = contraintName;
      this.translated = translated;
      this.installSource = installSource;
      this.dumpRestore = dumpRestore;
    }
  }
}
