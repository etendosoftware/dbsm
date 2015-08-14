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

package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.Arrays;
import java.util.Collection;

import org.apache.ddlutils.platform.postgresql.PostgreSQLStandarization;
import org.apache.ddlutils.platform.postgresql.PostgreSqlCheckTranslation;
import org.apache.ddlutils.platform.postgresql.PostgreSqlModelLoader;
import org.apache.ddlutils.translation.CombinedTranslation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test cases covering SQL standardization problems appeared in PostgreSQL 9.5.
 * 
 * See issue #30397
 * 
 * @author alostale
 *
 */
@RunWith(Parameterized.class)
public class Pg95SqlStandardization {

  /** how the contrains sould like once exported to xml file */
  private String expectedTranslation;

  /**
   * code in pg 9.4 obtained from
   * {@code
   * SELECT upper(pg_constraint.conname::text), pg_constraint.consrc 
   *   FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid 
   *  WHERE pg_constraint.contype = 'c' 
   *    and pg_constraint.conname ilike ?}
   * 
   * @see PostgreSqlModelLoader
   */
  private String pg94Code;

  /** code in pg 9.5 */
  private String pg95Code;

  private CombinedTranslation trl;

  private boolean execute;

  public Pg95SqlStandardization(String constraintName, String expectedTranslation, String pg94Code,
      String pg95Code, CombinedTranslation trl, boolean execute) {
    this.expectedTranslation = expectedTranslation;
    this.pg94Code = pg94Code;
    this.pg95Code = pg95Code;
    this.trl = trl;
    this.execute = execute;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays
        .asList(new Object[][] {
            {
                // Originally this was an issue, which has been fixed by reading constraints from DB
                // already beautified so these transformation won't occur anymore -> skipping test
                // case
                "AD_ORGTYPE_ISLEGALENTITY_CHK",
                "(((ISLEGALENTITY = 'Y') AND (ISBUSINESSUNIT = 'N')) OR ((ISLEGALENTITY = 'N') AND (ISBUSINESSUNIT = 'Y'))) OR ((ISLEGALENTITY = 'N') AND (ISBUSINESSUNIT = 'N'))",
                "((((islegalentity = 'Y'::bpchar) AND (isbusinessunit = 'N'::bpchar)) OR ((islegalentity = 'N'::bpchar) AND (isbusinessunit = 'Y'::bpchar))) OR ((islegalentity = 'N'::bpchar) AND (isbusinessunit = 'N'::bpchar)))",
                "(((islegalentity = 'Y'::bpchar) AND (isbusinessunit = 'N'::bpchar)) OR ((islegalentity = 'N'::bpchar) AND (isbusinessunit = 'Y'::bpchar)) OR ((islegalentity = 'N'::bpchar) AND (isbusinessunit = 'N'::bpchar)))",
                new PostgreSqlCheckTranslation(), //
                false //
            },
            {
                // Originally this was an issue, which has been fixed by reading constraints from DB
                // already beautified so these transformation won't occur anymore -> skipping test
                // case
                "C_DEBT_PAYMENT_C_SETTLEMEN_CH1",
                "((C_SETTLEMENT_CANCEL_ID IS NOT NULL) OR (C_SETTLEMENT_GENERATE_ID IS NOT NULL)) OR (ISPAID = 'N')",
                "(((c_settlement_cancel_id IS NOT NULL) OR (c_settlement_generate_id IS NOT NULL)) OR (ispaid = 'N'::bpchar))",
                "((c_settlement_cancel_id IS NOT NULL) OR (c_settlement_generate_id IS NOT NULL) OR (ispaid = 'N'::bpchar))",
                new PostgreSqlCheckTranslation(),//
                false //
            },
            {
                "C_INVOICE_V",
                "SELECT i.c_invoice_id, i.ad_client_id, i.ad_org_id, i.isactive, i.created, i.createdby, i.updated, i.updatedby, i.issotrx, i.documentno, i.docstatus, i.docaction, i.processing, i.processed, i.c_doctype_id, i.c_doctypetarget_id, i.c_order_id, i.description, i.salesrep_id, i.dateinvoiced, i.dateprinted, i.dateacct, i.c_bpartner_id, i.c_bpartner_location_id, i.ad_user_id, i.poreference, i.dateordered, i.c_currency_id, i.paymentrule, i.c_paymentterm_id, i.c_charge_id, i.m_pricelist_id, i.c_campaign_id, i.c_project_id, i.c_activity_id, i.isprinted, i.isdiscountprinted, CASE WHEN substr(d.docbasetype, 3) = 'C' THEN i.chargeamt * (-1) ELSE i.chargeamt END AS chargeamt, CASE WHEN substr(d.docbasetype, 3) = 'C' THEN i.totallines * (-1) ELSE i.totallines END AS totallines, CASE WHEN substr(d.docbasetype, 3) = 'C' THEN i.grandtotal * (-1) ELSE i.grandtotal END AS grandtotal, CASE WHEN substr(d.docbasetype, 3) = 'C' THEN (-1) ELSE 1 END AS multiplier, CASE WHEN substr(d.docbasetype, 2, 1) = 'P' THEN (-1) ELSE 1 END AS multiplierap, d.docbasetype FROM c_invoice i JOIN c_doctype d ON i.c_doctype_id = d.c_doctype_id ",
                "SELECT i.c_invoice_id,\n"
                    + "    i.ad_client_id,\n"
                    + "    i.ad_org_id,\n"
                    + "    i.isactive,\n"
                    + "    i.created,\n"
                    + "    i.createdby,\n"
                    + "    i.updated,\n"
                    + "    i.updatedby,\n"
                    + "    i.issotrx,\n"
                    + "    i.documentno,\n"
                    + "    i.docstatus,\n"
                    + "    i.docaction,\n"
                    + "    i.processing,\n"
                    + "    i.processed,\n"
                    + "    i.c_doctype_id,\n"
                    + "    i.c_doctypetarget_id,\n"
                    + "    i.c_order_id,\n"
                    + "    i.description,\n"
                    + "    i.salesrep_id,\n"
                    + "    i.dateinvoiced,\n"
                    + "    i.dateprinted,\n"
                    + "    i.dateacct,\n"
                    + "    i.c_bpartner_id,\n"
                    + "    i.c_bpartner_location_id,\n"
                    + "    i.ad_user_id,\n"
                    + "    i.poreference,\n"
                    + "    i.dateordered,\n"
                    + "    i.c_currency_id,\n"
                    + "    i.paymentrule,\n"
                    + "    i.c_paymentterm_id,\n"
                    + "    i.c_charge_id,\n"
                    + "    i.m_pricelist_id,\n"
                    + "    i.c_campaign_id,\n"
                    + "    i.c_project_id,\n"
                    + "    i.c_activity_id,\n"
                    + "    i.isprinted,\n"
                    + "    i.isdiscountprinted,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN i.chargeamt * (-1)::numeric\n"
                    + "            ELSE i.chargeamt\n"
                    + "        END AS chargeamt,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN i.totallines * (-1)::numeric\n"
                    + "            ELSE i.totallines\n"
                    + "        END AS totallines,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN i.grandtotal * (-1)::numeric\n"
                    + "            ELSE i.grandtotal\n" + "        END AS grandtotal,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN (-1)\n"
                    + "            ELSE 1\n" + "        END AS multiplier,\n" + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 2, 1) = 'P'::text THEN (-1)\n"
                    + "            ELSE 1\n" + "        END AS multiplierap,\n"
                    + "    d.docbasetype\n" + "   FROM c_invoice i\n"
                    + "     JOIN c_doctype d ON i.c_doctype_id::text = d.c_doctype_id::text;",
                " SELECT i.c_invoice_id,\n"
                    + "    i.ad_client_id,\n"
                    + "    i.ad_org_id,\n"
                    + "    i.isactive,\n"
                    + "    i.created,\n"
                    + "    i.createdby,\n"
                    + "    i.updated,\n"
                    + "    i.updatedby,\n"
                    + "    i.issotrx,\n"
                    + "    i.documentno,\n"
                    + "    i.docstatus,\n"
                    + "    i.docaction,\n"
                    + "    i.processing,\n"
                    + "    i.processed,\n"
                    + "    i.c_doctype_id,\n"
                    + "    i.c_doctypetarget_id,\n"
                    + "    i.c_order_id,\n"
                    + "    i.description,\n"
                    + "    i.salesrep_id,\n"
                    + "    i.dateinvoiced,\n"
                    + "    i.dateprinted,\n"
                    + "    i.dateacct,\n"
                    + "    i.c_bpartner_id,\n"
                    + "    i.c_bpartner_location_id,\n"
                    + "    i.ad_user_id,\n"
                    + "    i.poreference,\n"
                    + "    i.dateordered,\n"
                    + "    i.c_currency_id,\n"
                    + "    i.paymentrule,\n"
                    + "    i.c_paymentterm_id,\n"
                    + "    i.c_charge_id,\n"
                    + "    i.m_pricelist_id,\n"
                    + "    i.c_campaign_id,\n"
                    + "    i.c_project_id,\n"
                    + "    i.c_activity_id,\n"
                    + "    i.isprinted,\n"
                    + "    i.isdiscountprinted,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN i.chargeamt * '-1'::integer::numeric\n"
                    + "            ELSE i.chargeamt\n"
                    + "        END AS chargeamt,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN i.totallines * '-1'::integer::numeric\n"
                    + "            ELSE i.totallines\n"
                    + "        END AS totallines,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN i.grandtotal * '-1'::integer::numeric\n"
                    + "            ELSE i.grandtotal\n"
                    + "        END AS grandtotal,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 3) = 'C'::text THEN '-1'::integer\n"
                    + "            ELSE 1\n"
                    + "        END AS multiplier,\n"
                    + "        CASE\n"
                    + "            WHEN substr(d.docbasetype::text, 2, 1) = 'P'::text THEN '-1'::integer\n"
                    + "            ELSE 1\n" + "        END AS multiplierap,\n"
                    + "    d.docbasetype\n" + "   FROM c_invoice i\n"
                    + "     JOIN c_doctype d ON i.c_doctype_id::text = d.c_doctype_id::text;",
                new PostgreSQLStandarization(), //
                true //
            },
            {
                "TEST_V",
                "SELECT ps.duedate, trunc(now()) - trunc(ps.duedate) AS daysoverdue1, ps.created count(*) AS count FROM fin_payment_schedule ps GROUP BY ps.duedate, trunc(now()) - trunc(ps.duedate), ps.created ",
                " SELECT ps.duedate, trunc(now()) - trunc(ps.duedate) AS daysoverdue1, ps.created\n"
                    + "    count(*) AS count\n" //
                    + "   FROM fin_payment_schedule ps\n"
                    + "  GROUP BY ps.duedate, trunc(now()) - trunc(ps.duedate), ps.created;",
                " SELECT ps.duedate, trunc(now()) - trunc(ps.duedate) AS daysoverdue1, ps.created\n"
                    + "    count(*) AS count\n" //
                    + "   FROM fin_payment_schedule ps\n"
                    + "  GROUP BY ps.duedate, (trunc(now()) - trunc(ps.duedate)), ps.created;",
                new PostgreSQLStandarization(), //
                true //
            } //
        });
  }

  @Test
  public void sqlShouldBeTranslatedToSameString() throws NoSuchFieldException,
      IllegalAccessException {
    assumeThat("Should be executed", execute, is(true));
    System.out.println("----- 9.4 -----");
    assertThat("pg 9.4", CheckConstraints.translate(pg94Code, trl), equalTo(expectedTranslation));
    System.out.println("\n----- 9.5 -----");
    assertThat("pg 9.5", CheckConstraints.translate(pg95Code, trl), equalTo(expectedTranslation));
  }
}
