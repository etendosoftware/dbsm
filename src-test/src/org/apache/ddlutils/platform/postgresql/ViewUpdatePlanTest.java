/*
 ************************************************************************************
 * Copyright (C) 2026 Etendo Software
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 ************************************************************************************
 */
package org.apache.ddlutils.platform.postgresql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.AddViewChange;
import org.apache.ddlutils.alteration.RemoveViewChange;
import org.apache.ddlutils.alteration.RemoveFunctionChange;
import org.apache.ddlutils.alteration.RemoveTableChange;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.MaterializedView;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.SqlBuilder;
import org.junit.Test;

/** Unit tests for the selection and ordering of the views recreated during an update. */
public class ViewUpdatePlanTest {
  private static final String SELECT_1_AS_ID = "SELECT 1 AS id";
  private static final String SELECT_2_AS_ID = "SELECT 2 AS id";
  private static final String SELECT_ID_FROM_BASE = "SELECT id FROM base";
  private static final String CHILD = "CHILD";
  private static final String C_INVOICE_V = "C_INVOICE_V";
  private static final String C_INVOICELINE_V = "C_INVOICELINE_V";
  private static final String UNRELATED = "UNRELATED";
  private static final String FIN_PAYMENT_DETAIL_V = "FIN_PAYMENT_DETAIL_V";
  private static final String FIN_PAYMENT_SCHED_INV_V = "FIN_PAYMENT_SCHED_INV_V";
  private static final String FIN_PAYMENT_PROP_DETAIL_V = "FIN_PAYMENT_PROP_DETAIL_V";
  private static final String MATERIALIZED = "MATERIALIZED";

  /** Views that did not change generate no drop or create SQL. */
  @Test
  public void unchangedViewsProduceNoSql() throws Exception {
    Database current = model("BASE", SELECT_1_AS_ID, CHILD, SELECT_ID_FROM_BASE);
    SqlBuilder builder = new PostgreSql11Platform().getSqlBuilder();
    StringWriter sql = new StringWriter();
    builder.setWriter(sql);
    Database desired = (Database) current.clone();
    builder.prepareDatabaseForAlter(current, desired, null);
    builder.alterDatabasePostScript(current, desired, null, Collections.<ModelChange>emptyList(),
        current, null);
    assertEquals("", sql.toString());
  }

  /** Formatting, case, comments and single-source qualifiers are not treated as changes. */
  @Test
  public void whitespaceCaseAndSingleSourceQualifiersAreNotChanges() {
    Database current = model("BASE", "SELECT id, sum(amount) AS amount FROM payments p GROUP BY id");
    Database desired = model("BASE",
        "select p.id, sum (p.amount) AS amount FROM payments p GROUP BY p.id; -- comment");
    assertTrue(plan(current, desired).getDrops().isEmpty());
  }

  /** Qualifiers over a derived table do not alter the normalized definition. */
  @Test
  public void derivedTableQualifiersAreNotChanges() {
    assertEquals(normalized("SELECT id FROM (SELECT 1 AS id) p"),
        normalized("SELECT p.id FROM (SELECT 1 AS id) p"));
  }

  /** Changes inside literals and quoted identifiers are detected. */
  @Test
  public void literalAndQuotedIdentifierChangesAreDetected() {
    assertFalse(normalized("SELECT 'A B; FROM child' AS value")
        .equals(normalized("SELECT 'A  B; FROM child' AS value")));
    assertFalse(normalized("SELECT \"AccountType\" FROM accounts")
        .equals(normalized("SELECT \"accounttype\" FROM accounts")));
  }

  /** Standardization keeps literals and quoted identifiers intact. */
  @Test
  public void standardizationPreservesLiteralAndIdentifierContents() {
    assertEquals(normalized("SELECT 'a;b::text' AS \"a;label\""),
        normalized(ViewDefinition.standardize("SELECT 'a;b::text'::text AS \"a;label\";")));
    assertEquals(normalized("SELECT (-1) AS id"),
        normalized(ViewDefinition.standardize("SELECT '-1'::integer::numeric AS id;")));
    assertEquals(normalized("SELECT TRUNC(p.created, 'MM') FROM payments p"),
        normalized(ViewDefinition.standardize("SELECT date_trunc('month', created) FROM payments p;")));
  }

  /** Qualifiers of correlated subquery references are kept. */
  @Test
  public void correlatedReferencesArePreserved() {
    String prefix = "SELECT id, (SELECT max(c.id) FROM child c WHERE c.id = ";
    assertEquals(normalized("SELECT p.id, (SELECT max(c.id) FROM child c WHERE c.id = p.id) FROM parent p"),
        normalized(prefix + "p.id) FROM parent p"));
    assertFalse(normalized(prefix + "p.id) FROM parent p")
        .equals(normalized(prefix + "id) FROM parent p")));
  }

  /** A changed invoice view drops its dependent first and recreates it last. */
  @Test
  public void invoiceDependentsDropFirstAndCreateLast() {
    Database current = model(C_INVOICE_V, SELECT_1_AS_ID, C_INVOICELINE_V,
        "SELECT id FROM c_invoice_v", UNRELATED, "SELECT 0 AS id");
    Database desired = model(C_INVOICE_V, SELECT_2_AS_ID, C_INVOICELINE_V,
        "SELECT id FROM c_invoice_v", UNRELATED, "SELECT 0 AS id");
    ViewUpdatePlan plan = plan(current, desired);
    assertEquals(Arrays.asList(C_INVOICELINE_V, C_INVOICE_V), names(plan.getDrops()));
    assertEquals(Arrays.asList(C_INVOICE_V, C_INVOICELINE_V), names(plan.getCreates()));
  }

  /** A changed payment view recreates its transitive dependents in order. */
  @Test
  public void paymentDependentsIncludeTheEntireChain() {
    Database current = payments(SELECT_1_AS_ID);
    ViewUpdatePlan plan = plan(current, payments(SELECT_2_AS_ID));
    assertEquals(Arrays.asList(FIN_PAYMENT_PROP_DETAIL_V, FIN_PAYMENT_SCHED_INV_V,
        FIN_PAYMENT_DETAIL_V), names(plan.getDrops()));
    assertEquals(Arrays.asList(FIN_PAYMENT_DETAIL_V, FIN_PAYMENT_SCHED_INV_V,
        FIN_PAYMENT_PROP_DETAIL_V), names(plan.getCreates()));
  }

  /** Adding a view does not recreate the views it depends on. */
  @Test
  public void newViewsDoNotRecreateExistingDependencies() {
    ViewUpdatePlan plan = plan(model("BASE", SELECT_1_AS_ID),
        model("BASE", SELECT_1_AS_ID, CHILD, SELECT_ID_FROM_BASE));
    assertTrue(plan.getDrops().isEmpty());
    assertEquals(Collections.singletonList(CHILD), names(plan.getCreates()));
  }

  /** Removed views are dropped dependents first. */
  @Test
  public void removedDependentsAreDroppedInReverseOrder() {
    ViewUpdatePlan plan = plan(model("BASE", SELECT_1_AS_ID, CHILD, SELECT_ID_FROM_BASE),
        new Database());
    assertEquals(Arrays.asList(CHILD, "BASE"), names(plan.getDrops()));
    assertTrue(plan.getCreates().isEmpty());
  }

  /** A table change recreates the views that read from that table and their dependents. */
  @Test
  public void tableChangesStillRecreateViews() throws Exception {
    Database current = model("BASE", "SELECT id FROM test", CHILD, SELECT_ID_FROM_BASE,
        UNRELATED, SELECT_1_AS_ID);
    Table removed = new Table();
    removed.setName("TEST");
    ViewUpdatePlan plan = new ViewUpdatePlan(current, (Database) current.clone(),
        Collections.<ModelChange>singletonList(new RemoveTableChange(removed)), false);
    assertEquals(Arrays.asList(CHILD, "BASE"), names(plan.getDrops()));
    assertEquals(Arrays.asList("BASE", CHILD), names(plan.getCreates()));
  }

  /** A function change unrelated to any view keeps all views. */
  @Test
  public void unrelatedFunctionChangesKeepViews() throws Exception {
    Database current = model("BASE", SELECT_1_AS_ID, CHILD, SELECT_ID_FROM_BASE);
    Function function = new Function();
    function.setName("UNRELATED_HELPER");
    ViewUpdatePlan plan = new ViewUpdatePlan(current, (Database) current.clone(),
        Collections.<ModelChange>singletonList(new RemoveFunctionChange(function)), false);
    assertTrue(plan.getDrops().isEmpty());
    assertTrue(plan.getCreates().isEmpty());
  }

  /** A function change recreates the views using it and their dependents. */
  @Test
  public void functionChangesIncludeTheirViewDependents() throws Exception {
    Database current = model("BASE", "SELECT helper(1) AS id", CHILD, SELECT_ID_FROM_BASE,
        UNRELATED, SELECT_1_AS_ID);
    Function function = new Function();
    function.setName("HELPER");
    ViewUpdatePlan plan = new ViewUpdatePlan(current, (Database) current.clone(),
        Collections.<ModelChange>singletonList(new RemoveFunctionChange(function)), false);
    assertEquals(Arrays.asList(CHILD, "BASE"), names(plan.getDrops()));
    assertEquals(Arrays.asList("BASE", CHILD), names(plan.getCreates()));
  }

  /** Forcing a table recreation without table changes keeps the views. */
  @Test
  public void forcedRecreationWithoutTableChangesKeepsViews() throws Exception {
    Database current = model("BASE", "SELECT id FROM test", CHILD, SELECT_ID_FROM_BASE);
    Table table = new Table();
    table.setName("TEST");
    current.addTable(table);
    SqlBuilder builder = new PostgreSql11Platform().getSqlBuilder();
    builder.setForcedRecreation("TEST");
    StringWriter sql = new StringWriter();
    builder.setWriter(sql);
    builder.prepareDatabaseForAlter(current, (Database) current.clone(), null);
    assertEquals("", sql.toString());
  }

  /** Explicit add/remove view changes are applied to the model without duplicate SQL. */
  @Test
  public void explicitViewChangesAreAppliedWithoutDuplicateSql() throws Exception {
    Database current = model("BASE", SELECT_1_AS_ID, CHILD, SELECT_ID_FROM_BASE);
    Database desired = model("BASE", SELECT_2_AS_ID, CHILD, SELECT_ID_FROM_BASE);
    List<ModelChange> changes = new ArrayList<>(Arrays.<ModelChange>asList(
        new RemoveViewChange(current.findView("BASE")), new AddViewChange(desired.findView("BASE"))));
    SqlBuilder builder = new PostgreSql11Platform().getSqlBuilder();
    builder.setWriter(new StringWriter());
    builder.prepareDatabaseForAlter(current, desired, null, changes);
    StringWriter sql = new StringWriter();
    builder.setWriter(sql);
    builder.alterDatabase(current, desired, null, changes);
    assertEquals("", sql.toString());
    assertEquals(SELECT_2_AS_ID, current.findView("BASE").getStatement());
    builder.alterDatabasePostScript(current, desired, null, changes, current, null);
    assertTrue(sql.toString().indexOf("CREATE OR REPLACE VIEW BASE")
        < sql.toString().indexOf("CREATE OR REPLACE VIEW CHILD"));
  }

  /** Materialized views are ordered together with regular views. */
  @Test
  public void materializedViewsShareTheDependencyOrder() throws Exception {
    Database current = model("BASE", SELECT_1_AS_ID, CHILD, "SELECT id FROM materialized");
    MaterializedView materialized = new MaterializedView(MATERIALIZED);
    materialized.setStatement(SELECT_ID_FROM_BASE);
    current.addMaterializedView(materialized);
    Database desired = (Database) current.clone();
    desired.findView("BASE").setStatement(SELECT_2_AS_ID);
    ViewUpdatePlan plan = plan(current, desired);
    assertEquals(Arrays.asList(CHILD, MATERIALIZED, "BASE"), names(plan.getDrops()));
    assertEquals(Arrays.asList("BASE", MATERIALIZED, CHILD), names(plan.getCreates()));
  }

  /** CTE names are not reported as relations the view depends on. */
  @Test
  public void cteAliasesDoNotCreateFalseDependencies() {
    ViewDefinition query = definition("WITH child AS (SELECT id FROM base) SELECT id FROM child");
    assertEquals(Collections.singleton("BASE"), query.getRelations());
    assertEquals(Collections.singleton(CHILD),
        definition("WITH child AS (SELECT id FROM child) SELECT id FROM child").getRelations());
    assertTrue(definition("WITH RECURSIVE child AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM child WHERE id < 3) SELECT * FROM child")
        .getRelations().isEmpty());
  }

  /** Relations are found in joins, comma lists and nested queries, ignoring literals. */
  @Test
  public void relationsAreFoundInJoinsCommaListsAndNestedQueries() {
    assertEquals(new java.util.HashSet<>(Arrays.asList("BASE", "SECOND", "THIRD", "FOURTH")),
        definition("SELECT 'FROM fake', $$JOIN fake$$, (SELECT count(*) FROM fourth) "
            + "FROM public.\"base\" b LEFT JOIN second s ON s.id=b.id, third t").getRelations());
    assertEquals(Collections.singleton("BASE"),
        definition("SELECT extract(year FROM created) FROM base").getRelations());
    assertEquals(Collections.singleton("BASE"),
        definition("SELECT 'backslash\\', E'escaped\\\' quote' FROM base").getRelations());
  }

  /** Cyclic view dependencies are rejected before any drop. */
  @Test(expected = DatabaseOperationException.class)
  public void cyclesAreRejectedBeforeDroppingAnything() {
    plan(new Database(), model("A", "SELECT * FROM b", "B", "SELECT * FROM a"));
  }

  private static ViewDefinition definition(String sql) {
    return new ViewDefinition(sql, false);
  }

  private static List<String> normalized(String sql) {
    return definition(sql).normalizedTokens();
  }

  private static ViewUpdatePlan plan(Database current, Database desired) {
    return new ViewUpdatePlan(current, desired, Collections.<ModelChange>emptyList(), false);
  }

  private static Database payments(String base) {
    return model(FIN_PAYMENT_DETAIL_V, base, FIN_PAYMENT_SCHED_INV_V,
        "SELECT id FROM fin_payment_detail_v", "FIN_PAYMENT_SCHED_ORD_V", SELECT_1_AS_ID,
        FIN_PAYMENT_PROP_DETAIL_V,
        "SELECT i.id FROM fin_payment_sched_inv_v i JOIN fin_payment_sched_ord_v o ON i.id=o.id");
  }

  static Database model(String... definitions) {
    Database model = new Database();
    model.setName("VIEW_TEST");
    for (int i = 0; i < definitions.length; i += 2) {
      View view = new View(definitions[i]);
      view.setStatement(definitions[i + 1]);
      model.addView(view);
    }
    return model;
  }

  private static List<String> names(List<StructureObject> objects) {
    List<String> names = new ArrayList<>();
    for (StructureObject object : objects) {
      names.add(object.getName());
    }
    return names;
  }
}
