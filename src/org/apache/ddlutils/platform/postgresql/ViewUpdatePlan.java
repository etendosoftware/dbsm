/*
 ************************************************************************************
 * Copyright (C) 2026 Etendo Software
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 ************************************************************************************
 */
package org.apache.ddlutils.platform.postgresql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.alteration.AddFunctionChange;
import org.apache.ddlutils.alteration.AddTableChange;
import org.apache.ddlutils.alteration.AddViewChange;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.RemoveFunctionChange;
import org.apache.ddlutils.alteration.RemoveViewChange;
import org.apache.ddlutils.alteration.TableChange;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.MaterializedView;
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.model.View;

/** Selects views to recreate and orders them without using cascading drops or SQL retries. */
class ViewUpdatePlan {
  private static final Log log = LogFactory.getLog(ViewUpdatePlan.class);
  private final List<StructureObject> drops;
  private final List<StructureObject> creates;

  ViewUpdatePlan(Database current, Database desired, List<ModelChange> changes,
      boolean caseSensitive) {
    Map<String, Entry> before = entries(current, caseSensitive);
    Map<String, Entry> after = entries(desired, caseSensitive);
    ChangeSummary summary = new ChangeSummary(changes, caseSensitive);
    Set<String> affected = affectedViews(before, after, summary);
    // A changed base view also requires its unchanged dependents, including transitive ones.
    boolean expanded;
    do {
      boolean fromCurrent = includeDependents(before, affected);
      boolean fromDesired = includeDependents(after, affected);
      expanded = fromCurrent || fromDesired;
    } while (expanded);
    drops = ordered(before, affected);
    Collections.reverse(drops);
    creates = ordered(after, affected);
    if (log.isDebugEnabled()) {
      log.debug("View update plan: " + drops.size() + " drops, " + creates.size()
          + " creates; model changes: " + summary.categories + "; changed functions: "
          + summary.functions);
    }
  }

  private static Set<String> affectedViews(Map<String, Entry> before, Map<String, Entry> after,
      ChangeSummary summary) {
    Set<String> names = new HashSet<>(before.keySet());
    names.addAll(after.keySet());
    Set<String> affected = new HashSet<>();
    for (String name : names) {
      Entry oldView = before.get(name);
      Entry newView = after.get(name);
      if (summary.recreateAll || oldView == null || newView == null || !oldView.sameAs(newView)
          || oldView.dependsOn(summary.tables, summary.functions)
          || newView.dependsOn(summary.tables, summary.functions)) {
        affected.add(name);
      }
    }
    return affected;
  }

  /** Tables and functions touched by the model changes, which invalidate the views using them. */
  private static class ChangeSummary {
    private final Set<String> tables = new HashSet<>();
    private final Set<String> functions = new HashSet<>();
    private final Map<String, Integer> categories = new TreeMap<>();
    private boolean recreateAll;

    ChangeSummary(List<ModelChange> changes, boolean caseSensitive) {
      for (ModelChange change : changes) {
        categories.merge(change.getClass().getSimpleName(), 1, Integer::sum);
        if (change instanceof TableChange) {
          tables.add(name(((TableChange) change).getChangedTable().getName(), caseSensitive));
        } else if (change instanceof AddTableChange) {
          tables.add(name(((AddTableChange) change).getNewTable().getName(), caseSensitive));
        } else if (change instanceof RemoveFunctionChange) {
          functions.add(
              name(((RemoveFunctionChange) change).getFunction().getName(), caseSensitive));
        } else if (change instanceof AddFunctionChange) {
          functions.add(
              name(((AddFunctionChange) change).getNewFunction().getName(), caseSensitive));
        } else if (!(change instanceof AddViewChange) && !(change instanceof RemoveViewChange)) {
          // Retain the previous conservative behavior for other structural changes, such as
          // sequences.
          recreateAll = true;
        }
      }
    }
  }

  List<StructureObject> getDrops() {
    return drops;
  }

  List<StructureObject> getCreates() {
    return creates;
  }

  static List<StructureObject> orderForCreation(Database model, boolean caseSensitive) {
    Map<String, Entry> entries = entries(model, caseSensitive);
    return ordered(entries, entries.keySet());
  }

  private static Map<String, Entry> entries(Database model, boolean caseSensitive) {
    Map<String, Entry> result = new TreeMap<>();
    for (View view : model.getViews()) {
      add(result, view, view.getStatement(), caseSensitive);
    }
    for (MaterializedView view : model.getMaterializedViews()) {
      add(result, view, view.getStatement(), caseSensitive);
    }
    return result;
  }

  private static void add(Map<String, Entry> result, StructureObject object, String statement,
      boolean caseSensitive) {
    result.put(name(object.getName(), caseSensitive),
        new Entry(object, new ViewDefinition(statement, caseSensitive)));
  }

  private static String name(String name, boolean caseSensitive) {
    return caseSensitive ? name : name.toUpperCase(Locale.ROOT);
  }

  private static boolean includeDependents(Map<String, Entry> entries, Set<String> affected) {
    boolean changed = false;
    for (Map.Entry<String, Entry> entry : entries.entrySet()) {
      if (!Collections.disjoint(entry.getValue().definition.getRelations(), affected)) {
        changed |= affected.add(entry.getKey());
      }
    }
    return changed;
  }

  private static List<StructureObject> ordered(Map<String, Entry> entries, Set<String> selected) {
    List<StructureObject> result = new ArrayList<>();
    Set<String> visiting = new HashSet<>();
    Set<String> visited = new HashSet<>();
    for (String name : entries.keySet()) {
      visit(name, entries, selected, visiting, visited, result);
    }
    return result;
  }

  private static void visit(String name, Map<String, Entry> entries, Set<String> selected,
      Set<String> visiting, Set<String> visited, List<StructureObject> result) {
    if (!selected.contains(name) || !entries.containsKey(name) || visited.contains(name)) {
      return;
    }
    if (!visiting.add(name)) {
      throw new DatabaseOperationException("Cyclic view dependencies involving " + name);
    }
    for (String dependency : entries.get(name).definition.getRelations()) {
      visit(dependency, entries, selected, visiting, visited, result);
    }
    visiting.remove(name);
    visited.add(name);
    result.add(entries.get(name).object);
  }

  private static class Entry {
    private final StructureObject object;
    private final ViewDefinition definition;

    Entry(StructureObject object, ViewDefinition definition) {
      this.object = object;
      this.definition = definition;
    }

    boolean dependsOn(Set<String> tables, Set<String> functions) {
      return !Collections.disjoint(definition.getRelations(), tables)
          || !Collections.disjoint(definition.getFunctions(), functions);
    }

    boolean sameAs(Entry other) {
      if (object.getClass() != other.object.getClass()
          || !definition.normalizedTokens().equals(other.definition.normalizedTokens())) {
        return false;
      }
      if (object instanceof MaterializedView) {
        MaterializedView a = (MaterializedView) object;
        MaterializedView b = (MaterializedView) other.object;
        return Arrays.equals(a.getColumns(), b.getColumns())
            && Arrays.equals(a.getIndices(), b.getIndices());
      }
      return true;
    }
  }
}
