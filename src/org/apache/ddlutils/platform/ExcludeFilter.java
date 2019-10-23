/*
 ************************************************************************************
 * Copyright (C) 2001-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.platform.modelexclusion.ExcludedFunction;
import org.apache.ddlutils.platform.modelexclusion.ExcludedMaterializedView;
import org.apache.ddlutils.platform.modelexclusion.ExcludedSequence;
import org.apache.ddlutils.platform.modelexclusion.ExcludedTable;
import org.apache.ddlutils.platform.modelexclusion.ExcludedTrigger;
import org.apache.ddlutils.platform.modelexclusion.ExcludedView;
import org.apache.log4j.Logger;
import org.openbravo.ddlutils.util.ExceptionRow;

/**
 * 
 * @author adrian
 */
public class ExcludeFilter implements Cloneable {
  Vector<String> prefixes = new Vector<String>();
  Vector<String> rejectedPrefixes = new Vector<String>();
  Vector<String> othersPrefixes = new Vector<String>();
  Vector<String> otherActivePrefixes = new Vector<String>();

  private HashMap<String, Vector<String>> prefixDependencies = new HashMap<String, Vector<String>>();
  String modName;
  static String externalPrefix = "EM";

  Vector<ExceptionRow> exceptions = new Vector<ExceptionRow>();
  Vector<ExceptionRow> othersexceptions = new Vector<ExceptionRow>();

  Vector<String> excludedTables = new Vector<String>();
  Vector<String> excludedFunctions = new Vector<String>();
  Vector<String> excludedTriggers = new Vector<String>();
  Vector<String> excludedViews = new Vector<String>();
  Vector<String> excludedMaterializedViews = new Vector<String>();
  Vector<String> excludedSequences = new Vector<String>();

  private Logger log4j = Logger.getLogger(getClass());

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("***Filtered tables: " + "\n");
    for (String s : excludedTables) {
      sb.append("  -" + s + "\n");
    }
    sb.append("***Filtered views: " + "\n");
    for (String s : excludedViews) {
      sb.append("  -" + s + "\n");
    }
    sb.append("***Filtered materialized views: " + "\n");
    for (String s : excludedMaterializedViews) {
      sb.append("  -" + s + "\n");
    }
    sb.append("***Filtered triggers: " + "\n");
    for (String s : excludedTriggers) {
      sb.append("  -" + s + "\n");
    }
    sb.append("***Filtered functions: " + "\n");
    for (String s : excludedFunctions) {
      sb.append("  -" + s + "\n");
    }
    sb.append("***Filtered sequences: " + "\n");
    for (String s : excludedSequences) {
      sb.append("  -" + s + "\n");
    }
    return sb.toString();
  }

  @Override
  public ExcludeFilter clone() {
    ExcludeFilter filter = new ExcludeFilter();

    filter.prefixes.addAll(prefixes);
    filter.rejectedPrefixes.addAll(rejectedPrefixes);
    filter.othersPrefixes.addAll(othersPrefixes);
    filter.otherActivePrefixes.addAll(otherActivePrefixes);

    for (String prefDep : prefixDependencies.keySet()) {
      Vector<String> dep = new Vector<String>();
      dep.addAll(prefixDependencies.get(prefDep));
      filter.prefixDependencies.put(prefDep, dep);
    }

    filter.excludedTables.addAll(excludedTables);
    filter.excludedViews.addAll(excludedViews);
    filter.excludedMaterializedViews.addAll(excludedMaterializedViews);
    filter.excludedTriggers.addAll(excludedTriggers);
    filter.excludedFunctions.addAll(excludedFunctions);
    filter.excludedSequences.addAll(excludedSequences);

    for (ExceptionRow row : exceptions) {
      filter.exceptions.add((ExceptionRow) row.clone());
    }
    for (ExceptionRow row : othersexceptions) {
      filter.othersexceptions.add((ExceptionRow) row.clone());
    }
    return filter;
  }

  public void fillFromFile(File file) {

    try {
      DatabaseIO dbIO = new DatabaseIO();
      Vector<Object> list = dbIO.readExcludedObjects(file);

      for (Object obj : list) {
        if (obj instanceof ExcludedTable) {
          excludedTables.add(((ExcludedTable) obj).getName());
        } else if (obj instanceof ExcludedView) {
          excludedViews.add(((ExcludedView) obj).getName());
        } else if (obj instanceof ExcludedMaterializedView) {
          excludedMaterializedViews.add(((ExcludedMaterializedView) obj).getName());
        } else if (obj instanceof ExcludedFunction) {
          excludedFunctions.add(((ExcludedFunction) obj).getName());
        } else if (obj instanceof ExcludedTrigger) {
          excludedTriggers.add(((ExcludedTrigger) obj).getName());
        } else if (obj instanceof ExcludedSequence) {
          excludedSequences.add(((ExcludedSequence) obj).getName());
        }
      }
    } catch (Exception e) {
      log4j.error("ExcludeFilter file couldn't be read: " + file.getAbsolutePath(), e);
    }
  }

  public void exportToFile(File file) {

    try {
      Vector<Object> v = new Vector<Object>();

      String[] tables = getExcludedTables();
      for (String table : tables) {
        v.add(new ExcludedTable(table));
      }

      String[] views = getExcludedViews();
      for (String view : views) {
        v.add(new ExcludedView(view));
      }

      String[] functions = getExcludedFunctions();
      for (String function : functions) {
        v.add(new ExcludedFunction(function));
      }

      String[] triggers = getExcludedTriggers();
      for (String trigger : triggers) {
        v.add(new ExcludedTrigger(trigger));
      }

      String[] sequences = getExcludedSequences();
      for (String sequence : sequences) {
        v.add(new ExcludedSequence(sequence));
      }

      DatabaseIO dbIO = new DatabaseIO();
      dbIO.writeExcludedObjects(file, v);
    } catch (Exception e) {
      log4j.error("Error while writing file", e);
    }
  }

  public String[] getExcludedTables() {
    return excludedTables.toArray(new String[0]);
  }

  public String[] getExcludedViews() {
    return excludedViews.toArray(new String[0]);
  }

  public String[] getExcludedMaterializedViews() {
    return excludedMaterializedViews.toArray(new String[0]);
  }

  public String[] getExcludedSequences() {
    return excludedSequences.toArray(new String[0]);
  }

  public String[] getExcludedFunctions() {
    return excludedFunctions.toArray(new String[0]);
  }

  public String[] getExcludedTriggers() {
    return excludedTriggers.toArray(new String[0]);
  }

  public void addPrefix(String prefix) {
    this.prefixes.add(prefix);
  }

  public void addOthersPrefixes(Vector<String> othersPrefixes) {
    this.othersPrefixes.addAll(othersPrefixes);
  }

  public void addOtherActivePrefixes(Vector<String> otherActivePrefixes) {
    this.otherActivePrefixes.addAll(otherActivePrefixes);
  }

  // does case-insensitive matching on name, so case of name can be arbitrary
  public boolean compliesWithNamingRuleObject(String name) {
    if (isInOthersExceptionsObject(name)) {
      return false;
    }

    for (String prefix : prefixes) {
      if (hasPrefix(name, prefix)) {
        return true;
      }
    }
    // It doesn't comply with naming rule. We'll check exceptions table
    return isInExceptionsObject(name);
  }

  public boolean compliesWithNamingRuleTableObject(String name, String tableName) {
    if (isInOthersExceptionsTableObject(name, tableName)) {
      return false;
    }

    for (String prefix : prefixes) {
      if (hasPrefix(name, prefix)) {
        return true;
      }
    }
    // It doesn't comply with naming rule. We'll check exceptions table
    return isInExceptionsTableObject(name, tableName);
  }

  private boolean hasPrefix(String name, String prefix) {
    if (prefix.equalsIgnoreCase(name)) {
      return true;
    }
    return name.toUpperCase().startsWith(prefix.toUpperCase() + "_");
  }

  public void addDependencies(HashMap<String, Vector<String>> map) {
    prefixDependencies = map;
  }

  public boolean noPrefix() {
    return prefixes.size() == 0;
  }

  public void setName(String name) {
    this.modName = name;
  }

  public boolean isDependant(String name) {
    for (String prefix : prefixDependencies.get(modName)) {
      if (hasPrefix(name, prefix)) {
        return true;
      }
    }
    return false;
  }

  public boolean compliesWithExternalPrefix(String name, String tableName) {
    if (isInOthersExceptionsTableObject(name, tableName)) {
      return true;
    }
    return hasPrefix(name, externalPrefix);
  }

  public boolean compliesWithExternalNamingRule(String name, String tableName) {
    if (isInExceptionsTableObject(name, tableName)) {
      return true;
    }

    for (String prefix : prefixes) {
      if (hasPrefix(name, externalPrefix + "_" + prefix)) {
        return true;
      }
    }
    return false;
  }

  public Vector<ExceptionRow> getExceptions() {
    return exceptions;
  }

  public void addException(ExceptionRow row) {
    exceptions.add(row);
  }

  public void addOthersException(ExceptionRow row) {
    othersexceptions.add(row);
  }

  public boolean isInOthersExceptionsObject(String objectName) {
    for (ExceptionRow row : othersexceptions) {
      if (objectName.equalsIgnoreCase(row.name1)) {
        return true;
      }
    }
    return false;
  }

  public boolean isInExceptionsObject(String objectName) {
    for (ExceptionRow row : exceptions) {
      if (objectName.equalsIgnoreCase(row.name1)) {
        return true;
      }
    }
    return false;
  }

  public boolean isInExceptionsTableObject(String objectName, String tableName) {
    for (ExceptionRow row : exceptions) {
      if (objectName.equalsIgnoreCase(row.name1) && tableName.equalsIgnoreCase(row.name2)) {
        return true;
      }
    }
    return false;
  }

  public boolean isInOthersExceptionsTableObject(String objectName, String tableName) {
    for (ExceptionRow row : othersexceptions) {
      if (objectName.equalsIgnoreCase(row.name1) && tableName.equalsIgnoreCase(row.name2)) {
        return true;
      }
    }
    return false;
  }

  public String getExcludeFilterWhereClause(String fieldIdentifier, String[] excludedObjects,
      boolean firstExpressionInWhereClause) {
    // if no escape clause is provided, then use none
    // those dialects that require it (i.e. Oracle) will use the method that accepts a escape clause
    String escapeClause = null;
    return getExcludeFilterWhereClause(fieldIdentifier, excludedObjects,
        firstExpressionInWhereClause, escapeClause);
  }

  /**
   * Returns the where clause that should be used to exclude the provided list of excluded objects.
   * It supports using wildcards. In this case, the exclusion will be handled like a wildcard if it
   * contains the '%' character
   */
  public String getExcludeFilterWhereClause(String fieldIdentifier, String[] excludedObjects,
      boolean firstExpressionInWhereClause, String escapeClause) {
    StringBuilder whereClause = new StringBuilder();
    if (excludedObjects.length > 0) {
      if (firstExpressionInWhereClause) {
        whereClause.append(" WHERE ");
      } else {
        whereClause.append(" AND ");
      }
      // exclusions defined with wild cards must be handled differently that those not defined with
      // wild cards. the ones defined with wildcards will add clauses like this:
      // AND UPPER(fieldname) NOT LIKE UPPER(wildcard_exclusion1) AND UPPER(fieldname) NOT LIKE
      // UPPER(wildcard_exclusion2) ...
      // while the ones not defined with wildcards will add a clause like this one:
      // AND UPPER(fieldname) NOT IN (exclusion1, exclusion2, ...)
      List<String> nonWildcardExcludedObjects = getNonWildcardExcludedObjects(excludedObjects);
      List<String> wildcardExcludedObjects = getWildcardExcludedObjects(excludedObjects);
      if (!nonWildcardExcludedObjects.isEmpty()) {
        whereClause.append(
            buildNonWildcardExcludeFilterWhereClause(fieldIdentifier, nonWildcardExcludedObjects));
      }
      if (!wildcardExcludedObjects.isEmpty()) {
        if (!nonWildcardExcludedObjects.isEmpty()) {
          whereClause.append(" AND ");
        }
        whereClause.append(buildwildcardExcludeFilterWhereClause(fieldIdentifier,
            wildcardExcludedObjects, escapeClause));
      }
    }
    return whereClause.toString();
  }

  private String buildNonWildcardExcludeFilterWhereClause(String fieldIdentifier,
      List<String> nonWildcardExcludedObjects) {
    StringBuilder whereClause = new StringBuilder();

    whereClause.append("UPPER(" + fieldIdentifier + ") NOT IN (");
    whereClause.append(getListObjects((String[]) nonWildcardExcludedObjects
        .toArray(new String[nonWildcardExcludedObjects.size()])));
    whereClause.append(") ");

    return whereClause.toString();
  }

  private Object buildwildcardExcludeFilterWhereClause(String fieldIdentifier,
      List<String> wildcardExcludedObjects, String escapeClause) {
    StringBuilder whereClause = new StringBuilder();
    Iterator<String> iterator = wildcardExcludedObjects.iterator();
    while (iterator.hasNext()) {
      whereClause.append(" UPPER(" + fieldIdentifier + ")");
      whereClause.append(" NOT LIKE UPPER('" + iterator.next() + "')");
      if (!StringUtils.isBlank(escapeClause)) {
        whereClause.append(" " + escapeClause + " ");
      }
      if (iterator.hasNext()) {
        whereClause.append(" AND ");
      }
    }
    return whereClause.toString();
  }

  private List<String> getNonWildcardExcludedObjects(String[] excludedObjects) {
    List<String> nonWildcardExcludedObjects = new ArrayList<String>();
    for (String excludedObject : excludedObjects) {
      if (excludedObject != null && !excludedObject.contains("%")) {
        nonWildcardExcludedObjects.add(excludedObject);
      }
    }
    return nonWildcardExcludedObjects;
  }

  private List<String> getWildcardExcludedObjects(String[] excludedObjects) {
    List<String> wildcardExcludedObjects = new ArrayList<String>();
    for (String excludedObject : excludedObjects) {
      if (excludedObject != null && excludedObject.contains("%")) {
        wildcardExcludedObjects.add(excludedObject);
      }
    }
    return wildcardExcludedObjects;
  }

  private String getListObjects(String[] list) {
    StringBuffer s = new StringBuffer();
    for (int i = 0; i < list.length; i++) {
      if (i > 0) {
        s.append(", ");
      }
      s.append('\'');
      s.append(list[i]);
      s.append('\'');
    }
    return s.toString();
  }
}
