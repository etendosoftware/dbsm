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

package org.apache.ddlutils.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.io.DataSetTableQueryGeneratorExtraProperties.WhereClauseSimpleExpression;
import org.apache.ddlutils.model.Table;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
 * Class that provides methods that given a OBDatasetTable or a list of OBDatasetTables, return a
 * query that can be used to retrieve the contents of the datasets. If more than one dataset table
 * is provided, the resulting where clause will be the concatenation of all the dataset table where
 * clauses
 * 
 * A list of columns to use in the SELECT clause can be provided. If no columns are provided then
 * all of them will be retrieved using '*'
 * 
 * Some extra properties (additional where clauses, sort by clauses, etc) can be defined to
 * customize the query further. The DataSetTableQueryGeneratorExtraProperties stores all the
 * supported extra properties.
 */
public class DataSetTableQueryGenerator {

  /**
   * Generates a query that can be used to retrieve the records represented by the given dataset
   * 
   * @param dataSetTable
   *          a dataset table
   * @param columns
   *          the list of columns that will be used in the SELECT clause
   * @return a query string that can be used to fetch the records of the given dataset
   */
  public String generateQuery(OBDatasetTable dataSetTable, List<String> columns) {
    List<OBDatasetTable> dataSetTables = new ArrayList<>();
    dataSetTables.add(dataSetTable);
    return generateQuery(dataSetTables, columns);
  }

  /**
   * Generates a query that can be used to retrieve the records represented by the given datasets.
   * The resulting where clause will be the concatenation of all the dataset table where clauses. It
   * assumes that all the dataSetTables stored in the list reference the same table
   * 
   * @param dataSetTable
   *          a dataset table
   * @param columns
   *          the list of columns that will be used in the SELECT clause
   * @return a query string that can be used to fetch the records of the given datasets
   */
  public String generateQuery(List<OBDatasetTable> dataSetTables, List<String> columns) {
    DataSetTableQueryGeneratorExtraProperties extraProperties = new DataSetTableQueryGeneratorExtraProperties();
    return generateQuery(dataSetTables, columns, extraProperties);
  }

  /**
   * Generates a query that can be used to retrieve the records represented by the given dataset
   * 
   * @param dataSetTable
   *          a dataset table
   * @param extraProperties
   *          extra properties to further customize the query (see
   *          DataSetTableQueryGeneratorExtraProperties)
   * @return a query string that can be used to fetch the records of the given dataset
   */
  public String generateQuery(OBDatasetTable dataSetTable,
      DataSetTableQueryGeneratorExtraProperties extraProperties) {
    // if no columns are provided, all of them will be listed
    List<String> columnNames = new ArrayList<String>();
    return generateQuery(dataSetTable, columnNames, extraProperties);
  }

  /**
   * Generates a query that can be used to retrieve the records represented by a given dataset.
   * 
   * @param dataSetTable
   *          a dataset table
   * @param columns
   *          the list of columns that will be used in the SELECT clause
   * @param extraProperties
   *          extra properties to further customize the query (see
   *          DataSetTableQueryGeneratorExtraProperties)
   * @return a query string that can be used to fetch the records of the given dataset
   */
  public String generateQuery(OBDatasetTable dataSetTable, List<String> columns,
      DataSetTableQueryGeneratorExtraProperties extraProperties) {
    List<OBDatasetTable> dataSetTables = new ArrayList<>();
    dataSetTables.add(dataSetTable);
    return generateQuery(dataSetTables, columns, extraProperties);
  }

  /**
   * Generates a query that can be used to retrieve the records represented by the given datasets.
   * The resulting where clause will be the concatenation of all the dataset table where clauses. It
   * assumes that all the dataSetTables stored in the list reference the same table
   * 
   * @param dataSetTables
   *          a list of dataset tables
   * @param columns
   *          the list of columns that will be used in the SELECT clause
   * @param extraProperties
   *          extra properties to further customize the query (see
   *          DataSetTableQueryGeneratorExtraProperties)
   * @return a query string that can be used to fetch the records of the given dataset
   */
  public String generateQuery(List<OBDatasetTable> dataSetTables, List<String> columns,
      DataSetTableQueryGeneratorExtraProperties extraProperties) {
    customizeExtraProperties(dataSetTables, extraProperties);
    String moduleId = extraProperties.getModuleId();
    List<WhereClauseSimpleExpression> additionalWhereClauses = extraProperties
        .getAdditionalWhereClauses();
    String orderByClause = extraProperties.getOrderByClause();
    // all the dataSetTables reference the same table
    String tableName = dataSetTables.get(0).getName();
    String whereClause = joinWhereClausesWithOrOperator(dataSetTables, moduleId);
    if (!additionalWhereClauses.isEmpty()) {
      String additionalWhereClause = buildAdditionalWhereClause(additionalWhereClauses);
      if (StringUtils.isBlank(whereClause)) {
        whereClause = additionalWhereClause;
      } else {
        whereClause = "(" + whereClause + ") AND (" + additionalWhereClause + ")";
      }
    }
    return generateQuery(tableName, columns, whereClause, orderByClause);
  }

  /**
   * Hook that allows the subclasses of DataSetTableQueryGenerator to add
   * WhereClauseSimpleExpressions to DataSetTableQueryGeneratorExtraProperties, which will result in
   * extra where clauses being added to the query.
   * 
   * For instance, this method is extended by the
   * org.openbravo.retail.storeserver.synchronization.task.ExportStoreDataSetTableQueryGenerator to
   * create a where clause that ensures that the records part of exported ADRD tables are not
   * returned by the query
   * 
   * @param dataSetTables
   *          the dataset tables being exported
   * @param extraProperties
   *          the original extraProperties passed to the DataSetTableQueryGenerator constructor
   */
  protected void customizeExtraProperties(List<OBDatasetTable> dataSetTables,
      DataSetTableQueryGeneratorExtraProperties extraProperties) {
  }

  private String buildAdditionalWhereClause(
      List<WhereClauseSimpleExpression> additionalWhereClauses) {
    StringBuilder additionalWhereClause = new StringBuilder();
    Iterator<WhereClauseSimpleExpression> iterator = additionalWhereClauses.iterator();
    while (iterator.hasNext()) {
      WhereClauseSimpleExpression expression = iterator.next();
      additionalWhereClause.append(expression.getColumnName() + " " + expression.getOperator() + " "
          + expression.getValue());
      if (iterator.hasNext()) {
        additionalWhereClause.append(" AND ");
      }
    }
    return additionalWhereClause.toString();
  }

  private String joinWhereClausesWithOrOperator(List<OBDatasetTable> dataSetTables,
      String moduleId) {
    StringBuilder whereClauseBuilder = new StringBuilder();
    for (OBDatasetTable dataSetTable : dataSetTables) {
      String whereClause = getWhereClause(dataSetTable, moduleId);
      if (!StringUtils.isBlank(whereClause)) {
        if (whereClauseBuilder.length() > 0) {
          whereClauseBuilder.append(" OR ");
        }
        whereClauseBuilder.append(" (" + whereClause + ") ");
      }
    }
    return whereClauseBuilder.toString();
  }

  private String generateQuery(String tableName, List<String> columns, String whereClause,
      String orderByClause) {
    StringBuilder query = new StringBuilder();
    String columnNames = columns.isEmpty() ? "*" : stringifyColumnNames(columns);
    String transformedWhereClause = transformWhereClause(tableName, whereClause);
    query.append("SELECT " + columnNames + " FROM " + tableName + " ");
    if (!StringUtils.isBlank(transformedWhereClause)) {
      query.append(" WHERE " + transformedWhereClause);
    }
    if (!StringUtils.isBlank(orderByClause)) {
      query.append(" ORDER BY " + orderByClause);
    }
    return query.toString();
  }

  private String getWhereClause(OBDatasetTable dataSetTable, String moduleId) {
    String whereClause = dataSetTable.getWhereclause(moduleId);
    // if the dataset does not have a where clause the getWhereClause will return null
    // replace it with a clause that is always true to avoid working with null clauses
    if (whereClause == null) {
      whereClause = "1=1";
    }
    if (dataSetTable.getSecondarywhereclause() != null) {
      whereClause += " AND " + dataSetTable.getSecondarywhereclause() + " ";
    }
    return whereClause;
  }

  /**
   * Applies transformations on a given where clause
   * 
   * @param whereClause
   *          the original where clause
   * @return the where clause after applying the transformations
   */
  protected String transformWhereClause(String tableName, String whereClause) {
    return whereClause;
  }

  /**
   * Given a table, returns a list of its key columns separated by a comma, so that it can be used
   * in an orderBy clause
   */
  public String buildOrderByClauseUsingKeyColumns(Table table) {
    StringBuilder orderByColumns = new StringBuilder();
    for (int j = 0; j < table.getPrimaryKeyColumns().length; j++) {
      if (j > 0) {
        orderByColumns.append(",");
      }
      orderByColumns.append(table.getPrimaryKeyColumns()[j].getName());
    }
    return orderByColumns.toString();
  }

  /**
   * Given a list of strings, returns a string with the concatenation of all the strings, separated
   * with commas
   */
  private String stringifyColumnNames(List<String> columns) {
    StringBuilder listStringified = new StringBuilder();
    Iterator<String> iterator = columns.iterator();
    while (iterator.hasNext()) {
      listStringified.append(iterator.next());
      if (iterator.hasNext()) {
        listStringified.append(",");
      }
    }
    return listStringified.toString();
  }
}
