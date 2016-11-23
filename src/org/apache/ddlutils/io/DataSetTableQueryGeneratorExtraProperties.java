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
import java.util.List;

/**
 * Class that store optional, additional properties used by the DataSetTableQueryGenerator to
 * generate queries based on dataset tables (the mandatory ones being the dataset tables themselves
 * and the list of columns to use in the select clause)
 */
public class DataSetTableQueryGeneratorExtraProperties {

  private String orderByClause;
  private List<WhereClauseSimpleExpression> additionalWhereClauses = new ArrayList<>();
  private String moduleId = "";

  public String getOrderByClause() {
    return orderByClause;
  }

  public void setOrderByClause(String orderByClause) {
    this.orderByClause = orderByClause;
  }

  public void addWhereClauseExpression(String columnName, String operator, String value) {
    WhereClauseSimpleExpression whereClauseExpression = new WhereClauseSimpleExpression(columnName,
        operator, value);
    additionalWhereClauses.add(whereClauseExpression);
  }

  public List<WhereClauseSimpleExpression> getAdditionalWhereClauses() {
    return additionalWhereClauses;
  }

  public String getModuleId() {
    return moduleId;
  }

  public void setModuleId(String moduleId) {
    this.moduleId = moduleId;
  }

  public class WhereClauseSimpleExpression {
    private String columnName;
    private String operator;
    private String value;

    public WhereClauseSimpleExpression(String columnName, String operator, String value) {
      this.columnName = columnName;
      this.operator = operator;
      this.value = value;
    }

    public String getColumnName() {
      return columnName;
    }

    public void setColumnName(String columnName) {
      this.columnName = columnName;
    }

    public String getOperator() {
      return operator;
    }

    public void setOperator(String operator) {
      this.operator = operator;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

  }
}
