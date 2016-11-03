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

/**
 * Class that store optional, additional properties used by the DataSetTableQueryGenerator to
 * generate queries based on dataset tables (the mandatory ones being the dataset tables themselves
 * and the list of columns to use in the select clause)
 */
public class DataSetTableQueryGeneratorExtraProperties {

  private String orderByClause;
  private String additionalWhereClause;
  private String moduleId;

  public String getOrderByClause() {
    return orderByClause;
  }

  public void setOrderByClause(String orderByClause) {
    this.orderByClause = orderByClause;
  }

  public String getAdditionalWhereClause() {
    return additionalWhereClause;
  }

  public void setAdditionalWhereClause(String additionalWhereClause) {
    this.additionalWhereClause = additionalWhereClause;
  }

  public String getModuleId() {
    return moduleId;
  }

  public void setModuleId(String moduleId) {
    this.moduleId = moduleId;
  }
}
