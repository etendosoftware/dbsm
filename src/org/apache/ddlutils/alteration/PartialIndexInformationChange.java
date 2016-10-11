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
package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;

/**
 * This change class will be used for those platforms which does not support partial indexing, in
 * order to apply the alternative actions (for example updating comments). This helps to keep the
 * consistency between XML and DB if the where clause is changed on an existing index.
 * 
 * In platforms where partial indexes are supported, changes on partial indexes are handled with
 * {@link RemoveIndexChange} and {@link AddIndexChange} classes.
 */
public class PartialIndexInformationChange extends TableChangeImplBase {

  /** The index whose partial condition has changed */
  private Index _index;
  /** The old where clause */
  private String _oldWhereClause;
  /** The new where clause */
  private String _newWhereClause;

  /**
   * Creates a new partial index change. This means that the where clause condition of the index has
   * been modified.
   * 
   * @param table
   *          The table where the index belongs to
   * @param index
   *          The modified index
   * @param oldWhereClause
   *          The former where clause
   * @param newWhereClause
   *          The new where clause
   */
  public PartialIndexInformationChange(Table table, Index index, String oldWhereClause,
      String newWhereClause) {
    super(table);
    _index = index;
    _oldWhereClause = oldWhereClause;
    _newWhereClause = newWhereClause;
  }

  /**
   * Returns the modified index.
   * 
   * @return The index
   */
  public Index getIndex() {
    return _index;
  }

  /**
   * Returns the former where clause.
   * 
   * @return The old where clause
   */
  public String getOldWhereClause() {
    return _oldWhereClause;
  }

  /**
   * Returns the new where clause.
   * 
   * @return The new where clause
   */
  public String getNewWhereClause() {
    return _newWhereClause;
  }

  @Override
  public void apply(Database database, boolean caseSensitive) {
    _index.setWhereClause(_newWhereClause);
  }
}
