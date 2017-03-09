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
 * This change class will be used for those platforms which does not support contains search
 * indexes, in order to apply the alternative actions (for example updating comments). This helps to
 * keep the consistency between XML and DB if the containsSearch property is changed on an existing
 * index.
 * 
 * In platforms where contains search indexes are supported, changes on these kind of indexes are
 * handled with {@link RemoveIndexChange} and {@link AddIndexChange} classes.
 */
public class ContainsSearchIndexInformationChange extends TableChangeImplBase {

  /** The index whose containsSearch property has changed */
  private Index _index;
  /** The new containsSearch property */
  private boolean _newContainsSearchValue;

  /**
   * Creates a new contains search index change. This means that the containsSearch property of the
   * index has been modified.
   * 
   * @param table
   *          The table where the index belongs to
   * @param index
   *          The modified index
   * @param newContainsSearchValue
   *          The new value of the containsSearch property
   */
  public ContainsSearchIndexInformationChange(Table table, Index index,
      boolean newContainsSearchValue) {
    super(table);
    _index = index;
    _newContainsSearchValue = newContainsSearchValue;
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
   * Returns the new containsSearch property value.
   * 
   * @return The new value of the containsSearch property
   */
  public boolean getNewContainsSearch() {
    return _newContainsSearchValue;
  }

  @Override
  public void apply(Database database, boolean caseSensitive) {
    _index.setContainsSearch(_newContainsSearchValue);
  }
}
