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
 * This change class will be used for those platforms which does not support similarity indexes, in
 * order to apply the alternative actions (for example updating comments). This helps to keep the
 * consistency between XML and DB if the similarity property is changed on an existing index.
 * 
 * In platforms where similarity indexes are supported, changes on these kind of indexes are handled
 * with {@link RemoveIndexChange} and {@link AddIndexChange} classes.
 */
public class SimilarityIndexInformationChange extends TableChangeImplBase {

  /** The index whose similarity property has changed */
  private Index _index;
  /** The new similarity property */
  private boolean _newSimilarityValue;

  /**
   * Creates a new similarity index change. This means that the similarity property of the index has
   * been modified.
   * 
   * @param table
   *          The table where the index belongs to
   * @param index
   *          The modified index
   * @param newSimilarity
   *          The new value of the similarity property
   */
  public SimilarityIndexInformationChange(Table table, Index index, boolean newSimilarityValue) {
    super(table);
    _index = index;
    _newSimilarityValue = newSimilarityValue;
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
   * Returns the new similarity property value.
   * 
   * @return The new value of the similarity property
   */
  public boolean getNewSimilarity() {
    return _newSimilarityValue;
  }

  @Override
  public void apply(Database database, boolean caseSensitive) {
    _index.setSimilarity(_newSimilarityValue);
  }
}
