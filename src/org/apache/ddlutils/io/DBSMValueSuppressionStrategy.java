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

import org.apache.commons.betwixt.AttributeDescriptor;
import org.apache.commons.betwixt.ElementDescriptor;
import org.apache.commons.betwixt.strategy.ValueSuppressionStrategy;
import org.apache.ddlutils.model.Index;

public class DBSMValueSuppressionStrategy extends ValueSuppressionStrategy {

  private static final String WHERE_CLAUSE = "whereClause";

  /**
   * Determines if the given attribute value be suppressed
   * 
   * @param attributeDescriptor
   *          AttributeDescriptor describing the attribute, not null
   * @param value
   *          String with the attribute value
   * @return true if the attribute should not be written for the given value
   */
  @Override
  public boolean suppressAttribute(AttributeDescriptor attributeDescriptor, String value) {
    // For attributes, use default strategy: suppress all null values
    return ValueSuppressionStrategy.DEFAULT.suppressAttribute(attributeDescriptor, value);
  }

  /**
   * Determines if the given element value be suppressed
   * 
   * @param element
   *          ElementDescriptor describing the element
   * @param namespaceUri
   *          the namespace of the element to be written
   * @param localName
   *          the local name of the element to be written
   * @param qualifiedName
   *          the qualified name of the element to be written
   * @param value
   *          the Object value
   * @return true if the element should be suppressed (in other words, not written) for the given
   *         value
   */
  @Override
  public boolean suppressElement(ElementDescriptor element, String namespaceUri, String localName,
      String qualifiedName, Object value) {
    // Do not export Index empty whereClause element
    if (WHERE_CLAUSE.equals(localName) && value != null && value instanceof Index) {
      return ((Index) value).getWhereClause() == null;
    }
    return false;
  }
}
