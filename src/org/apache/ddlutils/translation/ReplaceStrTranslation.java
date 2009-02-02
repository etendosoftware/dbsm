/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.translation;

/**
 * 
 * @author adrian
 */
public class ReplaceStrTranslation extends ReplacePatTranslation {

  /** Creates a new instance of ReplaceExtTranslation */
  public ReplaceStrTranslation(String searchStr, String replaceStr) {
    super(getPattern(searchStr), replaceStr);
  }

  private static String getPattern(String searchStr) {

    StringBuffer pattern = new StringBuffer();
    for (int i = 0; i < searchStr.length(); i++) {
      char c = searchStr.charAt(i);
      if (c == ' ') {
        pattern.append("(\\s|\\t)");
      } else if (Character.isLetter(c)) {
        pattern.append('[');
        pattern.append(Character.toUpperCase(c));
        pattern.append(Character.toLowerCase(c));
        pattern.append(']');
      } else if (c == '[') {
        pattern.append("(\\[)");
      } else if (c == ']') {
        pattern.append("(\\])");
      } else if (c == '(') {
        pattern.append("(\\()");
      } else if (c == ')') {
        pattern.append("(\\))");
      } else if (c == '\'') {
        pattern.append("(')");
      } else {
        pattern.append(c);
      }
    }
    return pattern.toString();
  }
}
