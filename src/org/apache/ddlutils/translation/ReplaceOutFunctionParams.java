/*
 ************************************************************************************
 * Copyright (C) 2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.translation;

import org.apache.commons.lang3.StringUtils;

/**
 * Replacements for functions with out parameters, treated as special to skip to apply regexp when
 * it's known it will not find any result as in some cases regexp can be very slow for non matching
 * expressions.
 */
public class ReplaceOutFunctionParams extends ReplacePatTranslation {
  private static final String PARAM_SEPARATOR = ",";
  private String functionName;
  private int numOfParams;

  public ReplaceOutFunctionParams(String pattern, String replace, String functionName) {
    super(pattern, replace);
    this.functionName = functionName.toLowerCase();
    numOfParams = StringUtils.countMatches(_p.pattern(), PARAM_SEPARATOR);
  }

  @Override
  public String exec(String s) {
    if (!s.toLowerCase().contains(functionName)) {
      return s;
    } else if (numOfParams > StringUtils.countMatches(s, PARAM_SEPARATOR)) {
      // Pattern has more parameters than actual String, as regexp won't match, we can skip it
      return s;
    }
    return super.exec(s);
  }

}
