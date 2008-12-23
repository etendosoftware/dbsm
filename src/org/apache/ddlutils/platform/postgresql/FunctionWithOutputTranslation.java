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

package org.apache.ddlutils.platform.postgresql;

import java.util.regex.Pattern;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.translation.ReplacePatTranslation;

/**
 * 
 * @author adrian
 */
public class FunctionWithOutputTranslation extends ReplacePatTranslation {

    /** Creates a new instance of FunctionWithOutputTranslation */
    public FunctionWithOutputTranslation(Function f) {

        StringBuffer strPattern = new StringBuffer();
        strPattern.append("(");
        addPattern(strPattern, f.getName());
        strPattern.append(")(\\s|\\t)*\\(");

        StringBuffer strInto = new StringBuffer();

        StringBuffer strParams = new StringBuffer();

        for (int i = 0; i < f.getParameterCount(); i++) {

            // Build the pattern
            if (i > 0) {
                strPattern.append(",");
            }
            if (i < f.getParameterCount() - 1) {
                strPattern.append("([^,;]+)");
            } else {
                strPattern.append("([^,;\\)]+)");
            }

            Parameter p = (Parameter) f.getParameter(i);
            if (p.getModeCode() == Parameter.MODE_OUT) {
                // Build the SELECT INTO clause
                if (strInto.length() > 0) {
                    strInto.append(",");
                }
                strInto.append("$");
                strInto.append(3 + i);
            } else {
                // Build the parameters clause
                if (strParams.length() > 0) {
                    strParams.append(",");
                }
                strParams.append("$");
                strParams.append(3 + i);
            }
        }

        // The pattern
        strPattern.append("\\)");
        _p = Pattern.compile(strPattern.toString());

        // The replace string
        StringBuffer strReplace = new StringBuffer();
        strReplace.append("SELECT * INTO ");
        strReplace.append(strInto);
        strReplace.append(" FROM $1(");
        strReplace.append(strParams);
        strReplace.append(")");
        _replaceStr = strReplace.toString();
        //        
        // System.out.println(_p);
        // System.out.println(_replaceStr);

    }

    private static void addPattern(StringBuffer pattern, String searchStr) {

        for (int i = 0; i < searchStr.length(); i++) {
            char c = searchStr.charAt(i);
            if (Character.isLetter(c)) {
                pattern.append('[');
                pattern.append(Character.toUpperCase(c));
                pattern.append(Character.toLowerCase(c));
                pattern.append(']');
            } else {
                pattern.append(c);
            }
        }
    }
}
