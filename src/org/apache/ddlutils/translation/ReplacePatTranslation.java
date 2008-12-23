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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author adrian
 */
public class ReplacePatTranslation implements Translation {

    protected Pattern _p;
    protected String _replaceStr;

    protected ReplacePatTranslation() {
        _p = null;
        _replaceStr = null;
    }

    /** Creates a new instance of ReplaceTranslation */
    public ReplacePatTranslation(String pattern, String replaceStr) {
        _p = Pattern.compile(pattern);
        _replaceStr = replaceStr;
    }

    public String exec(String s) {

        Matcher m = _p.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, _replaceStr);
        }
        m.appendTail(sb);
        return sb.toString();

    }

}
