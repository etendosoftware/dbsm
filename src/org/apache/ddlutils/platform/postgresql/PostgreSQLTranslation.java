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

import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;
import org.apache.ddlutils.translation.ReplacePatTranslation;

/**
 *
 * @author adrian
 */
public class PostgreSQLTranslation extends CombinedTranslation {
    
    /** Creates a new instance of PostgreSQLTranslation */
    public PostgreSQLTranslation() {
        append(new ReplaceStrTranslation("SYSDATE", "CURRENT_TIMESTAMP")); // See also PostgresSQLBuilder.getNativeFunction();
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)('MM')(\\))", "DATE_TRUNC('month',$3)"));
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)( 'MM')(\\))", "DATE_TRUNC('month',$3)"));
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)('DD')(\\))", "DATE_TRUNC('day',$3)"));
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)( 'DD')(\\))", "DATE_TRUNC('day',$3)"));
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)('DY')(\\))", "DATE_TRUNC('week',$3)"));
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)( 'DY')(\\))", "DATE_TRUNC('week',$3)"));
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)('Q')(\\))", "DATE_TRUNC('quarter',$3)"));
        append(new ReplacePatTranslation("(TRUNC)(\\()(\\S+)(,)( 'Q')(\\))", "DATE_TRUNC('quarter',$3)"));
        append(new ReplacePatTranslation("(SUM)(\\()(\\w+)(\\))", "SUM(CAST($3 as NUMERIC))"));
        append(new ReplacePatTranslation("(SUM)(\\()(\\w+)(\\.)(\\w+)(\\))", "SUM(CAST($3$4$5 as NUMERIC))"));
    }
    
}
