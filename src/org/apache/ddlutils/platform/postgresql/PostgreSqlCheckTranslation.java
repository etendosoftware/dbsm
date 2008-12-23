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
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;
import org.apache.ddlutils.translation.Translation;

/**
 * 
 * @author adrian
 */
public class PostgreSqlCheckTranslation extends CombinedTranslation {

    public PostgreSqlCheckTranslation() {

        append(new ReplaceStrTranslation(" = ANY", " in"));
        append(new ReplaceStrTranslation("ARRAY[", ""));
        append(new ReplaceStrTranslation("]", ""));
        append(new ReplaceStrTranslation("::bpchar", ""));
        append(new ReplaceStrTranslation("::text", ""));
        append(new ReplaceStrTranslation("::character varying", ""));
        append(new ReplacePatTranslation(
                "\\(([0-9\\.\\-]+?)\\)::[Nn][Uu][Mm][Ee][Rr][Ii][Cc]", "$1"));
        append(new ReplacePatTranslation("\\((.*)\\) in \\((.*)\\)", "$1 IN $2"));
        append(new ReplacePatTranslation("\\[", ""));
        append(new Translation() {
            public String exec(String s) {
                return s.substring(1, s.length() - 1).toUpperCase();
            }
        });
    }
}
