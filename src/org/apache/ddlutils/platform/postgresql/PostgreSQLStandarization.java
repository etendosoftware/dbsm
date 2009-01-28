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

/**
 * 
 * @author adrian
 */
public class PostgreSQLStandarization extends CombinedTranslation {

    /** Creates a new instance of PostgreSQLTranslation */
    public PostgreSQLStandarization() {
        // postgres castings '::text', '::numeric', '::character varying',
        // '::date', '::bpchar', '::timestamp', '::\"unknown\"' , ::timestamp
        // with time zone
        append(new ReplacePatTranslation(
                "::[A-Za-z\"]*( varying)?( with time zone)?(\\[\\])?", ""));

        // sql "in" sentence and "not in"
        append(new ReplacePatTranslation(
                "=\\s*[Aa][Nn][Yy]\\s*\\(\\s*[Aa][Rr][Rr][Aa][Yy]\\s*\\[(.*)\\]\\s*\\)",
                "IN ($1)"));
        append(new ReplacePatTranslation(
                "<>\\s*[Aa][Ll][Ll]\\s*\\(\\s*[Aa][Rr][Rr][Aa][Yy]\\s*\\[(.*)\\]\\s*\\)",
                "NOT IN ($1)"));

        // date truncs date_trunk('month', now()) --> trunc(now(),'MM')
        // suports 3 levels of recursivily in parenthesis -->
        // date_trunk('month', ...(...(...(...)...)...)...)
        append(new ReplacePatTranslation(
                "[Dd][Aa][Tt][Ee]_[Tt][Rr][Uu][Nn][Cc]\\s*\\(\\s*'[Mm][Oo][Nn][Tt][Hh]'\\s*,\\s*([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*)\\s*\\)",
                "TRUNC($1, 'MM')"));
        append(new ReplacePatTranslation(
                "[Dd][Aa][Tt][Ee]_[Tt][Rr][Uu][Nn][Cc]\\s*\\(\\s*'[Dd][Aa][Yy]'\\s*,\\s*([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*)\\s*\\)",
                "TRUNC($1, 'DD')"));
        append(new ReplacePatTranslation(
                "[Dd][Aa][Tt][Ee]_[Tt][Rr][Uu][Nn][Cc]\\s*\\(\\s*'[Ww][Ee][Ee][Kk]'\\s*,\\s*([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*)\\s*\\)",
                "TRUNC($1, 'DY')"));
        append(new ReplacePatTranslation(
                "[Dd][Aa][Tt][Ee]_[Tt][Rr][Uu][Nn][Cc]\\s*\\(\\s*'[Qq][Uu][Aa][Rr][Tt][Ee][Rr]'\\s*,\\s*([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*(\\([^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*\\))?[^(\\(\\))]*)\\s*\\)",
                "TRUNC($1, 'Q')"));

        // numeric cast in sums
        append(new ReplacePatTranslation(
                "[Ss][Uu][Mm]\\s*\\(\\s*[Cc][Aa][Ss][Tt]\\s*\\((.*)\\s*[Aa][Ss]\\s*[Nn][Uu][Mm][Ee][Rr][Ii][Cc]\\)\\s*\\)",
                "SUM($1)"));

        // removes the caracter ";" at the end of sql sentence
        append(new ReplaceStrTranslation(";", ""));
        // append(new ByLineTranslation(new
        // ReplacePatTranslation("^[\\s]*(.*?)[\\s]*","$1")));
    }

}
