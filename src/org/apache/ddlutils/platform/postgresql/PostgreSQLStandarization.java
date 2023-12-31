/*
 ************************************************************************************
 * Copyright (C) 2001-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.postgresql;

import org.apache.ddlutils.translation.ByLineTranslation;
import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;
import org.apache.ddlutils.translation.Translation;

/**
 * 
 * @author adrian
 */
public class PostgreSQLStandarization extends CombinedTranslation {

  /** Creates a new instance of PostgreSQLTranslation */
  public PostgreSQLStandarization() {
    // Starting from 9.5, what before was:
    // i.grandtotal * (-1)::numeric
    // now is:
    // i.grandtotal * '-1'::integer::numeric
    // keeping old format
    append(new ReplacePatTranslation("'([0-9-]*)'::integer", "\\($1\\)"));

    // Starting from 9.5, some elements in group by are enclosed by brackets, they were not before,
    // so let's keep old format
    append(new Translation() {
      @Override
      public String exec(String s) {
        if (s.indexOf("GROUP BY") == -1) {
          return s;
        }
        String result = "";
        for (String line : s.split("\n")) {
          String modifiedLine;
          if (line.trim().startsWith("GROUP BY")) {
            modifiedLine = "";
            for (String groupByElement : line.split(",")) {
              String modifiedGroupByElement = groupByElement.trim();
              if (modifiedGroupByElement.startsWith("(") && modifiedGroupByElement.endsWith(")")) {
                modifiedGroupByElement = modifiedGroupByElement.substring(1,
                    modifiedGroupByElement.length() - 1);
              }
              modifiedLine += modifiedGroupByElement + ", ";
            }
            if (modifiedLine.endsWith(", ") && !line.trim().endsWith(",")) {
              modifiedLine = modifiedLine.substring(0, modifiedLine.length() - 2);
            }

          } else {
            modifiedLine = line;
          }
          result += modifiedLine + "\n";
        }
        return result;
      }
    });

    // postgres castings '::text', '::numeric', '::character varying', '::double precision'
    // '::date', '::bpchar', '::timestamp', '::\"unknown\"' , '::timestamp with time zone'
    // '::timestamp without time zone'
    append(new ReplacePatTranslation(
        "::[A-Za-z\"]*( varying)?( with time zone)?( without time zone)?( precision)?(\\[\\])?",
        ""));

    // sql "in" sentence and "not in"
    append(new ReplacePatTranslation(
        "=\\s*[Aa][Nn][Yy]\\s*\\(\\s*[Aa][Rr][Rr][Aa][Yy]\\s*\\[([^\\)]*)\\]\\s*\\)", "IN ($1)"));
    append(new ReplacePatTranslation(
        "<>\\s*[Aa][Ll][Ll]\\s*\\(\\s*[Aa][Rr][Rr][Aa][Yy]\\s*\\[([^\\)]*)\\]\\s*\\)",
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

    // blank spaces added in PostgreSQL 8.4
    append(new ByLineTranslation(new ReplacePatTranslation("^[ ]*(.*)", "$1")));
    append(new ReplacePatTranslation("\\([ ]*", "("));

    append(new ReplaceStrTranslation(" \n", " "));
    append(new ReplaceStrTranslation("\n ", " "));
    append(new ReplaceStrTranslation("\n", " "));
    // removes the caracter ";" at the end of sql sentence
    append(new ReplaceStrTranslation(";", ""));
    // append(new ByLineTranslation(new
    // ReplacePatTranslation("^[\\s]*(.*?)[\\s]*","$1")));
    append(new ReplaceStrTranslation("~~", "LIKE"));
  }
}
