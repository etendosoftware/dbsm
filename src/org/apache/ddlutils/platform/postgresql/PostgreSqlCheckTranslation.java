/*
 ************************************************************************************
 * Copyright (C) 2001-2015 Openbravo S.L.U.
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
    // pg 9.3+ adds extra casting to character varying columns, let's remove them
    // example: translated check "PROCESSED IN ('Y', 'N')"
    // can be exported as "ANY ((ARRAY['Y'::character varying, 'N'::character varying])::text[]))"
    // or as "ANY (ARRAY[('Y'::character varying)::text, ('N'::character varying)::text]))"
    // this replacement covers 2nd case resulting in:
    // "ANY (ARRAY['M'::character varying, 'P'::character varying, 'T'::character varying]))"
    append(new ReplacePatTranslation("\\(([^\\(]*)(::character varying)\\)::text", "$1$2"));

    // replaces " = ANY" by " in"
    // in (ARRAY['M'::character varying, 'P'::character varying, 'T'::character varying]))
    append(new ReplaceStrTranslation(" = ANY", " in"));

    // removes ARRAY and brackets:
    // "in ('M'::character varying, 'P'::character varying, 'T'::character varying))"
    append(new ReplaceStrTranslation("ARRAY[", ""));
    append(new ReplaceStrTranslation("]", ""));

    // removes extra castings: "((type) in ('M', 'P', 'T'))"
    append(new ReplaceStrTranslation("::bpchar", ""));
    append(new ReplaceStrTranslation("::text", ""));
    append(new ReplaceStrTranslation("::character varying", ""));

    // removes pending bracket in case 1:
    // "((type) in (('M', 'P', 'T')[))" -> ((type) in (('M', 'P', 'T')))
    append(new ReplacePatTranslation("\\[", ""));

    // handles numeric casting
    append(new ReplacePatTranslation("([0-9\\.\\-]+?)::[Nn][Uu][Mm][Ee][Rr][Ii][Cc]", "$1"));

    append(new Translation() {
      @Override
      public String exec(String s) {
        return s.toUpperCase();
      }
    });
  }
}
