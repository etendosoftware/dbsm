/*
 ************************************************************************************
 * Copyright (C) 2001-2019 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.postgresql;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;

/**
 * 
 * @author adrian
 */
public class PostgreSqlCheckTranslation extends CombinedTranslation {

  private static final Pattern STRING_LITERAL = Pattern.compile("\\'(.*?)\\'");

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

    append(this::capitalizeAllButStrings);
  }

  private String capitalizeAllButStrings(String s) {
    List<String> matchList = new ArrayList<>();
    Matcher regexMatcher = STRING_LITERAL.matcher(s);

    // TODO: from JDK 9 StringBuilder is supported in Matcher.appendReplacement
    StringBuffer sb = new StringBuffer();
    int idx = 0;
    while (regexMatcher.find()) {
      regexMatcher.appendReplacement(sb, "{" + idx + "}");
      matchList.add(regexMatcher.group());
      idx++;
    }
    regexMatcher.appendTail(sb);
    return MessageFormat.format(sb.toString().toUpperCase(), matchList.toArray());
  }
}
