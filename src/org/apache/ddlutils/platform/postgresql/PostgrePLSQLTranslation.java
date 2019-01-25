/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.postgresql;

import java.sql.Types;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.translation.ByLineTranslation;
import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;
import org.apache.ddlutils.translation.Translation;

/**
 * 
 * @author adrian
 */
public class PostgrePLSQLTranslation extends CombinedTranslation {

  /** Creates a new instance of ProcedureTranslation */
  public PostgrePLSQLTranslation(Database database) {
    append(new ReplaceStrTranslation(" RAISE NO_DATA_FOUND", " RAISE 'NO_DATA_FOUND'"));

    // Here goes the common translations for triggers and functions
    // append(............
    // Numeric Type
    append(new ReplaceStrTranslation(" NUMBER,", " NUMERIC,"));
    append(new ByLineTranslation(
        new ReplacePatTranslation("(\\s)+[Nn][Uu][Mm][Bb][Ee][Rr](\\s|\\t)+", "$1NUMERIC$2")));
    append(new ReplaceStrTranslation(" NUMBER(", " NUMERIC("));
    append(new ReplaceStrTranslation(" NUMBER)", " NUMERIC)"));
    append(new ByLineTranslation(
        new ReplacePatTranslation("(\\s)+[Nn][Uu][Mm][Bb][Ee][Rr](\\s|\\t)*$", "$1NUMERIC$2")));
    append(new ReplaceStrTranslation(" NUMBER;", " NUMERIC;"));
    append(new ReplaceStrTranslation(" NUMBER:", " NUMERIC:"));
    append(new ReplaceStrTranslation("'NUMBER'", "'NUMERIC'"));

    // Varchar Type
    append(new ByLineTranslation(new ReplacePatTranslation(
        "(.*\\s)([Nn][Vv][Aa][Rr][Cc][Hh][Aa][Rr]2?)(.*)", "$1VARCHAR$3 --OBTG:$2--")));
    append(new ByLineTranslation(new ReplacePatTranslation(
        "(.*\\s)([Vv][Aa][Rr][Cc][Hh][Aa][Rr]2)(.*)", "$1VARCHAR$3 --OBTG:$2--")));
    append(new ReplaceStrTranslation(" BYTE)", ")"));
    // append(new ReplacePatTranslation("'VARCHAR2'", "'VARCHAR'"));

    // Now() function
    append(new ReplacePatTranslation("[Ss][Yy][Ss][Dd][Aa][Tt][Ee]", "now()"));
    /*
     * append(new ReplaceStrTranslation("SYSDATE,", "now(),")); append(new
     * ReplaceStrTranslation(" SYSDATE ", " now() ")); append(new ReplaceStrTranslation("SYSDATE$",
     * "now()")); append(new ReplaceStrTranslation("(SYSDATE", "(now()")); append(new
     * ReplaceStrTranslation(" SYSDATE;", " now();")); append(new ReplaceStrTranslation("=SYSDATE",
     * "=now()")); append(new ReplaceStrTranslation("<SYSDATE", "<now()"));
     */

    // TimeStamp Type
    append(new ReplaceStrTranslation(" DATE,", " TIMESTAMP,"));
    append(new ByLineTranslation(
        new ReplacePatTranslation("(\\s)+[Dd][Aa][Tt][Ee](\\s|\\t)+", "$1TIMESTAMP$2")));
    append(new ByLineTranslation(
        new ReplacePatTranslation("(\\s)+[Dd][Aa][Tt][Ee](\\s|\\t)*$", "$1TIMESTAMP$2")));
    append(new ReplaceStrTranslation("TO_DATE", "TO_DATE"));
    append(new ReplaceStrTranslation(" DATE;", " TIMESTAMP;"));
    append(new ReplaceStrTranslation("'DATE'", "'TIMESTAMP'"));

    // Text Type
    append(new ReplaceStrTranslation("LONG,", "TEXT,"));
    append(new ReplaceStrTranslation(" LONG ", " TEXT "));
    append(new ReplaceStrTranslation("LONG$", "TEXT"));
    append(new ReplaceStrTranslation("CLOB,", "TEXT,"));
    append(new ReplaceStrTranslation(" CLOB ", " TEXT "));
    append(new ReplaceStrTranslation("CLOB$", "TEXT"));
    append(new ReplaceStrTranslation("BLOB,", "OID,"));
    append(new ReplaceStrTranslation(" BLOB ", " OID "));
    append(new ReplaceStrTranslation("BLOB$", "OID"));

    // append(new ReplaceStrTranslation("'", "''"));
    // append(new ByLineTranslation(new
    // ReplacePatTranslation("^(AS|IS)([\\s|\\t]*)$", "AS '")));
    append(new ByLineTranslation(
        new ReplacePatTranslation("^([\\s\\t]*)(.+?)rowcount(\\s|\\t)*:=(\\s|\\t)*SQL%ROWCOUNT;",
            "$1GET DIAGNOSTICS $3$2rowcount:=ROW_COUNT;")));
    // append(new RemoveTranslation("COMMIT;"));
    // append(new RemoveTranslation("ROLLBACK;"));

    append(new ReplaceStrTranslation("COMMIT;", "-- COMMIT;"));
    append(new ReplaceStrTranslation("ROLLBACK;", "-- ROLLBACK;"));

    append(new ReplaceStrTranslation("EXECUTE IMMEDIATE", "EXECUTE"));

    append(new ReplaceStrTranslation("NO_DATA_FOUND", "DATA_EXCEPTION"));
    append(new ReplaceStrTranslation("Not_Fully_Qualified", "INTERNAL_ERROR"));
    append(new ReplaceStrTranslation("OB_exception", "RAISE_EXCEPTION"));

    append(new ReplaceStrTranslation("SQLCODE", "SQLSTATE"));
    // append(new ReplacePatTranslation("TYPE RECORD", "-- TYPE RECORD"));
    // append(new ReplaceStrTranslation("TYPE invalid_tab",
    // "-- TYPE invalid_tab"));
    // append(new ReplaceStrTranslation("TYPE ArrayName",
    // "-- TYPE ArrayName"));
    // append(new ReplaceStrTranslation("TYPE ArrayPesos",
    // "-- TYPE ArrayPesos"));

    append(new ByLineTranslation(new ReplacePatTranslation(
        "^(.+?)([\\s|\\t|\\(]+?)([^\\s|\\t|\\(]+?)%[Rr][Oo][Ww][Tt][Yy][Pp][Ee](.*)",
        "$1$2RECORD$4 --OBTG:$3--")));

    append(new ReplacePatTranslation("<<", "-- <<"));
    append(new ReplaceStrTranslation("REF CURSOR", "REFCURSOR"));
    append(new ReplaceStrTranslation("MINUS", "EXCEPT"));
    append(new ByLineTranslation(new ReplacePatTranslation(
        "[Tt][Yy][Pp][Ee]_[Rr][Ee][Ff](\\s|\\t)*;(\\s|\\t)*", "TYPE_REF%TYPE;")));
    append(new ReplaceStrTranslation("NOW()", "TO_DATE(NOW())"));
    append(new ReplacePatTranslation("\\(NEW.Updated-NEW.Created\\)",
        "substract_days\\(NEW.Updated,NEW.Created\\)"));

    append(new ReplacePatTranslation("OPEN(.+?)FOR(.+?);", "OPEN $1 FOR EXECUTE $2;"));

    append(new ReplacePatTranslation("TYPE TYPE_Ref IS ", "TYPE_Ref "));
    append(new ReplacePatTranslation(
        "([Tt][yY][Pp][Ee])(\\s*)([Rr][Ee][cC][oO][rR][dD])(\\s*)([iI][sS])", "--$1$2$3$4$5"));

    append(new ReplacePatTranslation("ArrayPesos;", "INTEGER[10];"));
    append(new ReplacePatTranslation("ArrayPesos\\(", "Array("));
    append(new ReplacePatTranslation("ArrayName;", "VARCHAR[20];"));
    append(new ReplacePatTranslation("ArrayName\\(", "Array("));

    // Procedures with output parameters... and Perform
    for (int i = 0; i < database.getFunctionCount(); i++) {
      if (database.getFunction(i).hasOutputParameters()) {
        appendFunctionWithOutputTranslation(database.getFunction(i));
      } else {
        // Perform
        if (database.getFunction(i).getTypeCode() == Types.NULL) {
          append(new ReplaceStrTranslation(" " + database.getFunction(i).getName() + " *(",
              " PERFORM " + database.getFunction(i).getName() + "("));
        }
      }
    }

    // Special functions created in pre-script
    append(new ReplacePatTranslation("(?i)AD_Enable_Triggers[\\s]*\\(",
        "PERFORM AD_Enable_Triggers("));
    append(new ReplacePatTranslation("(?i)AD_Disable_Triggers[\\s]*\\(",
        "PERFORM AD_Disable_Triggers("));

    // Miscellaneous translations
    append(new ChangeFunction2Translation());

    // Remove procedure name after last END
    append(new Translation() {
      @Override
      public String exec(String s) {
        int i = s.lastIndexOf("END ");
        return i >= 0 ? s.substring(0, i + 4) : s;
      }
    });
  }

  private void appendFunctionWithOutputTranslation(Function func) {

    try {
      Function f = (Function) func.clone();

      // First the recursive call
      Parameter lastparam = f.getParameter(f.getParameterCount() - 1);
      if (lastparam.getDefaultValue() != null && !lastparam.getDefaultValue().equals("")) {
        f.removeParameter(f.getParameterCount() - 1);
        appendFunctionWithOutputTranslation(f);
      }

      // Last the function translation
      append(new FunctionWithOutputTranslation(func));

    } catch (CloneNotSupportedException e) {
      // Will not happen
    }
  }

}
