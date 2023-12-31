/*
 ************************************************************************************
 * Copyright (C) 2008-2018 Openbravo S.L.U.
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
import java.util.Vector;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.translation.ByLineTranslation;
import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.ReplaceOutFunctionParams;
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;

public class PostgrePLSQLStandarization extends CombinedTranslation {

  static Vector<String> outFunctions = new Vector<String>();
  static Vector<ByLineTranslation> patternsOutFunctions = new Vector<ByLineTranslation>();

  public PostgrePLSQLStandarization(Database database) {

    // Numeric Type
    append(new ReplaceStrTranslation(" NUMERIC,", " NUMBER,"));
    append(new ByLineTranslation(new ReplacePatTranslation(
        "(\\s|\\(|')+[Nn][Uu][Mm][Ee][Rr][Ii][Cc](\\s|\\t|:|,|\\|')+", "$1NUMBER$2")));
    append(new ReplaceStrTranslation(" NUMERIC(", " NUMBER("));
    append(new ReplaceStrTranslation(" NUMERIC)", " NUMBER)"));
    append(
        new ByLineTranslation(new ReplacePatTranslation("(\\s)+NUMERIC(\\s|\\t)*$", "$1NUMBER$2")));
    append(new ReplaceStrTranslation(" NUMERIC;", " NUMBER;"));
    append(new ReplaceStrTranslation(" NUMERIC:", " NUMBER:"));
    append(new ReplaceStrTranslation("'NUMERIC'", "'NUMBER'"));

    // Varchar Type
    append(new ByLineTranslation(new ReplacePatTranslation(
        "(.*)([\\s|\\t])VARCHAR(.*) --OBTG:([Nn]?[Vv][Aa][Rr][Cc][Hh][Aa][Rr]2?)--", "$1$2$4$3")));

    // TimeStamp Type
    append(new ReplaceStrTranslation(" TIMESTAMP,", " DATE,"));
    append(
        new ByLineTranslation(new ReplacePatTranslation("(\\s)+TIMESTAMP(\\s|\\t)+", "$1DATE$2")));
    append(
        new ByLineTranslation(new ReplacePatTranslation("(\\s)+TIMESTAMP(\\s|\\t)*$", "$1DATE$2")));
    append(new ReplaceStrTranslation("TO_DATE", "TO_DATE"));
    append(new ReplaceStrTranslation(" TIMESTAMP;", " DATE;"));
    append(new ReplaceStrTranslation("'TIMESTAMP'", "'DATE'"));

    // TEXT BLOBS!!!!!!!!!

    append(new ByLineTranslation(
        new ReplacePatTranslation("^([\\s\\t]*)GET DIAGNOSTICS (\\s|\\t)*(.+?)rowcount:=ROW_COUNT;",
            "$1$3rowcount:=SQL%ROWCOUNT;")));
    append(new ReplaceStrTranslation("-- COMMIT;", "COMMIT;"));
    append(new ReplaceStrTranslation("-- ROLLBACK;", "ROLLBACK;"));
    append(new ByLineTranslation(
        new ReplacePatTranslation(" --OBTG:SAVEPOINT(.*);--", "SAVEPOINT$1;")));

    append(new ReplaceStrTranslation("DATA_EXCEPTION", "NO_DATA_FOUND"));
    append(new ReplaceStrTranslation("INTERNAL_ERROR", "Not_Fully_Qualified"));
    append(new ReplaceStrTranslation("RAISE_EXCEPTION", "OB_exception"));

    append(new ReplaceStrTranslation("SQLSTATE", "SQLCODE"));
    append(new ByLineTranslation(
        new ReplacePatTranslation("RECORD(.*) --OBTG:(.*)--", "$2%ROWTYPE$1")));

    append(new ReplacePatTranslation("-- <<", "<<"));
    append(new ReplaceStrTranslation("REFCURSOR", "REF CURSOR"));
    append(new ReplacePatTranslation("(\\s|\\n)EXCEPT(\\s|\\n)", "$1MINUS$2"));
    append(new ReplacePatTranslation("TYPE_REF%TYPE;", "TYPE_REF;"));
    // append(new ReplaceStrTranslation("NOW()", "TO_DATE(NOW())"));
    append(new ReplaceStrTranslation("substract_days(NEW.Updated,NEW.Created)",
        "(NEW.Updated-NEW.Created)"));

    append(new ReplacePatTranslation("OPEN (.+?) FOR EXECUTE (.+?);", "OPEN$1FOR$2;"));
    append(new ByLineTranslation(
        new ReplacePatTranslation("^([^\\-]+)EXECUTE", "$1EXECUTE IMMEDIATE")));

    append(new ReplacePatTranslation("TYPE_Ref ", "TYPE TYPE_Ref IS "));
    append(new ReplacePatTranslation(
        "--([Tt][yY][Pp][Ee])(\\s*)([Rr][Ee][cC][oO][rR][dD])(\\s*)([iI][sS])", "$1$2$3$4$5"));

    append(new ReplacePatTranslation("INTEGER[10];", "ArrayPesos;"));
    append(new ReplacePatTranslation("Array\\(", "ArrayPesos("));
    append(new ReplacePatTranslation("VARCHAR[20];", "ArrayName;"));
    append(new ReplacePatTranslation("Array\\(", "ArrayName("));

    append(new ReplacePatTranslation("[Tt][Oo]_[Dd][Aa][Tt][Ee]\\([Nn][Oo][Ww]\\(\\)\\)", "now()"));

    for (int i = 0; i < database.getFunctionCount(); i++) {
      if (database.getFunction(i).getTypeCode() == Types.NULL) {
        append(new ReplacePatTranslation(
            "[Pp][Ee][Rr][Ff][Oo][Rr][Mm][\\s|\\t]*"
                + generateStringPat(database.getFunction(i).getName()) + "[\\s]*\\(",
            database.getFunction(i).getName() + "("));
      }
    }

    // Special functions created in pre-script
    append(new ReplacePatTranslation("(?i)perform[\\s|\\t]*AD_Enable_Triggers[\\s]*\\(",
        "AD_Enable_Triggers("));
    append(new ReplacePatTranslation("(?i)perform[\\s|\\t]*AD_Disable_Triggers[\\s]*\\(",
        "AD_Disable_Triggers("));

    // The next translations are the translations corresponding to the
    // ChangeFunction2Translation class

    append(new ByLineTranslation(new ReplaceStrTranslation("AS ' DECLARE", "AS")));
    // Here there should be the elimination of the clauses "ALIAS FOR"...
    // but as there shouldn't exist
    // we don't delete them yet.
    append(new ByLineTranslation(new ReplacePatTranslation(
        "RAISE NOTICE '%',(.*)([^\\s]+)([\\s|\\t]*);", "DBMS_OUTPUT.PUT_LINE($1$2)$3;")));
    append(new ByLineTranslation(
        new ReplacePatTranslation("RAISE EXCEPTION '%',(.*)([^\\s]+)([\\s|\\t]*); --OBTG:(.*)--",
            "RAISE_APPLICATION_ERROR($4,$1$2)$3;")));

    append(new ByLineTranslation(
        new ReplacePatTranslation("DECLARE (.*) CURSOR (.*) FOR", "CURSOR $1 $2IS")));
    append(new ByLineTranslation(new ReplacePatTranslation(
        "^(.+?)([\\s|\\t|\\(]+?) NOT FOUND (.+?) --OBTG:(.*)--", "$1$2$4%NOTFOUND$3")));

    append(new ByLineTranslation(new ReplacePatTranslation(
        "RAISE EXCEPTION '%','RBack'; --OBTG:tosavepoint(.*)", "ROLLBACK TO SAVEPOINT $1;")));
    append(new ByLineTranslation(
        new ReplacePatTranslation("RAISE EXCEPTION '%',\\s*SQLERRM\\s*;", "RAISE;")));
    append(new ByLineTranslation(
        new ReplacePatTranslation("RAISE EXCEPTION '%',\\s*'Rollback'\\s*;", "RAISE;")));
    append(
        new ByLineTranslation(new ReplacePatTranslation("RAISE EXCEPTION '%',\\s*'';", "RAISE;")));
    append(new ByLineTranslation(
        new ReplacePatTranslation("RAISE EXCEPTION '%',\\s*(.*);", "RAISE $1;")));

    append(new ByLineTranslation(new ReplacePatTranslation("--(.*) Exception;", "$1Exception;")));
    append(new ByLineTranslation(
        new ReplacePatTranslation("--(.*)\\sPRAGMA EXCEPTION_INIT", "$1PRAGMA EXCEPTION_INIT")));

    append(new ByLineTranslation(
        new ReplacePatTranslation("(.+?)Array\\[(.+?)\\](.+?)$", "$1Array($2)$3")));
    // append(new ReplaceStrTranslation("Array[","Array("));
    // append(new ReplaceStrTranslation("]",")"));

    append(new ByLineTranslation(new ReplacePatTranslation(
        "FOR UPDATE(\\(?)(;?) (.*)--OBTG:([^);\n]+?)--", "FOR UPDATE$4$1$2$3")));
    append(new ByLineTranslation(new ReplacePatTranslation(
        "^(.+?)([\\s|\\t|\\(]+?)([Nn][Ee][Xx][Tt][Vv][Aa][Ll])\\('([^\\s|\\t|\\(]+?)'\\)(.+?)$",
        "$1$2$4.$3$5")));
    append(new ReplacePatTranslation("RAISE '(.*)'", "RAISE NO_DATA_FOUND"));
  }

  private String generateStringPat(String in) {
    String out = "";
    for (int i = 0; i < in.length(); i++) {
      if (in.charAt(i) == '_') {
        out = out + "[" + in.charAt(i) + "]";
      } else {
        out = out + "[" + in.substring(i, i + 1).toLowerCase()
            + in.substring(i, i + 1).toUpperCase() + "]";
      }
    }
    return out;
  }

  public static void generateOutPatterns(Database database) {
    patternsOutFunctions = new Vector<>();
    for (int i = 0; i < database.getFunctionCount(); i++) {
      Function f = database.getFunction(i);
      boolean paramsOutExist = false;
      int defAct = f.getParameterCount();

      do {
        defAct--;
        String paramsOut = "";
        String paramsIn = "";
        int numParamsOut = 0;
        int numParamsIn = 0;
        int[] posParams = new int[f.getParameterCount()];
        for (int j = 0; j <= defAct; j++) {
          Parameter p = f.getParameter(j);
          if (p.getModeCode() == Parameter.MODE_OUT) {
            paramsOutExist = true;
            if (!paramsOut.equals("")) {
              paramsOut += ",";
            }
            paramsOut += "[\\s]*([^\\(\\s]+)[\\s]*";
            posParams[j] = numParamsOut;
            numParamsOut++;
          } else {
            if (!paramsIn.equals("")) {
              paramsIn += ",";
            }
            paramsIn += "\\s*(.+?)\\s*";// "[\\s]*([^\\s]+)[\\s]*";
            posParams[j] = numParamsIn;
            numParamsIn++;
          }
        }

        if (paramsOutExist) {
          String paramPos = "";
          for (int j = 0; j <= defAct; j++) {
            if (f.getParameter(j).getModeCode() == Parameter.MODE_IN) {
              posParams[j] += numParamsOut + 1;
            }
          }
          for (int j = 0; j <= defAct; j++) {
            if (j > 0) {
              paramPos += ", ";
            }
            paramPos += "$" + (posParams[j] + 1);
          }
          String nameRegExp = "";
          for (int ind = 0; ind < f.getName().length(); ind++) {
            nameRegExp += "[" + f.getName().substring(ind, ind + 1).toUpperCase()
                + f.getName().substring(ind, ind + 1).toLowerCase() + "]";
          }

          String patternIn = "(?i)SELECT\\s*\\*\\s*INTO" + paramsOut + "FROM\\s*(" + nameRegExp
              + ")\\s*\\(" + paramsIn + "\\)";
          String patternOut = "$" + (numParamsOut + 1) + "(" + paramPos + ")";
          if (!outFunctions.contains(f.getName())) {
            outFunctions.add(f.getName());
          }
          patternsOutFunctions.add(new ByLineTranslation(
              new ReplaceOutFunctionParams(patternIn, patternOut, f.getName())));

        }
      } while (defAct > 0 && f.getParameter(defAct).getDefaultValue() != null);
    }
  }

  public static String translateDefault(String defaultOrg) {
    String newDefault = defaultOrg.replaceAll("[Tt][Oo]_[Dd][Aa][Tt][Ee]\\([Nn][Oo][Ww]\\(\\)\\)",
        "now()");
    return newDefault;
  }
}
