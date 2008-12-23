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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ddlutils.translation.Translation;

/**
 * 
 * @author adrian
 */
public class ChangeFunction2Translation implements Translation {

    /** Creates a new instance of ChangeFunction2Translator */
    public ChangeFunction2Translation() {
    }

    public String exec(String s) {

        BufferedReader br = new BufferedReader(new StringReader(s));
        StringWriter bw = new StringWriter();

        String inputStr;
        int recCount = 0;
        int i = 1;
        int j = 1;
        String varname[] = { "", "", "", "", "", "", "", "", "", "", "", "",
                "", "", "", "", "", "", "", "", "", "", "", "", "" };

        try {
            while ((inputStr = br.readLine()) != null) {
                recCount++;

                // if (recCount%100==0)
                // System.out.println("line : " + recCount);

                String patternStr1 = "(.+?\\s)(IN)(\\s.+)";
                Pattern pattern1 = Pattern.compile(patternStr1);
                Matcher matcher1 = pattern1.matcher(inputStr);
                String patternStr2 = "AS '";
                Pattern pattern2 = Pattern.compile(patternStr2);
                Matcher matcher2 = pattern2.matcher(inputStr);
                String patternStr3 = "^$";
                Pattern pattern3 = Pattern.compile(patternStr3);
                Matcher matcher3 = pattern3.matcher(inputStr);
                String patternStr4 = "^/$";
                Pattern pattern4 = Pattern.compile(patternStr4);
                Matcher matcher4 = pattern4.matcher(inputStr);
                String patternStr5 = "(.+)(DBMS_OUTPUT.PUT_LINE)(\\s|\\t)*(\\()(.+?)(\\))(\\s|\\t)*;(.+)*$";
                Pattern pattern5 = Pattern.compile(patternStr5);
                Matcher matcher5 = pattern5.matcher(inputStr);
                String patternStr5a = "(.+)(DBMS_OUTPUT.PUT)(\\s|\\t)*(\\()(.+?)(\\))(\\s|\\t)*;(.+)*$";
                Pattern pattern5a = Pattern.compile(patternStr5a);
                Matcher matcher5a = pattern5a.matcher(inputStr);
                String patternStr6 = "(.+)CURSOR(\\s|\\t)([^\\s|\\t|\\(]+)([\\s|\\t|\\(])(.+)*(\\s|\\t)*(IS)";
                Pattern pattern6 = Pattern.compile(patternStr6);
                Matcher matcher6 = pattern6.matcher(inputStr);
                String patternStr7 = "(.+FOR(\\s|\\t))(.+)((\\s|\\t)IN(\\s|\\t))(.+)((\\s|\\t)LOOP)";
                Pattern pattern7 = Pattern.compile(patternStr7);
                Matcher matcher7 = pattern7.matcher(inputStr);

                String patternStr11 = "(?i)(.+?)(RAISE_APPLICATION_ERROR)(\\()(.+?)(,)(.+?)(\\))(\\s|\\t)*;(\\s|\\t)*$";
                Pattern pattern11 = Pattern.compile(patternStr11);
                Matcher matcher11 = pattern11.matcher(inputStr);

                String patternStr12 = "(?i)(.+?)(RAISE_APPLICATION_ERROR )(\\()(.+?)(,)(.+?)(\\))(\\s|\\t)*;(\\s|\\t)*$";
                Pattern pattern12 = Pattern.compile(patternStr12);
                Matcher matcher12 = pattern12.matcher(inputStr);

                // String patternStr14 =
                // "(.+?)([Aa][Dd]_[Ss][Ee][Qq][Uu][Ee][Nn][Cc][Ee]_[Dd][Oo][Cc])(\\s|\\t)*\\((.+?),(.+?),(.+?),(.+?)\\)";
                // Pattern pattern14 = Pattern.compile(patternStr14);
                // Matcher matcher14 = pattern14.matcher(inputStr);
                // String patternStr15 =
                // "(.+?)([Aa][Dd]_[Ss][Ee][Qq][Uu][Ee][Nn][Cc][Ee]_[Dd][Oo][Cc][Tt][Yy][Pp][Ee])(\\s|\\t)*\\((.+?),(.+?),(.+?),(.+?)\\)";
                // Pattern pattern15 = Pattern.compile(patternStr15);
                // Matcher matcher15 = pattern15.matcher(inputStr);
                // String patternStr16 =
                // "(.+?)([Aa][Dd]_[Ss][Ee][Qq][Uu][Ee][Nn][Cc][Ee]_[Nn][Ee][Xx][Tt])(\\s|\\t)*\\((.+?),(.+?),(.+?)\\)";
                // Pattern pattern16 = Pattern.compile(patternStr16);
                // Matcher matcher16 = pattern16.matcher(inputStr);
                // String patternStr17 =
                // "([Cc]_[Ii][Nn][Vv][Oo][Ii][Cc][Ee]_[Cc][Rr][Ee][Aa][Tt][Ee])(\\s|\\t)*\\((.+?),(.+?),(.+?)\\)";
                // Pattern pattern17 = Pattern.compile(patternStr17);
                // Matcher matcher17 = pattern17.matcher(inputStr);
                // String patternStr18 =
                // "([Cc]_[Vv][Aa][Ll][Ii][Dd]_[Cc][Oo][Mm][Bb]][Ii][Nn][Aa][Tt][Ii][Oo][Nn])(\\s|\\t)*\\((.+?),(.+?)\\)";
                // Pattern pattern18 = Pattern.compile(patternStr18);
                // Matcher matcher18 = pattern18.matcher(inputStr);
                // String patternStr19 =
                // "([Mm]_[Cc][Hh][Ee][Cc][Cc][Kk]_[Ss][Tt][Oo][Cc][Kk])(\\s|\\t)*\\((.+?),(.+?),(.+?),(.+?),(.+?)\\)";
                // Pattern pattern19 = Pattern.compile(patternStr19);
                // Matcher matcher19 = pattern19.matcher(inputStr);
                // String patternStr20 =
                // "([Mm]_[Ii][Nn][Oo][Uu][Tt]_[Cc][Rr][Ee][Aa][Tt][Ee])(\\s|\\t)*\\((.+?),(.+?),(.+?),(.+?),(.+?),(.+?)\\)";
                // Pattern pattern20 = Pattern.compile(patternStr20);
                // Matcher matcher20 = pattern20.matcher(inputStr);
                // String patternStr21 =
                // "([Mm][Aa]_[Gg][Ll][Oo][Bb][Aa][Ll][Uu][Ss][Ee]_[Dd][Ii][Ss][Tt][Rr][Ii][Bb][Uu][Tt][Ee])(\\s|\\t)*\\((.+?),(.+?),(.+?),(.+?)\\)";
                // Pattern pattern21 = Pattern.compile(patternStr21);
                // Matcher matcher21 = pattern21.matcher(inputStr);
                // String patternStr21a =
                // "([Mm][Aa]_[Ss][Tt][Aa][Nn][Dd][Aa][Rr][Dd]_[Cc][Oo][Ss][Tt]_[Ss][Ee][Qq][Uu][Ee][Nn][Cc][Ee])(\\s|\\t)*\\((.+?),(.+?),(.+?),(.+?),(.+?)\\)";
                // Pattern pattern21a = Pattern.compile(patternStr21a);
                // Matcher matcher21a = pattern21a.matcher(inputStr);

                // String patternStr22 =
                // "^(.+?)(\\s|\\t)*([^(\\s|\\t|\\()]+)%NOTFOUND(.+?)$";
                String patternStr22 = "^(.+?)([\\s|\\t|\\(]+?)([^\\s|\\t|\\(]+?)%NOTFOUND(.+?)$";
                Pattern pattern22 = Pattern.compile(patternStr22);
                Matcher matcher22 = pattern22.matcher(inputStr);

                String patternStr23 = "(.+?)(RAISE )(.+?);";
                Pattern pattern23 = Pattern.compile(patternStr23);
                Matcher matcher23 = pattern23.matcher(inputStr);
                String patternStr23a = "(.+?)(RAISE);";
                Pattern pattern23a = Pattern.compile(patternStr23a);
                Matcher matcher23a = pattern23a.matcher(inputStr);

                String patternStr24 = "(.+?)[Ee][Xx][Cc][Ee][Pp][Tt][Ii][Oo][Nn];(\\s|\\t)*$";
                Pattern pattern24 = Pattern.compile(patternStr24);
                Matcher matcher24 = pattern24.matcher(inputStr);

                String patternStr24a = "(.+?)PRAGMA EXCEPTION_INIT(.+?)$";
                Pattern pattern24a = Pattern.compile(patternStr24a);
                Matcher matcher24a = pattern24a.matcher(inputStr);

                String patternStr25 = "(.+?)(v_pesos|v_units|v_tens|v_hundreds|v_tenys|v_twentys)(\\()(.+?)(\\))([^\\)]+?)$";
                Pattern pattern25 = Pattern.compile(patternStr25);
                Matcher matcher25 = pattern25.matcher(inputStr);
                String patternStr26 = "(.+?)Array(\\()(.+?)(\\))(.+?)$";
                Pattern pattern26 = Pattern.compile(patternStr26);
                Matcher matcher26 = pattern26.matcher(inputStr);

                String patternStr33 = "^(.+?)FOR UPDATE([^);\n]+?)(\\(?)(;?)(\\s|\\t)*$";
                Pattern pattern33 = Pattern.compile(patternStr33);
                Matcher matcher33 = pattern33.matcher(inputStr);

                String patternStr34 = "^(.+?)ROLLBACK(.+?);$";
                Pattern pattern34 = Pattern.compile(patternStr34);
                Matcher matcher34 = pattern34.matcher(inputStr);

                String patternStr35 = "^(.+?)(SAVEPOINT(.+?);(\\s|\\t)*)$";
                Pattern pattern35 = Pattern.compile(patternStr35);
                Matcher matcher35 = pattern35.matcher(inputStr);

                String patternStr36 = "^(.+?)([\\s|\\t|\\(]+?)([^\\s|\\t|\\(]+?)\\.([Nn][Ee][Xx][Tt][Vv][Aa][Ll])(.+?)$";
                Pattern pattern36 = Pattern.compile(patternStr36);
                Matcher matcher36 = pattern36.matcher(inputStr);

                String patternStr37 = "(.*)ROLLBACK TO SAVEPOINT (.*);";
                Pattern pattern37 = Pattern.compile(patternStr37);
                Matcher matcher37 = pattern37.matcher(inputStr);

                if (matcher1.find()) {
                    String patternStr1a = "\\sFOR\\s";
                    Pattern pattern1a = Pattern.compile(patternStr1a);
                    Matcher matcher1a = pattern1a.matcher(matcher1.group(1));
                    String patternStr1b = "\\sIF\\s";
                    Pattern pattern1b = Pattern.compile(patternStr1b);
                    Matcher matcher1b = pattern1b.matcher(matcher1.group(1));
                    String patternStr1c = "\\sELSIF\\s";
                    Pattern pattern1c = Pattern.compile(patternStr1c);
                    Matcher matcher1c = pattern1c.matcher(matcher1.group(1));
                    String patternStr1d = "\\sWHERE\\s";
                    Pattern pattern1d = Pattern.compile(patternStr1d);
                    Matcher matcher1d = pattern1d.matcher(matcher1.group(1));

                    if (matcher1b.find()) {
                        bw.write(inputStr);
                    } else if (matcher1c.find()) {
                        bw.write(inputStr);
                    } else if (matcher1d.find()) {
                        bw.write(inputStr);
                    } else if (matcher1a.find()) {
                        if (matcher7.find()) {
                            bw.write(inputStr);
                        } else {
                            bw.write(inputStr);
                        }
                    } else {
                        bw.write(inputStr);
                    }
                } else if (matcher2.find()) {
                    bw.write("AS ' DECLARE");
                    bw.write("\n");
                    for (i = 1; i < j; i++) {
                        bw.write(varname[i] + "ALIAS FOR $" + i + ";");
                        bw.write("\n");
                    }
                    i = 1;
                    j = 1;
                } else if (matcher3.find()) {
                } else if (matcher4.find()) {
                    bw.write("");
                } else if (matcher37.find()) {
                    bw.write(matcher37.group(1)
                            + "RAISE EXCEPTION '%','RBack'; --OBTG:tosavepoint"
                            + matcher37.group(2));
                } else if (matcher5.find()) { // System.out.println(5);
                    String patternStr5_1 = "'";
                    Pattern pattern5_1 = Pattern.compile(patternStr5_1);
                    Matcher matcher5_1 = pattern5_1.matcher(matcher5.group(5));

                    bw.write(matcher5.group(1)
                            + "RAISE NOTICE '%',"
                            + matcher5_1.replaceAll("'")
                            + (matcher5.group(7) != null ? matcher5.group(7)
                                    : "")
                            + ";"
                            + (matcher5.group(8) != null ? matcher5.group(8)
                                    : ""));
                } else if (matcher5a.find()) { // System.out.println(5);
                    String patternStr5a1 = "'";
                    Pattern pattern5a1 = Pattern.compile(patternStr5a1);
                    Matcher matcher5a1 = pattern5a1.matcher(matcher5a.group(5));

                    bw.write(matcher5a.group(1)
                            + "RAISE NOTICE '%',"
                            + matcher5a1.replaceAll("'")
                            + (matcher5a.group(7) != null ? matcher5a.group(7)
                                    : "")
                            + ";"
                            + (matcher5a.group(8) != null ? matcher5a.group(8)
                                    : ""));
                } else if (matcher11.find()) {

                    String patternStr11a = "'";
                    String code = matcher11.group(4);
                    Pattern pattern11a = Pattern.compile(patternStr11a);
                    Matcher matcher11a = pattern11a.matcher(matcher11.group(6));

                    bw.write(matcher11.group(1)
                            + "RAISE EXCEPTION '%',"
                            + matcher11a.replaceAll("'")
                            + (matcher11.group(8) != null ? matcher11.group(8)
                                    : "") + ";" + " --OBTG:" + code + "--");

                } else if (matcher12.find()) {

                    String patternStr12a = "'";
                    String code = matcher12.group(4);
                    Pattern pattern12a = Pattern.compile(patternStr12a);
                    Matcher matcher12a = pattern12a.matcher(matcher12.group(6));

                    bw.write(matcher12.group(1)
                            + "RAISE EXCEPTION '%',"
                            + matcher12a.replaceAll("'")
                            + (matcher12.group(8) != null ? matcher12.group(8)
                                    : "") + ";" + " --OBTG:" + code + "--");

                } else if (matcher6.find()) {
                    bw.write(matcher6.group(1)
                            + "DECLARE "
                            + matcher6.group(3)
                            + " CURSOR "
                            + (matcher6.group(5) != null ? matcher6.group(5)
                                    : "") + " FOR");
                    // } else if (matcher14.find()) {
                    // bw.write(matcher14.group(1) + matcher14.group(7) +
                    // ":= "+matcher14.group(2)+"("+matcher14.group(4)+","+matcher14.group(5)+","+matcher14.group(6)+");");
                    // } else if (matcher15.find()) {
                    // bw.write(matcher15.group(1) + matcher15.group(7) +
                    // ":= "+matcher15.group(2)+"("+matcher15.group(4)+","+matcher15.group(5)+","+matcher15.group(6)+");");
                    // } else if (matcher16.find()) {
                    // bw.write(matcher16.group(1) + matcher16.group(6) +
                    // ":= "+matcher16.group(2)+"("+matcher16.group(4)+","+matcher16.group(5)+");");
                    // } else if (matcher17.find()) {
                    // bw.write(matcher17.group(5) +
                    // ":= "+matcher17.group(1)+"("+matcher17.group(3)+","+matcher17.group(4)+");");
                    // } else if (matcher18.find()) {
                    // bw.write(matcher18.group(3) +
                    // ":= "+matcher18.group(1)+"("+matcher18.group(4)+");");
                    // } else if (matcher19.find()) {
                    // bw.write("SELECT * INTO "+matcher19.group(6) + ","+
                    // matcher19.group(7)
                    // +" FROM "+matcher19.group(1)+"("+matcher19.group(3)+","+matcher19.group(4)+","+matcher19.group(5)+");");
                    // } else if (matcher20.find()) {
                    // bw.write(matcher20.group(7) +
                    // ":= "+matcher20.group(1)+"("+matcher20.group(3)+","+matcher20.group(4)+","+matcher20.group(5)+","+matcher20.group(6)+","+matcher20.group(8)+");");
                    // } else if (matcher21.find()) {
                    // bw.write("SELECT * INTO "+matcher21.group(5) + ","+
                    // matcher21.group(6)
                    // +" FROM "+matcher21.group(1)+"("+matcher21.group(3)+","+matcher21.group(4)+");");
                    // } else if (matcher21a.find()) {
                    // bw.write("SELECT * INTO "+matcher21a.group(7)
                    // +" FROM "+matcher21a.group(1)+"("+matcher21a.group(3)+","+matcher21a.group(4)+","+matcher21a.group(5)+","+matcher21a.group(6)+");");

                } else if (matcher22.find()) {
                    bw.write(matcher22.group(1) + matcher22.group(2)
                            + " NOT FOUND " + matcher22.group(4) + " --OBTG:"
                            + matcher22.group(3) + "--");
                } else if (matcher23.find()) {// System.out.println(23);
                    bw.write(matcher23.group(1) + "RAISE EXCEPTION '%',"
                            + matcher23.group(3) + ";");
                } else if (matcher23a.find()) {// System.out.println(23a);
                    bw.write(matcher23a.group(1)
                            + "RAISE EXCEPTION '%', SQLERRM;");
                } else if (matcher24.find()) {
                    bw.write("--" + matcher24.group(1) + " Exception;");
                } else if (matcher24a.find()) {// System.out.println(24);
                    bw.write("--" + matcher24a.group(1)
                            + " PRAGMA EXCEPTION_INIT" + matcher24a.group(2));
                } else if (matcher25.find()) {
                    bw.write(matcher25.group(1) + matcher25.group(2) + "["
                            + matcher25.group(4) + "]" + matcher25.group(6));
                } else if (matcher26.find()) {
                    bw.write(matcher26.group(1) + "Array[" + matcher26.group(3)
                            + "]" + matcher26.group(5));
                } else if (matcher33.find()) {// System.out.println(matcher33.group(1)+" - "+matcher33.group(4));
                    bw.write(matcher33.group(1) + "FOR UPDATE"
                            + matcher33.group(3) + matcher33.group(4)
                            + " --OBTG:" + matcher33.group(2) + "--");
                } else if (matcher34.find()) {// System.out.println(matcher34.group(1)+" - "+matcher34.group(4));
                    bw.write(matcher34.group(1)
                            + "RAISE EXCEPTION '%','Rollback';");
                } else if (matcher35.find()) {// System.out.println(matcher35.group(1)+" - "+matcher35.group(4));
                    bw.write(matcher35.group(1) + " --OBTG:"
                            + matcher35.group(2) + "--");
                } else if (matcher36.find()) {// System.out.println(36);System.out.println("1:"+matcher36.group(1)+"2:"+matcher36.group(2)+"3:"+matcher36.group(3)+"4:"+matcher36.group(4)+"5:"+matcher36.group(5));
                    bw.write(matcher36.group(1) + matcher36.group(2)
                            + matcher36.group(4) + "('" + matcher36.group(3)
                            + "')" + matcher36.group(5));
                } else {
                    bw.write(inputStr);
                }
                bw.write("\n");

            }
            // System.out.println(i);
            bw.close();

        } catch (IOException e) {
        }

        return bw.toString();
    }

}
