package org.apache.ddlutils.translation;

import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LiteralFilter {

  Hashtable<String, String> literals;
  int numLit = 0;

  public LiteralFilter() {
    literals = new Hashtable<String, String>();
    numLit = 0;
  }

  public String removeLiterals(String body) {

    Pattern pattLit = Pattern.compile("'([^'\\n]*?)'");
    Matcher matcher = pattLit.matcher(body);
    String firstPart = "";
    numLit = 0;
    while (matcher.find()) {
      String code = "##OBTGC" + numLit;
      literals.put(code, matcher.group(1));
      firstPart += body.substring(0, matcher.start()) + "'" + code + "'";
      body = body.substring(matcher.end());
      numLit++;
      matcher = pattLit.matcher(body);
    }
    body = firstPart + body;

    // We restore literals we need for the translation process
    for (int j = numLit - 1; j >= 0; j--) {
      String code = "##OBTGC" + j;
      String rep = literals.get(code);
      if (rep.equals("") || rep.equals("%") || rep.equals("RBack") || rep.equals("UPDATE")
          || rep.equals("DELETE") || rep.equals("INSERT"))
        body = body.replaceFirst(code, rep);
    }

    return body;
  }

  public String restoreLiterals(String body) {

    // We finally restore literals
    for (int i = numLit - 1; i >= 0; i--) {
      String code = "##OBTGC" + i;
      String rep = literals.get(code);
      body = body.replaceFirst(code, rep);
    }

    return body;
  }

}