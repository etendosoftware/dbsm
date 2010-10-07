package org.apache.ddlutils.platform.postgresql;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.translation.ReplacePatTranslation;

public class PostgrePLSQLFunctionStandarization extends PostgrePLSQLStandarization {

  public PostgrePLSQLFunctionStandarization(Database database, int numFunction) {
    super(database);

    append(new ReplacePatTranslation("([.|\\n]*)(.*)[\\s|\\n]*END[\\n|\\s|\\r]*$", "$1$2\nEND "
        + database.getFunction(numFunction).getName()));

    for (int i = 0; i < patternsOutFunctions.size(); i++) {
      // if(body.contains(outFunctions.get(i)))
      // append(patternsOutFunctions.get(outFunctions.get(i)));
      append(patternsOutFunctions.get(i));
    }
  }

}
