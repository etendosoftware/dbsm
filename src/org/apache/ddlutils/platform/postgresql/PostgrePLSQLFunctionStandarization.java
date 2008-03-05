package org.apache.ddlutils.platform.postgresql;


import java.util.Hashtable;
import java.util.Vector;
import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.Translation;
import org.apache.ddlutils.model.*;

public class PostgrePLSQLFunctionStandarization extends PostgrePLSQLStandarization {
	
	
	public PostgrePLSQLFunctionStandarization(Database database, int numFunction)
	{
		super(database);
		

        append(new ReplacePatTranslation("([.|\\n]*)(.*)[\\s|\\n]*END[\\n|\\s|\\r]*$","$1$2\nEND "+database.getFunction(numFunction).getName()));
        
        Function f=database.getFunction(numFunction);
        String body=f.getBody();

        for(int i=0;i<patternsOutFunctions.size();i++)
        {
        	//if(body.contains(outFunctions.get(i)))
    			//append(patternsOutFunctions.get(outFunctions.get(i)));
    		append(patternsOutFunctions.get(i));
        }
	}

}
