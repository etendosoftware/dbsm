package org.apache.ddlutils.platform.postgresql;

import org.apache.ddlutils.model.*;
import org.apache.ddlutils.translation.ByLineTranslation;
import org.apache.ddlutils.translation.CombinedTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.Translation;

public class PostgrePLSQLTriggerStandarization extends PostgrePLSQLStandarization {

	public PostgrePLSQLTriggerStandarization(Database database, int numTrigger) {
		
		super(database);


		append(new ReplacePatTranslation("IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;([\\n|\\s|\\r]*)EXCEPTION","EXCEPTION"));
        append(new ReplacePatTranslation("IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;([\\s|\\t])[\\s|\\t|\\r]*\\n([.|\\r|\\s|\\n|\\t]*)END([.|\\s|\\n|\\r]*)$","$2END$1\n$3"));
        append(new ReplacePatTranslation("IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; ","RETURN;"));

 
        //append(new ByLineTranslation(new ReplaceStrTranslation( "IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; \n\rEXCEPTION","EXCEPTION")));

        append(new ReplaceStrTranslation("TG_OP = 'INSERT'","INSERTING"));
        append(new ReplaceStrTranslation("TG_OP = 'UPDATE'","UPDATING"));
        append(new ReplaceStrTranslation("TG_OP = 'DELETE'","DELETING"));
        append(new ReplacePatTranslation("([Oo][Ll][Dd])\\.",":$1."));
        append(new ReplacePatTranslation("([Nn][Ee][Ww])\\.",":$1."));   
        
        // Insert procedure name after last END
        //append(new ReplacePatTranslation("((.|\\n|\\p{Cntrl})*)","$1"+database.getTrigger(numTrigger).getName()));
        //append(new ReplacePatTranslation("END(\\([\\s|\\t]*)","END "));
        append(new ReplacePatTranslation("([.|\\n]*)(.*)([\\s|\\n]*)END([.|\\n|\\s|\\r|\\t]*)$","$1$2$3END "+database.getTrigger(numTrigger).getName()+"$4"));
        /*
        final Database database2=database;
        final int numTrigger2=numTrigger;http://www.marca.com/
        append(new Translation() {
        	Trigger trigger=database2.getTrigger(numTrigger2);
            public String exec(String s) {
                int i = s.lastIndexOf("END ");
                return i >= 0 ? s.substring(0, i + 4)+trigger.getName()  : s;
            }
        });*/
        

        Trigger f=database.getTrigger(numTrigger);
        String body=f.getBody();

        for(int i=0;i<patternsOutFunctions.size();i++)
        {
        	//if(body.contains(outFunctions.get(i)))
        		//append(patternsOutFunctions.get(outFunctions.get(i)));
        	append(patternsOutFunctions.get(i));
        }
	}

}
