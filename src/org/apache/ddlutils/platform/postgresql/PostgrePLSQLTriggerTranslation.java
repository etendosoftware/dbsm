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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.translation.ByLineTranslation;
import org.apache.ddlutils.translation.ReplacePatTranslation;
import org.apache.ddlutils.translation.ReplaceStrTranslation;
import org.apache.ddlutils.translation.Translation;

/**
 *
 * @author adrian
 */
public class PostgrePLSQLTriggerTranslation extends PostgrePLSQLTranslation {
    
    /** Creates a new instance of PostgrePLSQLTriggerTranslation */
    public PostgrePLSQLTriggerTranslation(Database database) {
        super(database);
        
        // Here goes the specific translations for triggers

        append(new ReplaceStrTranslation(":old.", "OLD."));
        append(new ReplaceStrTranslation(":new.", "NEW."));        
        append(new ReplacePatTranslation("RETURN;", "IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; "));
        append(new ReplacePatTranslation("INSERTING", "TG_OP = 'INSERT'"));
        append(new ReplacePatTranslation("UPDATING", "TG_OP = 'UPDATE'"));
        append(new ReplacePatTranslation("DELETING", "TG_OP = 'DELETE'"));
        append(new ByLineTranslation(new ReplacePatTranslation("^EXCEPTION", "IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; \n\rEXCEPTION")));
        
        //Add return for trigger
        append(new Translation() {
          public String exec(String s) {
              int i = s.lastIndexOf("END ");
              return i >= 0 ? s.substring(0, i)+"IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; \n\r"+s.substring(i,s.length())  : s;
          }
      });
    }
    
}
