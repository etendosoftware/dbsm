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

package org.openbravo.ddlutils.task;

import org.apache.ddlutils.platform.ExcludeFilter;

/**
 *
 * @author adrian
 */
public class OpenbravoExcludeFilter extends ExcludeFilter {
    
    public String[] getExcludedTables() {
        return new String[] {
            "PLAN_TABLE", 
            "AD_SYSTEM_MODEL" };
    }
    
    public String[] getExcludedViews() {
        return new String[] {
            "DUAL", 
            "USER_CONS_COLUMNS", 
            "USER_TABLES", 
            "USER_CONSTRAINTS", 
            "USER_INDEXES", 
            "USER_IND_COLUMNS", 
            "USER_OBJECTS",
            "USER_TAB_COLUMNS", 
            "USER_TRIGGERS" };
    }

    public String[] getExcludedFunctions() {
        return new String[] {
            "EXIST_LANGUAGE",
            "INSERT_PG_LANGUAGE",
            "CREATE_LANGUAGE",
            "DATEFORMAT",
            "TO_NUMBER",
            "TO_DATE",
            "TO_TIMESTAMP",
            "TO_CHAR",
            "ROUND",
            "RPAD",
            "SUBSTR",
            "TO_INTERVAL",
            "ADD_MONTHS",
            "ADD_DAYS",
            "TYPE_OID",
            "SUBSTRACT_DAYS",
            "TRUNC",
            "INSTR",
            "LAST_DAY",
            "IS_TRIGGER_ENABLED",
            "DROP_VIEW",
            
            "AD_SCRIPT_DISABLE_TRIGGERS",
            "AD_SCRIPT_DISABLE_CONSTRAINTS",
            "AD_SCRIPT_ENABLE_TRIGGERS",
            "AD_SCRIPT_ENABLE_CONSTRAINTS",
            "AD_SCRIPT_DROP_RECREATE_INDEXES",
            "AD_SCRIPT_EXECUTE",
            "DBA_GETATTNUMPOS",
            "DBA_GETSTANDARD_SEARCH_TEXT",
            "DUMP",
            "NEGATION"};
    }

}
