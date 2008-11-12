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

package org.apache.ddlutils.platform;

import java.util.HashMap;
import java.util.Vector;

/**
 *
 * @author adrian
 */
public class ExcludeFilter {
	Vector<String> prefixes = new Vector<String>();
    Vector<String> rejectedPrefixes = new Vector<String>();
    Vector<String> othersPrefixes = new Vector<String>();
    Vector<String> otherActivePrefixes = new Vector<String>();
    private HashMap<String, Vector<String>> prefixDependencies = new HashMap<String, Vector<String>>();
    String modName;
    static String externalPrefix = "EM";
    
    public String[] getExcludedTables() {
        return new String[0];
    }
    public String[] getExcludedViews() {
        return new String[0];
    }
    public String[] getExcludedSequences() {
        return new String[0];
    }
    public String[] getExcludedFunctions() {
        return new String[0];
    }
    public String[] getExcludedTriggers() {
        return new String[0];
    }   
    
    public void addPrefix(String prefix)
    {
      this.prefixes.add(prefix);
    }
    
    public void addOthersPrefixes(Vector<String> othersPrefixes)
    {
      this.othersPrefixes.addAll(othersPrefixes);
    }
    
    public void addOtherActivePrefixes(Vector<String> otherActivePrefixes)
    {
      this.otherActivePrefixes.addAll(otherActivePrefixes);
    }
    
    public boolean compliesWithNamingRule(String name)
    {
    	for(String prefix: prefixes)
    	{
    		if(hasPrefix(name, prefix))
    				return true;
    	}
    	return false;
    }
    
    private boolean hasPrefix(String name, String prefix)
    {
      return name.toUpperCase().startsWith(prefix.toUpperCase()+"_");
    }
    
    public boolean compliesWithOthersNamingRule(String name)
    {
      if(othersPrefixes==null || othersPrefixes.size()==0)
      {
        return false;
      }
      boolean b=false;
      int i=0;
      while(!b && i<othersPrefixes.size())
      {
        if(hasPrefix(name, othersPrefixes.get(i)))
          b=true;
        i++;
      }
      return b;
    }
    
    public boolean compliesWithOtherActiveNamingRule(String name)
    {
        if(otherActivePrefixes==null || otherActivePrefixes.size()==0)
        {
          return false;
        }
        boolean b=false;
        int i=0;
        while(!b && i<otherActivePrefixes.size())
        {
          if(hasPrefix(name, otherActivePrefixes.get(i)))
            b=true;
          i++;
        }
        return b;
    }
    
    public void addDependencies(HashMap<String, Vector<String>> map)
    {
    	prefixDependencies=map;
    }
    
    public boolean noPrefix()
    {
      return prefixes.size()==0;
    }
    
    public void setName(String name)
    {
    	this.modName=name;
    }
    
    public boolean isDependant(String name)
    {
    	for(String prefix:prefixDependencies.get(modName))
    	{
    		if(hasPrefix(name, prefix))
    				return true;
    	}
		return false;
    }
    
    public boolean compliesWithExternalPrefix(String name)
    {
    	return hasPrefix(name, externalPrefix);
    }
    
    public boolean compliesWithExternalNamingRule(String name)
    {
    	for(String prefix: prefixes)
    		if(hasPrefix(name, externalPrefix+"_"+prefix))
    				return true;
    	return false;
    }
}