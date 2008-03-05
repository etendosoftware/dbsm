
package org.apache.ddlutils.translation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CommentFilter{

	Hashtable<String, String> comments;
	int numLit=0;
	
	public CommentFilter()
	{
		comments=new Hashtable<String, String>();
		numLit=0;
	}
	
	public String removeComments(String body)
	{

        StringBuffer sb = new StringBuffer();
        BufferedReader br = new BufferedReader(new StringReader(body));

		Pattern pattLit = Pattern.compile("--([^\\n\\r-]*)[\\n|\\s]");
		Matcher matcher=pattLit.matcher(body);
		String firstPart="";
		numLit=0;
		while(matcher.find())
		{
			String code="##OBCO"+numLit;
			comments.put(code, matcher.group(1));
			firstPart+=body.substring(0, matcher.start())+"--"+code;
			body=body.substring(matcher.end()-1);
			numLit++;
			matcher=pattLit.matcher(body);
		}
		body=firstPart+body;
		
		//We restore literals we need for the translation process
        for(int j=numLit-1;j>=0;j--)
        {
        	String code="##OBCO"+j;
        	String rep=comments.get(code);
        	//System.out.println(code+";;"+rep);
        	if(rep.startsWith("OBTG:") || rep.contains("COMMIT") || rep.contains("ROLLBACK") || rep.contains("TYPE") || rep.contains("<<")
        			||rep.contains("EXCEPTION") || rep.contains("PRAGMA") || rep.contains("INTERNAL_ERROR"))
        		body=body.replaceFirst(code, rep);
        }
        
        return body;
	}
	
	public String restoreComments(String body)
	{

		//We finally restore literals
        for(int i=numLit-1;i>=0;i--)
        {
        	String code="##OBCO"+i;
        	String rep=comments.get(code);
        	body=body.replaceFirst(code, rep);
        }
        
        return body;
	}
	
	
	
}