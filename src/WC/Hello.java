package WC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;

public class Hello {
	
	public static void main (String[] args) 
	{
    	System.out.println(args);	
		Pattern regex = Pattern.compile("\\(.*?\\)|(,)");
		Pattern p = Pattern.compile("\\[([^\\]]*)\\]");
		

		
		String str ="[,s,]";
		Matcher match = regex.matcher(str);
		
				
		if (match.find()) {
			System.out.println(match.group());
		}
		
		
		String strOnlynum = "13 165900 150 7672 22647 34612 48720 59280 74602 87545 95495 107182 131087 141522 153710";
		String strMix ="00s";
		
		Pattern pattern = Pattern.compile("^[0-9a-zA-Z]+$");
		
		Matcher matcher = pattern.matcher(strOnlynum);
	
		System.out.println(matcher.matches());
		
		Matcher matcher2 = pattern.matcher(strMix);
		System.out.println(matcher2.matches());
		
		if (matcher.matches()) {
			
		}
		
		String s1="am\u00e9ricain";
		String s2="//am\u00e9rica.inb";
		
		String s3="rock";		
		
		if(s1.contains("\\"))
		{
			System.out.println("s1 contains \\");
		}
		
		if(s2.contains("/"))
		{
			System.out.println("s1 contains /");
		}
		
		
		for (int i = 0; i < s1.length(); i++) {
			 
			if (!Character.isUnicodeIdentifierPart(i) ) {
		              System.out.println("found");
		          }
		      }
		
		int index = 0;
		int largest = -1;
		
		int[] myArray = {4,4,1,3,1,3,4,4,5,6,67};
		
		int[] myArray1 = {1,1,1,1,1,1,1,1,1,1,1};
		
		for ( int i = 0; i < myArray.length; i++ )
		{
		    if ( myArray[i] > largest )
		    {
		        largest = myArray[i];
		        index = i;
		    }
		}
		
		if(largest==1 && myArray.length>0)
		{
			//
		}
		else
		{
			
		}
	
		
		System.out.println( largest);
		System.out.println( "--");
		System.out.println( myArray1[index]);

	
		
		ArrayList<Genere> hot = new ArrayList<Genere>();
		
		System.out.println("After sorting");
		hot.add(new Genere("pop", 2222));
		hot.add(new Genere("rock", 32));
		hot.add(new Genere("pop", 800));
		
	
		Collections.sort(hot,new Genere());
		
		for (Genere genere : hot) {
			System.out.println(genere.hotCount);
			
		}
		
	}
}
