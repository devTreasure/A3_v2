package WC;

import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArrayUtility {
	
	public static Pattern DigitsInArray = Pattern.compile("\\d");
	
	public static  boolean hasSameValue(String input) {
	
		
		  ArrayList<Integer> data = parseIntArray(input);
		  Collections.sort(data);
		
		  if(data.size() <= 1) 
		  {
	 		return true;
		  }
		  else
		  {
			return data.get(0)==data.get(data.size()-1);
		  }
		
		
	}

	private static ArrayList<Integer> parseIntArray(String input) {
		
		ArrayList<Integer> data = new ArrayList<>();
		{
		
		
		
		if(input == null || input.trim().length()==0) {
			return new ArrayList<>();
		}
		
		Matcher matcher = DigitsInArray.matcher(input);
		while (matcher.find()) {
			String value = null;
	        try {
				value = matcher.group(0);
			
				if(value !=null &&  (! value.isEmpty() )   && (!value.equalsIgnoreCase("nan")))
				{
					data.add(Integer.parseInt(value));
				}
			} catch (NumberFormatException e) {
				System.out.println("Error: Can not convert string to integer:" + value);
			}
	    }
		}
		return data;
		
	
	}
	
	public static void main(String[] args) {
		System.out.println(ArrayUtility.hasSameValue("[]"));
		
		 ArrayList<Integer> a1= ArrayUtility.parseIntArray("[]");
		 
  		System.out.println(ArrayUtility.hasSameValue("[[1,2,3,4,5,67,]]"));
  		
  		ArrayList<Integer> a111= ArrayUtility.parseIntArray("[[1,2,3,4,5,67,]]");
  		
  		
  		for (Integer integer : a111) {
			System.out.println(integer);
		}
  		
		ArrayList<Integer> a11= ArrayUtility.parseIntArray("[]");
		 
		System.out.println(ArrayUtility.hasSameValue("[2]"));
		
		 ArrayList<Integer> a2= ArrayUtility.parseIntArray("[2]");
		 
		System.out.println(ArrayUtility.hasSameValue("[2, 2]"));
		 ArrayList<Integer> a3= ArrayUtility.parseIntArray("[2, 2]");
		 
		System.out.println(ArrayUtility.hasSameValue("[2, 2, 2]"));
		 ArrayList<Integer> a4= ArrayUtility.parseIntArray("[2, 2, 2]");
		 		
	//	System.out.println(ArrayUtility.has("[4, 3, 2, 2, 2]"));
		System.out.println(ArrayUtility.hasSameValue("[4, 3,nan, 2, 2, 2]"));
		
		ArrayList<Integer> a5= ArrayUtility.parseIntArray("[4, 3,nan, 2, 2, 2]");
		for (Integer integer : a5) {
			System.out.println(integer);
		}
	}

}
