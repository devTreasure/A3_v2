package WC;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtility {

	public static Pattern DigitsInString = Pattern.compile("\\d");
	
	public static boolean hasOnlyAlhabats(String input) {
		
		
		
		if(input == null || input.trim().length()==0) {
			return false;
		}
		
		if(input.contains("\\u") || input.contains("\\U")) {
			return false;
		}
		
		Matcher matcher = DigitsInString.matcher(input);
		
		return !matcher.find();
		
	}
	
	public static void main(String[] args) {
		
		System.out.println(StringUtility.hasOnlyAlhabats("abc"));
		System.out.println(StringUtility.hasOnlyAlhabats("70s"));
		System.out.println(StringUtility.hasOnlyAlhabats("wu-tang"));
		System.out.println(StringUtility.hasOnlyAlhabats("wu-tang"));
		System.out.println(StringUtility.hasOnlyAlhabats("\\u00e9ire"));
		System.out.println(StringUtility.hasOnlyAlhabats("hello1"));
		System.out.println(StringUtility.hasOnlyAlhabats("am\\u00e9ricain"));		
		System.out.println(StringUtility.hasOnlyAlhabats("1 13 165900 150 7672 22647 34612 48720 59280 74602 87545 95495 107182 131087 141522 153710"));
		
	}
}
