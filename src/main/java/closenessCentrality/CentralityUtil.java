package closenessCentrality;

public class CentralityUtil {
	public static final String ZERO = "0";
	public static final String TAB_DELIM = "\t" ;
	public static final String NEWLINE = "\n" ;
	public static final String COMMA_DELIM = "," ;
	private static final String COMMA_STR = "comma" ;
	public static final String TAB_STR = "tab" ;
	public static final String COLON_STR = "colon";
	public static final String COLON_DELIM = ":";
	public static final String D2 = "D2";
	public static final String V2 = "V2";
	public static final String V1 = "V1";
	public static final String V3 = "V3";
	
	public static char checkDelim(String in){
		char delim = ' ';
	    if(COMMA_DELIM.equalsIgnoreCase(in) || COMMA_STR.equalsIgnoreCase(in)) {
	       	delim = ',';
	    }else if(TAB_DELIM.equalsIgnoreCase(in) || TAB_STR.equalsIgnoreCase(in)) {
	        	delim = '\t';
	    } else if(COLON_STR.equalsIgnoreCase(in) || COLON_DELIM.equalsIgnoreCase(in)){
        	delim = ':';
        }
	    return delim;
	}

	
	
}
