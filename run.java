import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class run {

	public static void main(String[] args) {
		String start=null,end=null,output=null,format="json",query="";
		String ipaddr=null;
		List formats = Arrays.asList("csv", "text", "json");
		
		//Main m = new Main("192.168.47.1","2016.01.01", "2016.12.07", "C:\\Users\\mcapra\\Documents\\0NLS\\", format, "{\"query\":{\"query_string\":{\"query\":\"syslog\"}}}");
		
		if((args.length > 3) && (args.length < 7)) {
			//probably json+format here
			if(args.length > 5) {
				JsonParser jp = new JsonParser(); //from gson
				try {
			    	jp.parse(args[5]);
			    }
				catch (JsonSyntaxException e) {
					System.out.println("Malformed JSON received. Check your query's syntax!");
					return;
				}
				format = args[4];
			}
			//probably just a format here
			else if(args.length > 4) {
				format = args[4];
				//definitely just a format, not JSON
				if(formats.contains(format)) {
					//do something?
				}
				//whoops definitely not a format, maybe JSON?
				else {
					boolean json=false;
					JsonParser jp = new JsonParser();
					try {
						args[4] = args[4].replaceAll("^\"|\"$", "");
				    	jp.parse(args[4]);
				    	json=true;
				    }
					catch (JsonSyntaxException e) {
						System.out.println("Malformed JSON received. Check your query's syntax!");
						System.out.println(args[4]);
						return;
					}
					
					if(json) {
						format="json";
						query=args[4];
					}
					//nah definitely not JSON, get out of here!
					else {
						System.out.println("Invalid format. Acceptable: csv, text, json");
						return;
					}
				}
			}
			ipaddr = args[0];
			start = args[1];
			end = args[2];
			if(!ip(ipaddr)) {
				System.out.println("Malformed IP address found. Please check your first argument!");
				return;
			}
			else if(isValidDate(start)) {
				if(isValidDate(end)) {
					//check output path existence and writability
					if(Files.isWritable(new File(args[3]).toPath())) {
						//check for trailing slash
						if(isValidPath(args[3])) {
							output = args[3] + "nls-export-" + System.currentTimeMillis() + "/";
							System.out.println("Creating directory " + output + "");
							File dir = new File(output);
							
							if(dir.mkdir()) {
								Main m = new Main(ipaddr, start, end, output, format, query);
							}
							else {
								System.out.println("Unable to create directory in \"" + args[3] + "\". Check permissions!");
							}
							
						}
						else {
							System.out.println("Please use a trailing slash on your path! (/path/to/out/, C:\\path\\to\\out\\)");
						}
						
					}
					else {
						System.out.println("Cannot write to path \"" + output + "\". Please check the path.");
					}
				}
				else {
					System.out.println("Invalid end_date. Must be in yyyy.mm.dd format.");
				}
			}
			else {
				System.out.println("Invalid start_date. Must be in yyyy.mm.dd format.");
			}
			
		}
		else printUsage();
		
		
	}
	
	public static void printUsage() {
		System.out.println("Version - 1.1.0");
		System.out.println("Usage: java -jar nlsexport.jar host start_date end_date output_path [format] [query]");
		System.out.println("host - The hostname or ip address of the remote Elasticsearch machine. Your Elasticsearch API must be front-facing for this application to work.");
		System.out.println("start_date - The starting date of your data set in yyyy.mm.dd format.");
		System.out.println("end_date - The ending date of your data set in yyyy.mm.dd format.");
		System.out.println("output_path - The path that will contain your data. Many individual files are created, so it is recommended to use a dedicated path.");
		System.out.println("format - OPTIONAL (json default), the format you would like your data to be saved in. Acceptable options: csv, json.");
		System.out.println("query - OPTIONAL (no default), the Elasticsearch query you would like to use to filter your results.");
		System.out.println("------------------------------------------");
		System.out.println("Sample: java -jar nlsexport.jar 192.168.0.1 2015.01.01 2015.12.31 /home/juser/export_nls csv");
		System.out.println("----- Exports all data from the year 2015 into CSV files into the /home/juser/export_nls path.");
		System.out.println("Sample: java -jar nlsexport.jar localhost 2015.01.01 2015.12.31 /home/juser/export_nls '{\"query\":{\"query_string\":{\"query\":\"syslog\"}}}'");
		System.out.println("----- Exports all syslog entries (given the query) from the year 2015 into raw JSON (default) into the /home/juser/export_nls path.");
		
	}
	
	public static boolean isValidDate(String text) {
	    Pattern p = Pattern.compile("^\\d{4}\\.(0[1-9]|1[012])\\.(0[1-9]|[12][0-9]|3[01])$");
	    Matcher m = p.matcher(text);
	    return m.find();
	}
	
	public static boolean isValidPath(String text) {
		Pattern p = Pattern.compile("(^(?:[a-zA-Z]\\:|\\\\\\\\[\\w\\.]+\\\\[\\w.$]+)\\\\(?:[\\w]+\\\\)*\\w([\\w.])+\\\\$)|(^\\/.*\\/$)");
	    Matcher m = p.matcher(text);
	    return m.find();
	}
	
	public static boolean ip(String text) {
	    Pattern p = Pattern.compile("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");
	    Matcher m = p.matcher(text);
	    return m.find();
	}

}
