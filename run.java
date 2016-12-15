import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class run {
	
	@Option(name="-host",usage="The host name of your NLS machine. Use \"localhost\" if running this application locally.")
    private String host;

    @Option(name="-date_start",usage="The start date for our date range. Must be in yyyy.mm.dd format.")
    private String date_start;

    @Option(name="-date_end",usage="The end date for our date range. Must be in yyyy.mm.dd format.")
    private String date_end;

    @Option(name="-output_path",usage="The path for our output files. Accepts Linux and Windows formats with trailing slash (/path/to/ or C:\\path\\to\\).")
    private String output_path;

    @Option(name="-output_format",usage="(Optional, Default: json) The format you would like your data to be saved in. Acceptable options: csv, json.")
    private String output_format = "json";
    
    @Option(name="-query",usage="(Optional, No Default) The Elasticsearch query you would like to use to filter your results.")
    private String query = "";
    
    // receives other command line parameters than options
    @Argument
    private static List<String> arguments = new ArrayList<String>();

	public static void main(String[] args) {
			try {
				new run().doMain(args);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//else printUsage();
	}
	
	public void doMain(String[] args) throws IOException {
		ParserProperties p = ParserProperties.defaults();
		CmdLineParser parser = new CmdLineParser(this, p.withOptionValueDelimiter("="));
		
		//used to better identify problematic CLI args
		String s = "";
		
        try {
        	//parser.parseArgument(args);
        	
        	for(int i=0; i<args.length; i++) {
        		s = args[i];
        		parser.parseArgument(args[i]);
        	}
        } 
        catch( CmdLineException e ) {
        	System.out.println(s);
            e.printStackTrace();
            return;
        }

        if((args.length > 3) && (args.length < 7)) {
        	if(host == null || date_start == null || date_end == null || output_path == null) {
        		System.out.println("Missing required parameter.");
        		printUsage();
        		return;
        	}
			if(isValidDate(date_start)) {
				if(isValidDate(date_end)) {
					//check output path existence and writability
					if(Files.isWritable(new File(output_path).toPath())) {
						//check for trailing slash
						if(isValidPath(output_path)) {
							output_path = output_path + "nls-export-" + System.currentTimeMillis() + "/";
							System.out.println("Creating directory " + output_path + "");
							File dir = new File(output_path);
							
							if(dir.mkdir()) {
								List formats = Arrays.asList("csv", "json");
								if(formats.contains(output_format)) {
									if(isValidJson(query)) {
										Main m = new Main(host, date_start, date_end, output_path, output_format, query);
									}
									else return;
								}
								else {
									System.out.println("Invalid format. Allowed formats: json, csv.");
								}
							}
							else {
								System.out.println("Unable to create directory in \"" + output_path + "\". Check permissions!");
							}
							
						}
						else {
							System.out.println("Please use a trailing slash on your path! (/path/to/out/, C:\\path\\to\\out\\)");
						}
						
					}
					else {
						System.out.println("Cannot write to path \"" + output_path + "\". Please check the path.");
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
		System.out.println("Version - 1.2.3");
		System.out.println("Usage: java -jar nlsexport.jar -host -date_start -date_end -output_path [-output_format] [-query]");
		System.out.println("host - The hostname or ip address of the remote Elasticsearch machine. Your Elasticsearch API must be front-facing for this application to work.");
		System.out.println("date_start - The starting date of your data set in yyyy.mm.dd format.");
		System.out.println("date_end - The ending date of your data set in yyyy.mm.dd format.");
		System.out.println("output_path - The path that will contain your data. Many individual files are created, so it is recommended to use a dedicated path.");
		System.out.println("output_format - OPTIONAL (json default), the format you would like your data to be saved in. Acceptable options: csv, json.");
		System.out.println("query - OPTIONAL (no default), the Elasticsearch query you would like to use to filter your results.");
		System.out.println("------------------------------------------");
		System.out.println("Sample: java -jar nlsexport.jar -host=192.168.0.1 -date_start=2015.01.01 -date_end=2015.12.31 -output_path=/home/juser/export_nls -output_format=csv");
		System.out.println("----- Exports all data from the year 2015 into CSV files into the /home/juser/export_nls path.");
		System.out.println("Sample: java -jar nlsexport.jar -host=localhost -date_start=2015.01.01 -date_end=2015.12.31 -output_path=/home/juser/export_nls -query='{\"query\":{\"query_string\":{\"query\":\"syslog\"}}}'");
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
	
	public boolean isValidJson(String text) {
		boolean json=false;
		JsonParser jp = new JsonParser();
		try {
			query = query.replaceAll("^\"|\"$", "");
	    	jp.parse(query);
	    	json=true;
	    }
		catch (JsonSyntaxException e) {
			System.out.println("Malformed JSON received. Check your query's syntax!");
			System.out.println(query);
			return false;
		}
		
		if(json) {
			return true;
		}
		
		return false;
	}

}
