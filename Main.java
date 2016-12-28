import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import hu.ssh.progressbar.ProgressBar;
import hu.ssh.progressbar.console.ConsoleProgressBar;

public class Main {

	String dateStart="",dateEnd="";
	String outputPath="",outputFormat="";
	String typesString="";
	ElasticSearch elasticsearch;
	int totalIndices=0;
	long totalDocuments=0;
	long startTime=0;
	
	public Main(String ip,String start, String end, String path, String format, String query, String types) {
		elasticsearch = new ElasticSearch(ip, query);
		dateStart = start;
		dateEnd = end;
		outputPath = path;
		outputFormat = format;
		typesString = types;
		
		doit();
	}
	
	/**
	 * Logs an event to the system log
	 * @param level The level of the event we are logging
	 */
	public void log(String event, int level) {
		String levelString; //unused currently
		
		switch (level) {
			case 0:  levelString = "";
	        	break;
	        case 1:  levelString = "<WARNING>";
	        	break;
	        case 2:  levelString = "<CRITICAL>";
        		break;
	        default: levelString = "<UNKNOWN>";
	        	break;
		}
		
		System.out.println("[" + System.currentTimeMillis() + "] " + levelString + " " + event);
	}
	
	/**
	 * Do the dang thing
	 */
	public void doit() {
		startTime = System.currentTimeMillis();
		ArrayList<String> indices = elasticsearch.getIndicesBetweenDates(dateStart, dateEnd);

		//cycle through every index and do the needful
		for(String index : indices) {
			boolean wasClosed=false;
			
			//check if index exists
			if(elasticsearch.doesIndexExist(index)) {
				totalIndices++;
				JsonElement state = elasticsearch.getElasticAPI("_cluster/state/metadata/" + index + "?filter_path=metadata.indices.*.state")
						.getAsJsonObject("metadata")
						.getAsJsonObject("indices")
						.getAsJsonObject(index)
						.get("state");
				
				//check if index is open
				if(state.toString().equals("\"close\"")) {
					wasClosed=true;
					log("Index is not open [" + index + "], opening it temporarily.", 0);
					elasticsearch.postElasticAPI("" + index + "/_open", "");
					log("Waiting for shards to be started for [" + index + "]. This may take a while.", 0);

					//Check if all 5 primary shards are started. If not, sleep with a backoff period to prevent inundating elasticsearch unneccesarily.
					int backoff=2000;
					while(!elasticsearch.areShardsStarted("" + index)) {
						if(backoff > 30000) {
							log("Hmm, assigning primary shards is taking a pretty long time. You might want to check on elasticsearch!",1);
						}
							
						try {
							Thread.sleep((long)(backoff += (backoff * 0.5)));
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					log("Primary shards for [" + index + "] appear to be started.", 0);
				}
				
				//Assess the types used, either from the provided arg or by asking Elasticsearch
				ArrayList<String> types = new ArrayList<String>();
				if(!typesString.equals("")) {
					types.addAll(Arrays.asList(typesString.toLowerCase().split("\\s+")));
				}
				else {
					types = elasticsearch.getTypesForIndex(index);
				}
				
				//remove the _default_ type since it causes NPEs
				types.remove("_default_");
				
				//Cycle through the types and get data from them
				for(String type : types) {
					if(isValidType(type)) {
						log("Writing data from [" + index + "/" + type + "].",0);
						
						try{	
							String scrollId = elasticsearch.startIndexScroll(index+"/"+type, 100, outputPath+index+"_"+type+".json");
							//Skip scroll if no results
							if(elasticsearch.scrollSize > 0) {
								int count = 0;
								int num = (int)Math.ceil((elasticsearch.scrollSize+100) / 100);
								ProgressBar progressBar = ConsoleProgressBar.on(System.out)
									    .withFormat("[:bar] :percent% :elapsed/:total    ETA: :eta")
									    .withTotalSteps(num);
								
								totalDocuments += elasticsearch.scrollSize;
								if(outputFormat.equals("json")) {
									PrintWriter writer = new PrintWriter(outputPath+index+"_"+type+".json", "UTF-8");
									
									//process the first few results of the scroll object
									for(String s : elasticsearch.scrollBuffer) {
										writer.println(s);
									}
									
									//process the rest of the scroll
									while(count < num) {
										progressBar.tickOne();
										JsonObject scroller = elasticsearch.getScrollData(scrollId);
										JsonArray records = (JsonArray)scroller.getAsJsonObject("hits").get("hits");
										for(int i=0; i<records.size(); i++) {
											String source = ((JsonObject)(records.get(i))).get("_source").toString();
											writer.println(source);
										}
										count++;;
									}
									System.out.println("");
									writer.close();
									
								}
								else if(outputFormat.equals("csv")) {
									PrintWriter writer = new PrintWriter(outputPath+index+"_"+type+".csv", "UTF-8");
									
									JsonObject headers = elasticsearch.getElasticAPI(index+"/"+type+"/_mapping")
											.getAsJsonObject(index)
											.getAsJsonObject("mappings")
											.getAsJsonObject(type)
											.getAsJsonObject("properties");
									
									String headerOut = "";
									
									for (Map.Entry<String,JsonElement> entry : headers.entrySet()) {
									    headerOut +=entry.getKey() + ",";
									}
									//remove last comma
									headerOut = headerOut.substring(0,headerOut.length()-1);
									writer.println(headerOut);
									
									for(String s : elasticsearch.scrollBuffer) {
										JsonParser jp = new JsonParser();
										try {
											s = s.replaceAll("^\"|\"$", "");
											writer.println(jsonToCsv(headers, (JsonObject)jp.parse(s)));
									    }
										catch (JsonSyntaxException e) {
											System.out.println("Malformed JSON received. Check your query's syntax!");
										}
									}
									
									//process the rest of the scroll
									while(count < num) {
										progressBar.tickOne();
										JsonObject scroller = elasticsearch.getScrollData(scrollId);
										JsonArray records = (JsonArray)scroller.getAsJsonObject("hits").get("hits");
										for(int i=0; i<records.size(); i++) {
											writer.println(jsonToCsv(headers, (JsonObject)((JsonObject)(records.get(i))).get("_source")));
										}
										count++;;
									}
									System.out.println("");
									writer.close();
								}
								else if(outputFormat.equals("text")) {
									PrintWriter writer = new PrintWriter(outputPath+index+"_"+type+".txt", "UTF-8");
									JsonParser jp = new JsonParser();
									
									//process the first few results of the scroll object
									for(String s : elasticsearch.scrollBuffer) {
										//this is kinda dumb, JsonObject -> String -> JsonObject. Saves re-writing how scrollBuffer works though
										try {
											String source = ((JsonObject)jp.parse(s)).get("message").toString();
											writer.println(source.substring(1, source.length()-1));
										}
										catch(NullPointerException e) {
											// element is null, skip
										}
										
									}
									
									//process the rest of the scroll
									while(count < num) {
										progressBar.tickOne();
										JsonObject scroller = elasticsearch.getScrollData(scrollId);
										JsonArray records = (JsonArray)scroller.getAsJsonObject("hits").get("hits");
										for(int i=0; i<records.size(); i++) {
											//use substring because JsonElement's start/end with "
											try {
												String source = ((JsonObject)(records.get(i))).getAsJsonObject("_source").get("message").toString();
												writer.println(source.substring(1, source.length()-1));
											}
											catch(NullPointerException e) {
												// element is null, skip
											}
										}
										count++;;
									}
									System.out.println("");
									writer.close();
								}
							
							}
							else {
								log("No results found in [" + index + "/" + type + "]." + " Skipping it.",0);
							}	

							
						}
						catch (IOException e) {
							e.printStackTrace();
						}
					} //isValidType
					else {
						log("Malformed type found: " + type + ", skipping.",1);
					}
					
				}
				
				
				
				log("Finished writing data from [" + index + "].",0);
				if(wasClosed) {
					log("Index [" + index + "] was previously closed. Closing it once again.",0);
					elasticsearch.postElasticAPI("" + index + "/_close", "");
				}
			}
			else {
				log("Index not found [" + index + "], skipping.", 1);
			}
		}
		String plural = (totalIndices > 1) ? "indices" : "index";
		log("Completed export of " + totalIndices + " " + plural + " and " + totalDocuments + " documents, took " + ((System.currentTimeMillis() - startTime)/1000) + " seconds.",0);
	}
	
	/**
	 * Convert a JsonObject to a CSV entry based on the given haeders
	 * @param headers The headers of our CSV file, basically every field that exists for the given index/type
	 * @param source The source JsonObject we will be converting to CSV
	 * @return The CSV entry as a String
	 */
	public String jsonToCsv(JsonObject headers, JsonObject source) {
		String out = "";
		
		for (Map.Entry<String,JsonElement> entry : headers.entrySet()) {
			try {
				if(source.get(entry.getKey()) == null) {
			    	out+=",";
			    }
				else {
					out+="\"" + stripReserved(source.get(entry.getKey()).getAsString()) + "\",";
				}
			}
			// entry is an actual json null, not just empty. todo handle this gracefully
			catch(UnsupportedOperationException e) {
					out += ",";
			}
			// JsonArray, treat it real special like
			catch(IllegalStateException e) {
    			Iterator<JsonElement> elements = ((JsonArray)source.get(entry.getKey())).iterator();
    			out += "\"";
    			while(elements.hasNext()) {
    				out += stripReserved(elements.next().toString()) + " ";
    			}
    			out += "\",";
    		}
		    
		}
		//remove last comma
		out = out.substring(0,out.length()-1);

		return unEscapeString(out);
	}
	
	/**
	 * This method helps keep everything on one nice neat line.
	 * @param s The String we are processing
	 * @return The processed String without newlines, tabs, or carriage returns
	 */
	public static String unEscapeString(String s) {
	    StringBuilder sb = new StringBuilder();
	    for (int i=0; i<s.length(); i++)
	        switch (s.charAt(i)){
	            case '\n': sb.append("\\n"); break;
	            case '\t': sb.append("\\t"); break;
	            case '\r': sb.append("\\r"); break;
	            // ... rest of escape characters
	            default: sb.append(s.charAt(i));
	        }
	    return sb.toString();
	}
	
	/**
	 * Strip out characters that are reserved by CSV. Easier than working around them for most readers.
	 * @param s The String we are processing
	 * @return The processed String without reserved CSV characters
	 */
	public static String stripReserved(String s) {
		StringBuilder sb = new StringBuilder();
	    for (int i=0; i<s.length(); i++)
	        switch (s.charAt(i)){
	            case ',': sb.append(""); break;
	            case '\"': sb.append(""); break;
	            // ... rest of escape characters
	            default: sb.append(s.charAt(i));
	        }
	    return sb.toString();
	}
	
	public static boolean isValidType(String s) {
		Pattern p = Pattern.compile("[\"<>#%\\{\\}\\|\\\\^~\\[\\];/?:@=& ]");
	    Matcher m = p.matcher(s);
	    return !m.find();
	}
	
	
}
