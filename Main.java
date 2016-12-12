import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
	String sourceElasticURL = "http://localhost:9200/";
	String queryString="";
	ArrayList<String> scrollBuffer = new ArrayList<String>();
	int totalIndices=0;
	long totalDocuments=0;
	long scrollSize=0;
	
	public Main(String ip,String start, String end, String path, String format, String query) {
		dateStart = start;
		dateEnd = end;
		outputPath = path;
		outputFormat = format;
		queryString = query;
		sourceElasticURL = "http://" + ip + ":9200/";
		
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
		
		System.out.println("[" + System.currentTimeMillis() + "] " + event);
	}
	
	/**
	 * Do the dang thing
	 */
	public void doit() {
		ArrayList<String> indices = getIndicesBetweenDates(dateStart, dateEnd);

		//cycle through every index and do the needful
		for(String index : indices) {
			boolean wasClosed=false;
			
			//check if index exists
			if(doesIndexExist(index)) {
				totalIndices++;
				JsonElement state = getElasticAPI(sourceElasticURL, "_cluster/state/metadata/" + index + "?filter_path=metadata.indices.*.state")
						.getAsJsonObject("metadata")
						.getAsJsonObject("indices")
						.getAsJsonObject(index)
						.get("state");
				//check if index is open
				
				if(state.toString().equals("\"close\"")) {
					wasClosed=true;
					log("Index is not open [" + index + "], opening it temporarily.", 1);
					postElasticAPI(sourceElasticURL, "" + index + "/_open", "");
					log("Waiting for shards to be started for [" + index + "]. This may take a while.", 0);

					//Check if all 5 primary shards are started. If not, sleep with a backoff period to prevent inundating elasticsearch unneccesarily.
					int backoff=2000;
					while(!areShardsStarted("" + index)) {
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
						
				ArrayList<String> types = getTypesForIndex(index);
				
				//remove the _default_ type since it causes NPEs
				types.remove("_default_");
				
				for(String type : types) {
					log("Writing data from [" + index + "/" + type + "].",0);
					
					try{	
						String scrollId = startIndexScroll(index+"/"+type, 100, outputPath+index+"_"+type+".json", queryString);
						//Skip scroll if no results
						if(scrollSize > 0) {
							int count = 0;
							int num = (int)Math.ceil((scrollSize+100) / 100);
							ProgressBar progressBar = ConsoleProgressBar.on(System.out)
								    .withFormat("[:bar] :percent% :elapsed/:total    ETA: :eta")
								    .withTotalSteps(num);
							
							totalDocuments += scrollSize;
							if(outputFormat.equals("json")) {
								PrintWriter writer = new PrintWriter(outputPath+index+"_"+type+".json", "UTF-8");
								
								//process the first few results of the scroll object
								for(String s : scrollBuffer) {
									writer.println(s);
								}
								
								//process the rest of the scroll
								while(count < num) {
									progressBar.tickOne();
									JsonObject scroller = getScrollData(scrollId);
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
								
								JsonObject headers = getElasticAPI(sourceElasticURL, index+"/"+type+"/_mapping")
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
								
								for(String s : scrollBuffer) {
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
									JsonObject scroller = getScrollData(scrollId);
									JsonArray records = (JsonArray)scroller.getAsJsonObject("hits").get("hits");
									for(int i=0; i<records.size(); i++) {
										writer.println(jsonToCsv(headers, (JsonObject)((JsonObject)(records.get(i))).get("_source")));
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
				}
				
				
				
				log("Finished writing data from [" + index + "].",0);
				if(wasClosed) {
					log("Index [" + index + "] was previously closed. Closing it once again.",0);
					postElasticAPI(sourceElasticURL, "" + index + "/_close", "");
				}
			}
			else {
				log("Index not found [" + index + "], skipping.", 1);
			}
		}
		String plural = (totalIndices > 1) ? "indices" : "index";
		log("Completed export of " + totalIndices + " " + plural + " and " + totalDocuments + " documents.",0);
	}
	
	/**
	 * Issue a GET request to the elasticsearch API
	 * @param urlString The url of the elasticsearch API in String form
	 * @param request The API request we are issuing
	 * @return The response from the elasticsearch API request as a JsonObject
	 */
	public JsonObject getElasticAPI(String urlString, String request) {
	    // Connect to the URL using java's native library
	    JsonParser jp = new JsonParser(); //from gson
	    JsonElement root = null;
	    JsonObject rootobj = null;
	    URL url = null;
	    InputStreamReader input = null;
	    
	    
		try {
			url = new URL(urlString + request);
			//url = new URL("http://192.168.67.201:9200/" + indexName + "/_mapping");
			//request = (HttpURLConnection) url.openConnection();
			input = new InputStreamReader(url.openStream(), "UTF-8");
			root = jp.parse(input);
			rootobj = root.getAsJsonObject(); //May be an array, may be an object. 
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			System.out.println("[ERROR] Could not connect to host on [ip here]. The provided host/port information appears to be malformed.");
			System.out.println(url.toString());
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			System.out.println("[ERROR] Could not connect to host on [ip here]. Check your elasticsearch.yml configuration and any firewall rules blocking port 9200");
			e.printStackTrace();
			return null;
		}
		 catch (JsonSyntaxException e) {
			System.out.println("[ERROR] Malformed JSON received. Is this an elasticsearch server you are sending requests to?");
			System.out.println(input.toString());
			e.printStackTrace();
			return null;
		}
		
		
		return rootobj;
		
	}
	
	/**
	 * Issue a POST request to the elasticsearch API
	 * @param urlString The url of the elasticsearch API in String form
	 * @param request The API request we are issuing
	 * @param postData The POST data, as a JsonObject, we are sending to the elasticsearch API
	 * @return The response from the elasticsearch API request as a JsonObject
	 */
	public JsonObject postElasticAPI(String urlString, String request, String postData) {
		BufferedReader in;
		JsonParser parser = new JsonParser();
		JsonObject rootobj = null;
		URL obj = null;
		try {
			obj = new URL(urlString + request);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			String response = "";
			//add request header
			con.setRequestMethod("POST");
			con.setRequestProperty("User-Agent", "NLS Export");
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

			String urlParameters = postData;

			// Send post request
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();

			int responseCode = con.getResponseCode();
		
			in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				response += inputLine;
			}
			
			rootobj = parser.parse(response).getAsJsonObject();
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//return the response object
			log("IOException on: " + obj + " " + postData, 2);
		}
		
		return rootobj;
	}
	
	/**
	 * Makes a call to the elasticsearch API to gather the mapping information for a specific index
	 * @param indexName The name of the index we wish to fetch the mapping for
	 * @return The index's mapping as a JsonObject
	 */
	public JsonObject getIndexMapping(String indexName) {
	    JsonObject rootobj = getElasticAPI(sourceElasticURL, indexName + "/_mapping");

		return ((JsonObject)rootobj.get(indexName));
	}
	
	/**
	 * 
	 * @param indexName
	 * @return
	 */
	public long getIndexRecordCount(String indexName) {
		JsonObject rootobj = getElasticAPI(sourceElasticURL, indexName + "/_count");
		
		return Long.parseLong(rootobj.get("count").toString());
	}
	
	/**
	 * Gets the data from an elasticsearch scroll to be later processed into individual index records.
	 * @param scrollId The ID of our previously created scroll from startIndexScroll()
	 * @return The scroll object
	 */
	public JsonObject getScrollData(String scrollId) {
		String data = "";
		JsonObject rootobj = postElasticAPI(sourceElasticURL, "_search/scroll?scroll=1m", scrollId);
		
		return rootobj;
	}
	
	/**
	 * Check whether or not a given index exists
	 * @param index The name of the index
	 * @return true if the index exists, false if it does not
	 */
	public boolean doesIndexExist(String index) {
		URL obj = null;
		InputStreamReader input = null;
		try {
			obj = new URL(sourceElasticURL + index);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			String response = "";
			//add request header
			con.setRequestMethod("HEAD");
			con.setRequestProperty("User-Agent", "NLS Export");
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

			// Send post request
			con.setDoOutput(true);
			
			if(con.getResponseCode() == 200)
				return true;
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//return the response object
			log("IOException on: " + obj, 2);
			e.printStackTrace();
		}
		
		
		return false;
	}
	
	public boolean areShardsStarted(String index) {
		Pattern pattern = Pattern.compile("(p STARTED)");
		BufferedReader in;
		JsonParser parser = new JsonParser();
		JsonObject rootobj = null;
		URL obj = null;
		try {
			obj = new URL(sourceElasticURL + "_cat/shards/" + index);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			String response = "";
			//add request header
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent", "NLS Export");
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		
			in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				response += inputLine;
			}
			Matcher matcher = pattern.matcher(response);
		    int count = 0;
		    while (matcher.find()) {
		      count++;
		    }
		    
		    if(count >= 5)
		    	return true;
			
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//return the response object
			log("IOException on: " + obj, 2);
		}

		return false;
	}
	
	/**
	 * Starts an elasticsearch scroll to be used in data export
	 * @param indexName The name of the index we are pulling data from
	 * @param size The size of each scroller reference, typically 100 is a good size
	 * @return The scroller ID used in future elasticsearch API calls
	 */
	
	public String startIndexScroll(String indexName, long size, String fileName, String query) {
		scrollBuffer = new ArrayList<String>();
		//curl -XGET '192.168.67.201:9200/2016.10.11/_search?scroll=30m&size=5'
		String scrollId = null;
		JsonObject rootobj = postElasticAPI(sourceElasticURL, indexName + "/_search?scroll=1m&size=" + size,query);
		
		// do initial insertion for first scroller results
		// todo fix this somehow, it's redundant 
		JsonArray records = (JsonArray)rootobj.getAsJsonObject("hits").get("hits");
		log("Started scroll with " + rootobj.getAsJsonObject("hits").get("total") + " records.",0);
		scrollSize = Long.parseLong(rootobj.getAsJsonObject("hits").get("total").toString());
		for(int i=0; i<records.size(); i++) {
			String id = ((JsonObject)(records.get(i))).get("_id").toString();
			String type = ((JsonObject)(records.get(i))).get("_type").toString();
			String source = ((JsonObject)(records.get(i))).get("_source").toString();
			
			//strip quotes because JsonObject is weird
			id = id.replaceAll("^\"|\"$", "");
			type = type.replaceAll("^\"|\"$", "");
			source = source.replaceAll("^\"|\"$", "");

			// handle first 100 scroll results
			scrollBuffer.add(source);
		}
		
		scrollId = rootobj.get("_scroll_id").toString().replaceAll("^\"|\"$", "");
		
		return scrollId;
	}

	/**
	 * Get the days between a set of dates. Useful for grabbing all the yyyy.MM.dd indices between
	 * two dates.
	 * @param start The start date in yyyy.MM.dd format.
	 * @param end The end date in yyyy.MM.dd format.
	 * @return An ArrayList<String> with all of our dates between the start and end dates.
	 */
	public static ArrayList<String> getIndicesBetweenDates(String start, String end){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd");
		Date dateStart = null;
		Date dateEnd = null;
		
        try {

            dateStart = formatter.parse(start);
            dateEnd = formatter.parse(end);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        
        ArrayList<String> dates = new ArrayList<String>();
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(dateStart);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(dateEnd);

        while(cal1.before(cal2) || cal1.equals(cal2))
        {
            dates.add("logstash-" + formatter.format(cal1.getTime()).toString());
            cal1.add(Calendar.DATE, 1);
        }
        return dates;
    }
	
	/**
	 * do the thing
	 * @param i
	 * @return
	 */
	public ArrayList<String> getTypesForIndex(String i) {
		ArrayList<String> types = new ArrayList<String>();

		JsonObject rootobj = getElasticAPI(sourceElasticURL, i + "/_mapping").getAsJsonObject(i).getAsJsonObject("mappings");

		Set<Map.Entry<String, JsonElement>> entries = rootobj.entrySet();//will return members of your object
		for (Map.Entry<String, JsonElement> entry: entries) {
		    types.add(entry.getKey());
		}
		
		return types;
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
    				 out += stripReserved(elements.next().getAsString()) + " ";
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
	
	
}
