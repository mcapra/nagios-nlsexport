import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class ElasticSearch {
	String sourceElasticURL = "http://localhost:9200/";
	String queryString="";
	ArrayList<String> scrollBuffer = new ArrayList<String>();
	long scrollSize=0;
	
	public ElasticSearch(String i, String q) {
		sourceElasticURL = "http://" + i + ":9200/";
		queryString = q;
	}
	
	/**
	 * Issue a GET request to the elasticsearch API
	 * @param urlString The url of the elasticsearch API in String form
	 * @param request The API request we are issuing
	 * @return The response from the elasticsearch API request as a JsonObject
	 */
	public JsonObject getElasticAPI(String request) {
	    // Connect to the URL using java's native library
	    JsonParser jp = new JsonParser(); //from gson
	    JsonElement root = null;
	    JsonObject rootobj = null;
	    URL url = null;
	    InputStreamReader input = null;
	    
	    
		try {
			url = new URL(sourceElasticURL + request);
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
	public JsonObject postElasticAPI(String request, String postData) {
		BufferedReader in;
		JsonParser parser = new JsonParser();
		JsonObject rootobj = null;
		URL obj = null;
		try {
			obj = new URL(sourceElasticURL + request);
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
		}
		
		return rootobj;
	}
	

	
	/**
	 * Makes a call to the elasticsearch API to gather the mapping information for a specific index
	 * @param indexName The name of the index we wish to fetch the mapping for
	 * @return The index's mapping as a JsonObject
	 */
	public JsonObject getIndexMapping(String indexName) {
	    JsonObject rootobj = getElasticAPI(indexName + "/_mapping");

		return ((JsonObject)rootobj.get(indexName));
	}
	
	/**
	 * 
	 * @param indexName
	 * @return
	 */
	public long getIndexRecordCount(String indexName) {
		JsonObject rootobj = getElasticAPI(indexName + "/_count");
		
		return Long.parseLong(rootobj.get("count").toString());
	}
	
	/**
	 * Gets the data from an elasticsearch scroll to be later processed into individual index records.
	 * @param scrollId The ID of our previously created scroll from startIndexScroll()
	 * @return The scroll object
	 */
	public JsonObject getScrollData(String scrollId) {
		String data = "";
		JsonObject rootobj = postElasticAPI("_search/scroll?scroll=1m", scrollId);
		
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
			e.printStackTrace();
		}

		return false;
	}
	
	/**
	 * Starts an elasticsearch scroll to be used in data export
	 * @param indexName The name of the index we are pulling data from
	 * @param size The size of each scroller reference, typically 100 is a good size
	 * @return The scroller ID used in future elasticsearch API calls
	 */
	
	public String startIndexScroll(String indexName, long size, String fileName) {
		scrollBuffer = new ArrayList<String>();
		//curl -XGET '192.168.67.201:9200/2016.10.11/_search?scroll=30m&size=5'
		String scrollId = null;
		JsonObject rootobj = postElasticAPI(indexName + "/_search?scroll=1m&size=" + size,queryString);
		
		// do initial insertion for first scroller results
		// todo fix this somehow, it's redundant 
		JsonArray records = (JsonArray)rootobj.getAsJsonObject("hits").get("hits");
		//log("Started scroll with " + rootobj.getAsJsonObject("hits").get("total") + " records.",0);
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
		
		// todo re-wind after getting scroll id and avoid the use of scrollBuffer completely
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
	public ArrayList<String> getIndicesBetweenDates(String start, String end){
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

		JsonObject rootobj = getElasticAPI(i + "/_mapping").getAsJsonObject(i).getAsJsonObject("mappings");

		Set<Map.Entry<String, JsonElement>> entries = rootobj.entrySet();//will return members of your object
		for (Map.Entry<String, JsonElement> entry: entries) {
		    types.add(entry.getKey());
		}
		
		return types;
	}
	
	

}
