package org.keedio.flume.interceptor;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flume.interceptor.Interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a cacheable interceptor
 *
 * @see
 * <a href="http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html">Spring
 * Cache docs</a>
 */
public class HeaderEnrichmentInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(HeaderEnrichmentInterceptor.class);
    
    public static final String CSV_HEADERS_FILE = "csvHeadersFile";
    public static final String CSV_SEPARATOR_CHAR = "csvSeparatorChar";
    public static final String MATCH_HEADER_KEY = "matchHeaderKey";
    
    private Map<String,Map<String,String>> matchHeaderValuesMap;
    private String matchHeaderKey;
    

    public HeaderEnrichmentInterceptor(String matchHeaderKey, Map<String,Map<String,String>>  matchHeaderValuesMap) {
        this.matchHeaderValuesMap = matchHeaderValuesMap;
        this.matchHeaderKey = matchHeaderKey;
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
    	
        Map<String, String> headers = event.getHeaders();
        
        if (headers.containsKey(matchHeaderKey)){
        	if (matchHeaderValuesMap.containsKey(headers.get(matchHeaderKey))){
		        	Map<String, String> extraHeaderInfo = matchHeaderValuesMap.get(headers.get(matchHeaderKey));
		
		        	for (Map.Entry<String, String> entry : extraHeaderInfo.entrySet()){
		        		headers.put(entry.getKey(),entry.getValue());
		        	}
		            event.setHeaders(headers);
        	}
        }
                
        return event;
    }
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event e : events) {
            intercept(e);
        }
        return events;
    }



    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {

        private String storeFilesGeo, csvSeparatorChar, matchHeaderKey;
        private Map<String,Map<String,String>> matchHeaderValuesMap = new HashMap<String,Map<String,String>>();

        public Interceptor build() {
        	
            BufferedReader br = null;
            String line = "";
            String[] headerLineValues;

            try {

                br = new BufferedReader(new FileReader(storeFilesGeo));
                String headerLine;
                
                
                // Get header line from CSV file
                if ((headerLine = br.readLine()) != null){
                	headerLineValues = headerLine.split(csvSeparatorChar);
                	
                	// Iterate over the lines of CSV file
	                while ((line = br.readLine()) != null) {
	                    String[] csvLine = line.split(csvSeparatorChar);

	                    String matchHeaderValue = csvLine[0];
                    	
	                    Map<String,String> auxMap= new HashMap<String,String>();
	                    for (int i=1;i<headerLineValues.length;i++){	                    	
	                    	auxMap.put(headerLineValues[i], csvLine[i]);
	                    }
	                    
	                    matchHeaderValuesMap.put(matchHeaderValue,auxMap);
	                }
                }else{
                	throw new IOException("CSV file is empty!");
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            
            return new HeaderEnrichmentInterceptor(matchHeaderKey,matchHeaderValuesMap);
        }

        public void configure(Context context) {
        	storeFilesGeo = context.getString(CSV_HEADERS_FILE);
            if (storeFilesGeo == null || storeFilesGeo.trim().length() == 0) {
                throw new IllegalArgumentException("Missing parameter: " + CSV_HEADERS_FILE, null);
            }
            matchHeaderKey = context.getString(MATCH_HEADER_KEY);
            if (matchHeaderKey == null || matchHeaderKey.trim().length() == 0) {
                throw new IllegalArgumentException("Missing parameter: " + MATCH_HEADER_KEY, null);
            }
            csvSeparatorChar = context.getString(CSV_SEPARATOR_CHAR,",");
        }
    }
}
