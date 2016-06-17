/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package esiptestbed.mudrod.weblog.pre;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.index.IndexRequest;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;


public class ImportLogFile extends DiscoveryStepAbstract{

	public ImportLogFile(Map<String, String> config, ESDriver es, SparkDriver spark, String time_suffix) {
		super(config, es, spark);
		this.time_suffix = time_suffix;
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		System.out.println("*****************Import starts******************");
		startTime=System.currentTimeMillis();
		readFile();
		endTime=System.currentTimeMillis();
		System.out.println("*****************Import ends******************Took " + (endTime-startTime)/1000+"s");
		es.refreshIndex();
		return null;
	}
	
	String time_suffix = null;

	String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";

	public static final int NUM_FIELDS = 9;
	Pattern p = Pattern.compile(logEntryPattern);
	Matcher matcher;
	
	public String SwithtoNum(String time){
		if (time.contains("Jan")){
			time = time.replace("Jan", "1");   
		}else if (time.contains("Feb")){
			time = time.replace("Feb", "2");   
		}else if (time.contains("Mar")){
			time = time.replace("Mar", "3");   
		}else if (time.contains("Apr")){
			time = time.replace("Apr", "4");   
		}else if (time.contains("May")){
			time = time.replace("May", "5");   
		}else if (time.contains("Jun")){
			time = time.replace("Jun", "6");   
		}else if (time.contains("Jul")){
			time = time.replace("Jul", "7");   
		}else if (time.contains("Aug")){
			time = time.replace("Aug", "8");   
		}else if (time.contains("Sep")){
			time = time.replace("Sep", "9");   
		}else if (time.contains("Oct")){
			time = time.replace("Oct", "10");   
		}else if (time.contains("Nov")){
			time = time.replace("Nov", "11");
		}else if (time.contains("Dec")){
			time = time.replace("Dec", "12");
		}

		return time;
	}
	
	public void readFile(){
		es.createBulkProcesser();
		
		//String time_suffix = config.get("httplogpath").substring(Math.max(config.get("httplogpath").length() - 2, 0));
		this.HTTP_type += time_suffix;
		this.FTP_type += time_suffix;
		this.Cleanup_type += time_suffix;
		this.SessionStats += time_suffix;
		
		/*config.put("HTTP_type", this.HTTP_type);
		config.put("FTP_type", this.FTP_type);
		config.put("Cleanup_type", this.Cleanup_type);
		config.put("SessionStats", this.SessionStats);*/
		
		String httplogpath = config.get("logDir") + config.get("httpPrefix") + time_suffix + "/" + config.get("httpPrefix") + time_suffix;
		String ftplogpath = config.get("logDir") + config.get("ftpPrefix") + time_suffix + "/" + config.get("ftpPrefix") + time_suffix;
		
		try {
			ReadLogFile(httplogpath, "http", config.get("indexName"), this.HTTP_type);
			ReadLogFile(ftplogpath, "FTP", config.get("indexName"), this.FTP_type);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		es.destroyBulkProcessor();
		
	}
	
	public void ReadLogFile(String fileName, String Type, String index, String type) throws ParseException, IOException, InterruptedException{
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		int count =0;
		try {
			String line = br.readLine();
		    while (line != null) {	
		    	if(Type.equals("FTP"))
		    	{
		    		ParseSingleLineFTP(line, index, type);
		    	}else{
		    		ParseSingleLineHTTP(line, index, type);
		    	}
		    	
		    	line = br.readLine();
		    	count++;
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
		    br.close();
		    
		    System.out.println("Num of " + Type + ": " + count);
		}
	}

	public void ParseSingleLineFTP(String log, String index, String type) throws IOException, ParseException{
		String ip = log.split(" +")[6];

		String time = log.split(" +")[1] + ":"+log.split(" +")[2] +":"+log.split(" +")[3]+":"+log.split(" +")[4];

		time = SwithtoNum(time);
		SimpleDateFormat formatter = new SimpleDateFormat("MM:dd:HH:mm:ss:yyyy");
		Date date = formatter.parse(time);
		String bytes = log.split(" +")[7];
		
		String request = log.split(" +")[8].toLowerCase();
		
		if(!request.contains("/misc/") && !request.contains("readme"))
		{
		IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder()
				.startObject()
				.field("LogType", "ftp")
				.field("IP", ip)
				.field("Time", date)
				.field("Request", request)
				.field("Bytes", Long.parseLong(bytes))
				.endObject());

		es.bulkProcessor.add(ir);
		}

	}

	public void ParseSingleLineHTTP(String log, String index, String type) throws IOException, ParseException{
		matcher = p.matcher(log);
		if (!matcher.matches() || 
				NUM_FIELDS != matcher.groupCount()) {
			//bw.write(log+"\n");
			return;
		}
		String time = matcher.group(4);
		time = SwithtoNum(time);
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
		Date date = formatter.parse(time);

		String bytes = matcher.group(7);
		if(bytes.equals("-")){
			bytes="0";
		}

		String request = matcher.group(5).toLowerCase();
		String agent = matcher.group(9);
		CrawlerDetection crawlerDe = new CrawlerDetection(this.config, this.es, this.spark);
		if(crawlerDe.CheckKnownCrawler(agent))
		{

		}
		else
		{
			if(request.contains(".js")||request.contains(".css")||request.contains(".jpg")||request.contains(".png")||request.contains(".ico")||
					request.contains("image_captcha")||request.contains("autocomplete")||request.contains(".gif")||
					request.contains("/alldata/")||request.contains("/api/")||request.equals("get / http/1.1")||
					request.contains(".jpeg")||request.contains("/ws/"))   //request.contains("/ws/")  need to be discussed
			{

			}else{
				IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder()
						.startObject()
						.field("LogType", "PO.DAAC")
						.field("IP", matcher.group(1))
						.field("Time", date)
						.field("Request", matcher.group(5))
						.field("Response", matcher.group(6))
						.field("Bytes", Integer.parseInt(bytes))
						.field("Referer", matcher.group(8))
						.field("Browser", matcher.group(9))
						.endObject());

			    es.bulkProcessor.add(ir);

			}
		}		
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}



}
