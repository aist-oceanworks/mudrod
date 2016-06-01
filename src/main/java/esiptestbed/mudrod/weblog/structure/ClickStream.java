package esiptestbed.mudrod.weblog.structure;

import java.io.Serializable;

import org.codehaus.jettison.json.JSONObject;

public class ClickStream implements Serializable{
	private String keywords;
	private String viewDataset;
	private String downloadDataset;
	private String sessionID;
	private String type;

	public ClickStream(String keywords, String viewDataset, boolean download){
		this.keywords = keywords;
		this.viewDataset = viewDataset;
		this.downloadDataset = "";
		if(download){
			this.downloadDataset = viewDataset;
		}
	}
	
	public ClickStream(){
		
	}
	
	public void setKeyWords(String query){
		this.keywords = query;
	}
	
	public void setViewDataset(String dataset){
		this.viewDataset = dataset;
	}
	
	public void setDownloadDataset(String dataset){
		this.downloadDataset = dataset;
	}

	public String getKeyWords(){
		return this.keywords;
	}
	
	public String getViewDataset(){
		return this.viewDataset;
	}
	
	public void setSessionId(String sessionID){
		this.sessionID = sessionID;
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	public String toString(){
		return "query:" + keywords + "|| view dataset:" + viewDataset + "|| download Dataset:" + downloadDataset;
	}
	
	public String toJson() {
		String jsonQuery = "{";
		jsonQuery += "\"query\":\"" + this.keywords + "\",";
		jsonQuery += "\"viewdataset\":\"" + this.viewDataset + "\",";
		jsonQuery += "\"downloaddataset\":\"" + this.downloadDataset + "\",";
		jsonQuery += "\"sessionId\":\"" + this.sessionID + "\",";
		jsonQuery += "\"type\":\"" + this.type + "\"";
		jsonQuery += "},";
		return jsonQuery;
	}
	
	public static ClickStream parseFromTextLine(String logline)throws Exception {
		JSONObject jsonData = new JSONObject(logline);
		ClickStream data = new ClickStream();
		data.setKeyWords(jsonData.getString("query"));
		data.setViewDataset(jsonData.getString("viewdataset"));
		data.setDownloadDataset(jsonData.getString("downloaddataset"));

		return data;
	}
	
}
