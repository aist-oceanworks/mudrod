package esiptestbed.mudrod.weblog.structure;

public class ClickThroughData {
	public String keywords;
	public String viewDataset;
	public String downloadDataset;
	public String sessionID;
	public String type;
	
	public ClickThroughData(){
		
	}
	
	public ClickThroughData(String keywords, String viewDataset, boolean download){
		this.keywords = keywords;
		this.viewDataset = viewDataset;
		this.downloadDataset = "";
		if(download){
			this.downloadDataset = viewDataset;
		}
		
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
}
