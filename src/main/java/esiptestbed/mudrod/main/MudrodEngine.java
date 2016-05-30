package esiptestbed.mudrod.main;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import esiptestbed.mudrod.discoveryengine.DiscoveryEngineAbstract;
import esiptestbed.mudrod.discoveryengine.WeblogDiscoveryEngine;
import esiptestbed.mudrod.driver.ESDriver;


public class MudrodEngine {
	private Map<String, String> config = new HashMap<String, String>();
	private ESDriver es = null;
	
	public MudrodEngine()
	{
		loadConfig();
		es = new ESDriver(config.get("clusterName"));
		System.out.println("fdsfa");
	}
	
	public Map<String, String> getConfig(){
		return config;
	}
	
	public void loadConfig(){
		String filePathName = "./src/main/java/esiptestbed/mudrod/main/config.xml";
		//TODO: load the configuration from the xml file, and populate the configs arraylist
		SAXBuilder saxBuilder = new SAXBuilder();
		File file=new File(filePathName);

		Document document;
		try {
			document = saxBuilder.build(file);
			Element rootNode = document.getRootElement();
			List<Element> para_list = rootNode.getChildren("para");
			
			for (int i = 0; i < para_list.size(); i++) {
				Element para_node = para_list.get(i);
				config.put(para_node.getAttributeValue("name"),para_node.getTextTrim());
			}
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for (Map.Entry<String, String> entry : config.entrySet()) {
		    System.out.println(entry.getKey()+" : "+entry.getValue());
		}

	}
	
	public void start(){
		DiscoveryEngineAbstract de = new WeblogDiscoveryEngine(config, es);
		de.preprocess();
		de.process();
		de.output();
	}
	
	public void end(){
		es.close();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        MudrodEngine test = new MudrodEngine();
        test.start();
        test.end();
	}

}
