package esiptestbed.mudrod.ontology.pre;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.filter.ElementFilter;
import org.jdom2.input.SAXBuilder;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;

public class AggregateTriples extends DiscoveryStepAbstract {

	public AggregateTriples(Map<String, String> config, ESDriver es) {
		super(config, es);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		File file = new File(this.config.get("ontologyOutputFile"));
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		FileWriter fw;
		try {
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		File[] files = new File(this.config.get("ontologyInputDir")).listFiles();
		for (File file_in : files) {      
			String ext = FilenameUtils.getExtension(file_in.getAbsolutePath());
			if(ext.equals("owl")){
				try {
					loadxml(file_in.getAbsolutePath());
					getAllClass();
				} catch (JDOMException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			}
		}

		try {
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Document document;
	public Element rootNode=null;
	final static String owl_namespace = "http://www.w3.org/2002/07/owl#";
	final static String rdf_namespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	final static String rdfs_namespace = "http://www.w3.org/2000/01/rdf-schema#";

	BufferedWriter bw = null;

	public void loadxml(String filePathName) throws JDOMException, IOException{
		SAXBuilder saxBuilder = new SAXBuilder();
		File file=new File(filePathName);

		document = saxBuilder.build(file);
		rootNode = document.getRootElement();

	}

	public void loopxml(){
		Iterator<?> processDescendants = rootNode.getDescendants(new ElementFilter()); 
		String text="";

		while(processDescendants.hasNext()) {
			Element e =  (Element) processDescendants.next();
			String currentName = e.getName();
			text=e.getTextTrim();
			if(text.equals("")){
				System.out.println(currentName);
			}else{
				System.out.println(currentName+":"+text);
			}
		}
	}

	public Element findChild(String str, Element ele){
		Iterator<?> processDescendants = ele.getDescendants(new ElementFilter()); 
		String name="";
		Element result=null;

		while(processDescendants.hasNext()) {
			Element e =  (Element) processDescendants.next();
			name=e.getName();
			if(name.equals(str)){
				result=e;
				return result;
			}
		}
		return result;

	}

	public void getAllClass() throws IOException{
		List classElements = rootNode.getChildren("Class",Namespace.getNamespace("owl",owl_namespace));

		for(int i = 0 ; i < classElements.size() ; i++) {
			Element classElement = (Element) classElements.get(i);
			String className = classElement.getAttributeValue("about", Namespace.getNamespace("rdf",rdf_namespace));

			if(className == null){
				className = classElement.getAttributeValue("ID", Namespace.getNamespace("rdf",rdf_namespace));
			}

			List SubclassElements = classElement.getChildren("subClassOf",Namespace.getNamespace("rdfs",rdfs_namespace));
			for(int j = 0 ; j < SubclassElements.size() ; j++)
			{
				Element subclassElement = (Element) SubclassElements.get(j);
				String SubclassName = subclassElement.getAttributeValue("resource", Namespace.getNamespace("rdf",rdf_namespace));
				if(SubclassName==null)
				{
					Element allValuesFromEle = findChild("allValuesFrom", subclassElement);
					if(allValuesFromEle!=null){
						SubclassName = allValuesFromEle.getAttributeValue("resource", Namespace.getNamespace("rdf",rdf_namespace));
						bw.write(cutString(className) + ",SubClassOf," + cutString(SubclassName) + "\n");	
					}
				}
				else
				{
					bw.write(cutString(className) + ",SubClassOf," + cutString(SubclassName) + "\n");
				}

			}

			List EqualclassElements = classElement.getChildren("equivalentClass",Namespace.getNamespace("owl",owl_namespace));
			for(int k = 0 ; k < EqualclassElements.size() ; k++)
			{
				Element EqualclassElement = (Element) EqualclassElements.get(k);
				String EqualclassElementName = EqualclassElement.getAttributeValue("resource", Namespace.getNamespace("rdf",rdf_namespace));

				if(EqualclassElementName!=null)
				{
					bw.write(cutString(className) + ",equivalentClass," + cutString(EqualclassElementName) + "\n");
				}
			}


		}
	}

	public String cutString(String str)
	{
		str = str.substring(str.indexOf("#")+1);
		String[] str_array = str.split("(?=[A-Z])");
		str = Arrays.toString(str_array);
		return str.substring(1, str.length()-1).replace(",", "");
	}

}
