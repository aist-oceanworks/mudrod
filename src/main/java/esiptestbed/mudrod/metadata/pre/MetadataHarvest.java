package esiptestbed.mudrod.metadata.pre;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.structure.DIFMetadata;

public class MetadataHarvest extends DiscoveryStepAbstract {

	public MetadataHarvest(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		return null;
		// TODO Auto-generated method stub
	}

	public void addMetadataMapping() {
		XContentBuilder Mapping;
		try {
			Mapping = jsonBuilder().startObject().startObject("analysis").startObject("filter")
					.startObject("english_stop").field("type", "stop").field("stopwords", "_english_s").endObject()
					.startObject("english_stemmer").field("type", "stemmer").field("language", "english").endObject()
					.startObject("english_possessive_stemmer").field("type", "stemmer")
					.field("language", "possessive_english").endObject().startObject("english_keywords")
					.field("type", "keyword_marker").field("keywords", "[]").endObject().endObject()
					.startObject("analyzer").startObject("english").field("tokenizer", "standard").endObject()
					.endObject().endObject().endObject();

			es.client.admin().indices().prepareCreate(config.get("indexName"))
					.setSettings(ImmutableSettings.settingsBuilder().loadFromSource(Mapping.toString())).execute()
					.actionGet();

			Mapping = jsonBuilder().startObject().startObject(config.get("metadataType")).startObject("properties")
					.startObject("shortName").field("type", "string").field("index", "not_analyzed").endObject()
					.startObject("longName").field("type", "string").field("index", "not_analyzed").endObject()
					.startObject("keyword").field("type", "string").field("index", "english").endObject()
					.startObject("term").field("type", "string").field("index", "english").endObject()
					.startObject("topic").field("type", "string").field("index", "english").endObject()
					.startObject("variable").field("type", "string").field("index", "english").endObject()
					.startObject("abstract").field("type", "string").field("index", "english").endObject().endObject()
					.endObject().endObject();

			es.client.admin().indices().preparePutMapping(config.get("metadataType")).setType("SWEET")
					.setSource(Mapping).execute().actionGet();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void importMetaDataToES() throws IOException, JAXBException {

		Timer timer = new Timer();
		TimerTask task = new TimerTask() {
			int count = 0;

			@Override
			public void run() {
				if (count == 0) {
					timer.cancel();
					return;
				}
				try {
					getMetadataByWS("");
				} catch (JAXBException | IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("every 1 seconds...");
			}
		};
		timer.schedule(task, 1000, 1000);
	}

	public void getMetadataByWS(String shortname) throws JAXBException, IOException, InterruptedException, ExecutionException {
		String uri = "http://podaac.jpl.nasa.gov/ws/metadata/dataset?format=gcmd&shortName=" + shortname;
		System.out.println(uri);
		URL url = new URL(uri);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Accept", "application/xml");
		InputStream xml = connection.getInputStream();

		JAXBContext jc = JAXBContext.newInstance(DIFMetadata.class);
		DIFMetadata metadata = (DIFMetadata) jc.createUnmarshaller().unmarshal(xml);
			
			String isoTopic = String.join(",", metadata.isoTopic);
			String sensor = metadata.sensor.sensor_short_name.trim() + "," + metadata.sensor.sensor_long_name.trim();
			String source = metadata.source.source_short_name.trim() + "," + metadata.source.source_long_name.trim();
			String project = metadata.project.project_short_name.trim() + "," + metadata.project.project_long_name.trim();
			IndexRequest ir = new IndexRequest(config.get("indexName"), config.get("metadataType")).source(jsonBuilder().startObject()
					.field("shortName", metadata.shortName)
					.field("keyword", metadata)
					.field("term", metadata)
					.field("topic", "")
					.field("variable", metadata)
					.field("abstract", metadata.summary.Abstract.trim())
					.field("isotopic", es.customAnalyzing(config.get("indexName"), isoTopic))
					.field("sensor", es.customAnalyzing(config.get("indexName"), sensor))
					.field("source", es.customAnalyzing(config.get("indexName"), source))
					.field("project", es.customAnalyzing(config.get("indexName"), project))
					.endObject());
			es.bulkProcessor.add(ir);
		
		connection.disconnect();
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}
}
