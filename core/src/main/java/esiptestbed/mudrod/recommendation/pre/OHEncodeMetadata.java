package esiptestbed.mudrod.recommendation.pre;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Seconds;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.OHEncoder;

// one binary encoding of metadata parameters
public class OHEncodeMetadata extends DiscoveryStepAbstract {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(OHEncodeMetadata.class);

	public OHEncodeMetadata(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
	}

	@Override
	public Object execute() {
		LOG.info("*****************Metadata OHEncode starts******************");
		startTime = System.currentTimeMillis();
		
		OHEncoder encoder = new OHEncoder(config);
		encoder.OHEncodeVars(es);
		encoder.OHEncodeaAllMetadata(es);
		
		endTime = System.currentTimeMillis();
		es.refreshIndex();
		LOG.info("*****************Metadata OHEncode ends******************Took {}s", (endTime - startTime) / 1000);
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}
}
