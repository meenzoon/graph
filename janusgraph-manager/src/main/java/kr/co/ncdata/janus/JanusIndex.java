package kr.co.ncdata.janus;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;

import java.util.Iterator;

@Slf4j
public class JanusIndex {
	JanusGraph graph;

	public JanusIndex() {
		graph = JanusGraphFactory.open(JanusManager.PROP_FILE_NAME);
	}

	public static void main(String[] args) {
		JanusIndex j = new JanusIndex();
		try {
			j.printSchema();
		} finally {
			if(j.graph != null && j.graph.isOpen())
				j.graph.close();
		}
	}

	private void printSchema(){
		// Janus Graph 관리 정보 조회를 위한 객체 생성
		JanusGraphManagement mgmt = graph.openManagement();

		try {
			Configuration conf = graph.configuration();
			Iterator<String> keys = conf.getKeys();
			while(keys.hasNext()){
				String key = keys.next();
				log.info("key: {}, config: {}", key, conf.getString(key));
			}

			log.info("schema: {}", mgmt.printSchema());

			for (JanusGraphIndex index: mgmt.getGraphIndexes(Vertex.class)) {
				log.info("index name: {}", index.name());
			}

			SchemaStatus status = mgmt.getGraphIndex("NODE_INDEX").getIndexStatus(mgmt.getPropertyKey("NODE_INDEX"));
			log.info("status: {}", status.isStable());

			//ElasticSearchIndex searchIndex = (ElasticSearchIndex) ((IndexProvider) graph.getBackend().getIndexInformation("search")).getSearchIndex();
		} finally {
			mgmt.rollback();
		}
	}
}
