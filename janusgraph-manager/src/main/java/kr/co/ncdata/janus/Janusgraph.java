package kr.co.ncdata.janus;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.indexing.IndexProvider;

import java.io.File;
import java.nio.file.Paths;

@Slf4j
public class Janusgraph {
	private final JanusGraph graph;

	public Janusgraph() {
		String osName = System.getProperty("os.name");
		log.info("osName: {}", osName);
		String CONF_PATH;
		// 각 환경에 따라 file 위치 가져올 수 있게 변경
		if(osName.toLowerCase().contains("win")){
			CONF_PATH = "C:\\tools\\map";
		} else {
			CONF_PATH = "/home/janus/janusgraph-1.0.0/conf";
		}
		System.out.println(CONF_PATH);

		graph = JanusGraphFactory.open(CONF_PATH + File.separator + "janusgraph-hbase-es.properties");
	}

	public static void main(String[] args) {
		Janusgraph janusgraph = new Janusgraph();
		try {
			janusgraph.printSchema();
		} finally {
			if(janusgraph.graph != null && janusgraph.graph.isOpen()){
				janusgraph.graph.close();
			}
		}
	}

	/**
	 * Test 용 Vertex 업로드 소스
	 */
	private void testVertexUpload() {
		// Vertex 또는 Edge 업로드를 위한 Transaction 생성
		JanusGraphTransaction tx = graph.newTransaction();
		try {
			Vertex v1 = tx.addVertex(T.label, "person", "name", "John", "age", 30);
			Vertex v2 = tx.addVertex(T.label, "person", "name", "Jane", "age", 28);
			v1.addEdge("knows", v2, "since", 2010);

			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		} finally {
			graph.close();
		}
	}

	private void printSchema(){
		// Janus Graph 관리 정보 조회를 위한 객체 생성
		JanusGraphManagement mgmt = graph.openManagement();

		try {
			log.info("schema: {}", mgmt.printSchema());

			for (JanusGraphIndex index: mgmt.getGraphIndexes(Vertex.class)) {
				log.info("index name: {}", index.name());
			}

			//mgmt.getGraphIndex("NODE_INDEX").isMixedIndex()SchemaStatus status = ;

			log.info("status isMixedIndex: {}", mgmt.getGraphIndex("NODE_INDEX").isMixedIndex());

			Configuration conf = graph.configuration();
			log.info("search backend: {}", conf.getString("index.search.backend"));
			//ElasticSearchIndex searchIndex = (ElasticSearchIndex) ((IndexProvider) graph.getBackend().getIndexInformation("search")).getSearchIndex();
		} finally {
			mgmt.rollback();
		}
	}
}
