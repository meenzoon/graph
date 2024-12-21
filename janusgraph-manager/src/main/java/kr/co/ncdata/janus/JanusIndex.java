package kr.co.ncdata.janus;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
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
			//j.printSchema();
			j.addEdgeKey();
		} finally {
			if (j.graph != null && j.graph.isOpen())
				j.graph.close();
		}
	}

	private void printSchema() {
		// Janus Graph 관리 정보 조회를 위한 객체 생성
		JanusGraphManagement mgmt = graph.openManagement();

		try {
			Configuration conf = graph.configuration();
			Iterator<String> keys = conf.getKeys();
			while (keys.hasNext()) {
				String key = keys.next();
				log.info("key: {}, config: {}", key, conf.getString(key));
			}

			log.info("schema: {}", mgmt.printSchema());

			JanusGraphIndex index = mgmt.getGraphIndex("NODE_INDEX");
			log.info("index name: {}, isUnique: {}, isMixedIndex: {}, isCompositeIndex: {}", index.name(),
				index.isUnique(), index.isMixedIndex(), index.isCompositeIndex());

			SchemaStatus status = index.getIndexStatus(mgmt.getPropertyKey("NODE_ID"));
			log.info("status: {}", status.isStable());

			//ElasticSearchIndex searchIndex = (ElasticSearchIndex) ((IndexProvider) graph.getBackend().getIndexInformation("search")).getSearchIndex();
		} finally {
			mgmt.rollback();
		}
	}

	/**
	 * Vertex Key 추가
	 */
	private void addVertexKey() {
		JanusGraphManagement mgmt = graph.openManagement();

		try {
			String keyName = "NODE_ID";
			if (mgmt.getPropertyKey(keyName) == null) {
				PropertyKey newKey = mgmt.makePropertyKey(keyName).dataType(String.class)  // 데이터 타입 지정 (예: String)
					.cardinality(Cardinality.SINGLE)  // 카디널리티 지정
					.make();

				mgmt.buildIndex("NODE_INDEX", Vertex.class).addKey(newKey).buildMixedIndex("search");
				//ManagementSystem.awaitGraphIndexStatus(graph, "EDGE_INDEX").call();

				// Edge Label에 새 Key 추가 (선택적)
				//String edgeLabel = "existingEdgeLabel";
				//mgmt.addProperties(mgmt.getEdgeLabel(edgeLabel), newKey);


				// 변경사항 커밋
				mgmt.commit();
				log.info("Vertex Key added: {}", keyName);
			} else {
				log.info("Vertex Key: {} already exists", keyName);
				mgmt.rollback();
			}
		} catch (Exception e) {
			mgmt.rollback();
			log.error("", e);
		}
	}

	/**
	 * Edge Key 추가
	 */
	private void addEdgeKey() {
		JanusGraphManagement mgmt = graph.openManagement();

		try {
			String keyName = "EDGE_ID";
			if (mgmt.getPropertyKey(keyName) == null) {
				PropertyKey newKey = mgmt.makePropertyKey(keyName).dataType(String.class)  // 데이터 타입 지정 (예: String)
					.cardinality(Cardinality.SINGLE)  // 카디널리티 지정
					.make();

				mgmt.buildIndex("EDGE_INDEX", Edge.class).addKey(newKey).buildMixedIndex("search");
				//ManagementSystem.awaitGraphIndexStatus(graph, "EDGE_INDEX").call();

				// Edge Label에 새 Key 추가 (선택적)
				//String edgeLabel = "existingEdgeLabel";
				//mgmt.addProperties(mgmt.getEdgeLabel(edgeLabel), newKey);


				// 변경사항 커밋
				mgmt.commit();
				log.info("Edge Key added: {}", keyName);
			} else {
				log.info("Edge Key: {} already exists", keyName);
				mgmt.rollback();
			}
		} catch (Exception e) {
			mgmt.rollback();
			log.error("", e);
		}
	}
}
