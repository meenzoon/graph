package kr.co.ncdata.janus;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;

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
			//j.removeVertexKey();
			j.addVertexKey();
			//j.addEdgeKey();
			j.printSchema();
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

		String vertexIndexName = "NODE_INDEX";
		String propertyKey = "NODE_ID";
		try {
			if (mgmt.getPropertyKey(propertyKey) == null) {
				mgmt.makePropertyKey(propertyKey).dataType(String.class).cardinality(Cardinality.SINGLE).make();
			}

			mgmt.buildIndex(vertexIndexName, Vertex.class).addKey(mgmt.getPropertyKey(propertyKey)).unique()
				.buildCompositeIndex();
			//.buildMixedIndex("search");
			mgmt.commit();
			ManagementSystem.awaitGraphIndexStatus(graph, vertexIndexName).status(SchemaStatus.INSTALLED).call();

			// 등록
			mgmt = graph.openManagement();
			mgmt.updateIndex(mgmt.getGraphIndex(vertexIndexName), SchemaAction.REGISTER_INDEX).get();
			mgmt.commit();
			ManagementSystem.awaitGraphIndexStatus(graph, vertexIndexName).status(SchemaStatus.REGISTERED).call();

			// index 활성화
			mgmt = graph.openManagement();
			mgmt.updateIndex(mgmt.getGraphIndex(vertexIndexName), SchemaAction.ENABLE_INDEX).get();
			mgmt.commit();
			ManagementSystem.awaitGraphIndexStatus(graph, vertexIndexName).status(SchemaStatus.ENABLED).call();
		} catch (Exception e) {
			mgmt.rollback();
			log.error("", e);
		}
	}

	/**
	 * Vertex Key 삭제
	 */
	private void removeVertexKey() {

		String vertexIndexName = "NODE_INDEX";
		try {
			// Index LifeCycle 에 따라 DISABLE -> DISCARD -> DROP
			JanusGraphManagement mgmt = graph.openManagement();
			mgmt.updateIndex(mgmt.getGraphIndex(vertexIndexName), SchemaAction.DISABLE_INDEX).get();
			mgmt.commit();
			ManagementSystem.awaitGraphIndexStatus(graph, vertexIndexName).status(SchemaStatus.DISABLED).call();

			mgmt = graph.openManagement();
			mgmt.updateIndex(mgmt.getGraphIndex(vertexIndexName), SchemaAction.DISCARD_INDEX).get();
			mgmt.commit();
			ManagementSystem.awaitGraphIndexStatus(graph, vertexIndexName).status(SchemaStatus.DISCARDED).call();

			mgmt = graph.openManagement();
			mgmt.updateIndex(mgmt.getGraphIndex(vertexIndexName), SchemaAction.DROP_INDEX).get();
			mgmt.commit();
			//ManagementSystem.awaitGraphIndexStatus(graph, vertexIndexName).call();

		} catch (Exception e) {
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
