package kr.co.ncdata.janus;

import kr.co.ncdata.janus.vo.MoctNodeVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.locationtech.jts.geom.Point;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

@Slf4j
public class JanusVertex {
	JanusGraph graph;
	JanusGraphTransaction tx;
	GraphTraversalSource g;

	public JanusVertex() {
		graph = JanusGraphFactory.open(JanusManager.PROP_FILE_NAME);

		g = graph.traversal();
		tx = graph.newTransaction();
	}

	public static void main(String[] args) {
		JanusVertex jm = new JanusVertex();
		try {
			//jm.proc();
			jm.addNode();
		} catch (Exception e) {
			log.error("", e);
		} finally {
			if (jm.tx != null && jm.tx.isOpen())
				jm.tx.close();

			if (jm.graph != null && jm.graph.isOpen())
				jm.graph.close();
		}
	}

	/**
	 * Test 용 Vertex 업로드 소스
	 */
	private void testVertexUpload() {
		// Vertex 또는 Edge 업로드를 위한 Transaction 생성
		tx = graph.newTransaction();
		try {
			Vertex v1 = tx.addVertex(T.label, "person", "name", "John", "age", 30);
			Vertex v2 = tx.addVertex(T.label, "person", "name", "Jane", "age", 28);
			v1.addEdge("knows", v2, "since", 2010);

			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			log.error("", e);
		}
	}

	public void proc() throws InterruptedException {
		int count = 0;
		int index = 0;

		log.info("add index start");

		JanusGraphManagement mgmt = graph.openManagement();

		// 속성 키 정의
		PropertyKey name = mgmt.makePropertyKey("nodeId").dataType(String.class).cardinality(Cardinality.SINGLE).make();

		// 인덱스 생성
		mgmt.buildIndex("NODE_ID", Vertex.class).addKey(name).buildCompositeIndex();

		mgmt.commit();
		ManagementSystem.awaitGraphIndexStatus(graph, "NODE_ID").call();

		log.info("add index end");

		tx = graph.newTransaction();

		try (BufferedReader reader = new BufferedReader(new FileReader(JanusManager.OSM_NODE_FILE))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] col = line.split(",");

				tx.addVertex(T.label, "node", "nodeId", col[0], "latitude", col[1], "longitude", col[2]);

				++count;
				if (++index % 10000 == 0) {
					tx.commit();
					tx = graph.newTransaction();
					log.info("count: {}", count);
					index = 0;
				}
			}
			tx.commit();
			log.info("end count: {}", count);
		} catch (Exception e) {
			log.error("", e);
		}
	}

	/**
	 * 국가 표준 노드 정보 업로드
	 */
	private void addNode() {
		NodeLinkReader redader = new NodeLinkReader();
		List<MoctNodeVo> nodeVoMap = redader.readNode();
		log.info("read node count: {}", nodeVoMap.size());

		tx = graph.newTransaction();

		try {
			int count = 0;
			int index = 0;

			for (MoctNodeVo nodeVo : nodeVoMap) {
				Point point = nodeVo.getPoint();
				Geoshape janusPoint = Geoshape.point(point.getY(), point.getX());

				tx.addVertex(T.label, "node", "NODE_ID", nodeVo.getNodeId(), "POINT", janusPoint, "NODE_TYPE",
					nodeVo.getNodeType(), "NODE_NAME", nodeVo.getNodeName(), "TURN_P", nodeVo.getTurnP());

				++count;
				if (++index % 10000 == 0) {
					tx.commit();
					tx = graph.newTransaction();
					log.info("add node vertex count: {}", count);
					index = 0;
				}
			}
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			log.error("", e);
		}

	}
}
