package kr.co.ncdata.janus;

import kr.co.ncdata.janus.helper.GeoHelper;
import kr.co.ncdata.janus.helper.NodeLinkReader;
import kr.co.ncdata.janus.vo.MoctLinkVo;
import kr.co.ncdata.janus.vo.MoctNodeVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.locationtech.jts.geom.Point;

import java.util.HashMap;
import java.util.List;

@Slf4j
public class JanusInmemory {
	JanusGraph graph;
	JanusGraphTransaction tx;
	GraphTraversalSource g;

	public JanusInmemory() {
		graph = JanusGraphFactory.open(JanusConfig.HBASE_ES_PROP_FILE_NAME);

		System.out.println("graph new transaction");
		g = graph.traversal();
		tx = graph.newTransaction();
	}

	public static void main(String[] args) {
		HashMap<String, Double> map = new HashMap<>();
		double start, end;

		JanusInmemory jm = new JanusInmemory();
		try {
			start = System.currentTimeMillis();
			jm.addVertexKey("NODE_INDEX", "NODE_ID", String.class);
			end = System.currentTimeMillis();
			map.put("nodeIndex", end - start);

			start = System.currentTimeMillis();
			jm.addVertexKey("POINT_INDEX", "POINT", Geoshape.class);
			end = System.currentTimeMillis();
			map.put("pointIndex", end - start);

			start = System.currentTimeMillis();
			jm.addNode();
			end = System.currentTimeMillis();
			map.put("allNode", end - start);

			start = System.currentTimeMillis();
			jm.addLink();
			end = System.currentTimeMillis();
			map.put("allLink", end - start);

			//jm.gpsMatching();

			System.out.println("NODE_ID 인덱스 생성 시간: " + map.get("nodeIndex"));
			System.out.println("POINT 인덱스 생성 시간: " + map.get("pointIndex"));
			System.out.println("모든 노드 삽입 시간: " + map.get("allNode"));
			System.out.println("모든 링크 삽입 시간: " + map.get("allLink"));

			jm.tx.commit();
		} catch (Exception e) {
			log.error("", e);
		} finally {
			if (jm.tx != null && jm.tx.isOpen())
				jm.tx.close();

			if (jm.graph != null && jm.graph.isOpen())
				jm.graph.close();
		}
	}

	private void addVertexKey(String vertexIndexName, String propertyKey, Class<?> dataType) {
		JanusGraphManagement mgmt = graph.openManagement();

		try {
			if (mgmt.getPropertyKey(propertyKey) == null) {
				mgmt.makePropertyKey(propertyKey).dataType(dataType).cardinality(Cardinality.SINGLE).make();
			}

			mgmt.buildIndex(vertexIndexName, Vertex.class)
				.addKey(mgmt.getPropertyKey(propertyKey))
				.unique()
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

	private void addLink() throws Exception {
		NodeLinkReader redader = new NodeLinkReader();
		List<MoctLinkVo> linkVoMap = redader.readLink();
		log.info("read link count: {}", linkVoMap.size());

		tx = graph.newTransaction();

		try {
			int count = 0;
			int index = 0;

			for (MoctLinkVo linkVo : linkVoMap) {
				String startNode = linkVo.getStartNode();
				String endNode = linkVo.getEndNode();

				try {
					Geoshape geoshape = GeoHelper.convertMultiLineString(linkVo.getLineString());
					log.info("startNode: {}, endNode: {}", startNode, endNode);

					Vertex startVertex = g.V().hasLabel("node").has("NODE_ID", startNode).next();
					Vertex endVertex = g.V().hasLabel("node").has("NODE_ID", endNode).next();

					g.V(startVertex)
						.addE("way")
						.to(endVertex)
						.property("LINK_ID", linkVo.getLinkId())
						.property("LANES", linkVo.getLanes())
						.property("ROAD_RANK", linkVo.getRoadRank())
						.property("ROAD_TYPE", linkVo.getRoadType())
						.property("ROAD_NO", linkVo.getRoadNo())
						.property("ROAD_NAME", linkVo.getRoadName())
						.property("GEOM", geoshape)
						.property("MAX_SPD", linkVo.getMaxSpd())
						.property("LENGTH", linkVo.getLength())
						.next();

				} catch (Exception e) {
					e.printStackTrace();
					break;
					//continue;
				}

				++count;
				if (++index % 10000 == 0) {
					g.tx().commit();
					//tx.commit();
					tx = graph.newTransaction();
					log.info("add link vertex count: {}", count);
					index = 0;
				}
			}
			//tx.commit();
			g.tx().commit();
			tx = graph.newTransaction();
		} catch (Exception e) {
			g.tx().rollback();

			//tx.rollback();
			log.error("", e);
		}
	}

	/*
	private void gpsMatching() {
		try (BufferedReader reader = new BufferedReader(
			new FileReader(JanusConfig. + File.separator + "_37630289.csv"))) {
			String line = reader.readLine();
			String[] split = line.split(",");
			double lng = Double.parseDouble(split[8]);
			double lat = Double.parseDouble(split[9]);
			Geoshape point = Geoshape.circle(lat, lng, 1);

			GraphTraversalSource g = graph.traversal();
			List<Vertex> list = g.V().hasLabel("node").has("POINT", Geo.geoWithin(point)).limit(1).toList();

			for (Vertex v : list) {
				System.out.println(v.property("NODE_NAME").value());
			}
		} catch (Exception e) {
			log.error("", e);
		}
	}
	 */
}
