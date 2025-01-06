package kr.co.ncdata.janus;

import kr.co.ncdata.janus.helper.GeoHelper;
import kr.co.ncdata.janus.helper.NodeLinkReader;
import kr.co.ncdata.janus.vo.MoctLinkVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.attribute.Geoshape;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

@Slf4j
public class JanusEdge {
	JanusGraph graph;
	JanusGraphTransaction tx;
	GraphTraversalSource g;

	public JanusEdge() {
		graph = JanusGraphFactory.open(JanusManager.PROP_FILE_NAME);

		System.out.println("graph new transaction");
		g = graph.traversal();
		tx = graph.newTransaction();
	}

	public static void main(String[] args) {
		JanusEdge edge = new JanusEdge();
		try {
			//edge.proc();
			edge.addLink();
		} catch (Exception e) {
			log.error("", e);
		} finally {
			if (edge.tx != null && edge.tx.isOpen())
				edge.tx.close();

			if (edge.graph != null && edge.graph.isOpen())
				edge.graph.close();
			if (edge.g != null) {
				try {
					edge.g.close();
				} catch (Exception e) {
					log.error("", e);
				}
			}

		}
	}

	public void proc() {
		int index = 0;

		log.info("read csv");
		try (BufferedReader reader = new BufferedReader(new FileReader(JanusManager.OSM_WAY_FILE))) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (++index == 1)
					continue;

				if (index % 100 == 0) {
					log.info("index: {}", index);
				}
				String[] col = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				String[] nodes = col[1].split(",");

				String startNode = nodes[0].replace("\"", "").replace("[", "").trim();
				String endNode = nodes[nodes.length - 1].replace("\"", "").replace("]", "").trim();


				//Vertex startVertex = .next();
				//System.out.println("find complete startVetext");
				//Vertex endVertex = .next();

				//System.out.println("add Edge");
				Vertex startVertex;
				Vertex endVertex;

				try {
					startVertex = g.V().has("nodeId", startNode).next();
					endVertex = g.V().has("nodeId", endNode).next();

					g.V(startVertex).addE("way").to(endVertex).next();
				} catch (Exception e) {
					log.error("index: {}, nodeId: {}, startNode: {}, endNode: {}", index, col[0], startNode, endNode);
					continue;
				}

				//startVertex.addEdge("way", endVertex);

				/*
				jgt.traversal().addE("way")
					.from(  __.V().has("id", startNode))
					.to(__.V().has("id", endNode));
				 */

				//jgt.addVertex(T.label, "osmNode", "id", col[0], "latitude", col[1], "longitude", col[2]);

				tx.commit();
				tx = graph.newTransaction();
				g.tx().commit();
			}
		} catch (Exception e) {
			log.error("", e);
		}
	}

	/**
	 * 국가 표준 링크 정보 업로드
	 */
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
					//log.info("startNode: {}, endNode: {}", startNode, endNode);

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
}
