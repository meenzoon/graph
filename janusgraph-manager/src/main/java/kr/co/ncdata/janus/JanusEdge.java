package kr.co.ncdata.janus;

import kr.co.ncdata.janus.helper.GeoHelper;
import kr.co.ncdata.janus.helper.NodeLinkReader;
import kr.co.ncdata.janus.vo.MoctLinkVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.attribute.Geoshape;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

@Slf4j
public class JanusEdge {

	public static void main(String[] args) {
		try {
			JanusEdge edge = new JanusEdge();
			JanusManager.initGraph(JanusConfig.HBASE_ES_PROP_FILE_NAME);
			//edge.proc();
			edge.addLink();
		} catch (Exception e) {
			log.error("", e);
		} finally {
			JanusManager.closeGraph();
		}
	}

	public void proc() {
		int index = 0;

		GraphTraversalSource g = JanusManager.getTraversalSource();

		log.info("read csv");
		try (BufferedReader reader = new BufferedReader(new FileReader(JanusConfig.OSM_WAY_FILE))) {
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

				g.tx().commit();
			}
		} catch (Exception e) {
			g.tx().rollback();
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

		GraphTraversalSource g = JanusManager.getTraversalSource();

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
					log.error("", e);
					break;
					//continue;
				}

				++count;
				if (++index % 10000 == 0) {
					g.tx().commit();
					g.tx().begin();
					log.info("add link vertex count: {}", count);
					index = 0;
				}
			}
			g.tx().commit();
			g.tx().close();
		} catch (Exception e) {
			g.tx().rollback();
			log.error("", e);
		}
	}
}
