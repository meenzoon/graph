package kr.co.ncdata.janus;

import kr.co.ncdata.janus.helper.NodeLinkReader;
import kr.co.ncdata.janus.vo.MoctNodeVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.util.system.ConfigurationUtil;
import org.locationtech.jts.geom.Point;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@Slf4j
public class JanusVertexRemote {
	GraphTraversalSource g;
	JanusGraph graph;
	Cluster cluster;
	Client client;
	Configuration conf;

	public JanusVertexRemote() throws Exception {
		conf = ConfigurationUtil.loadPropertiesConfig(JanusConfig.REMOTE_PROP_FILE_NAME);
		try {
			log.info("gremlin.remote.driver.clusterFile: {}", conf.getString("gremlin.remote.driver.clusterFile"));
			cluster = Cluster.open(conf.getString("gremlin.remote.driver.clusterFile"));
			client = cluster.connect();
		} catch (Exception e) {
			log.error("", e);
		}

		g = traversal().withRemote(conf);
	}

	public static void main(String[] args) throws Exception {
		JanusVertexRemote jm = new JanusVertexRemote();
		try {
			//jm.proc();
			jm.addNode();
		} catch (Exception e) {
			log.error("", e);
		} finally {
			if (jm.g != null)
				jm.g.close();
		}
	}

	/**
	 * 국가 표준 노드 정보 업로드
	 */
	private void addNode() {
		NodeLinkReader redader = new NodeLinkReader();
		List<MoctNodeVo> nodeVoMap = redader.readNode();
		log.info("read node count: {}", nodeVoMap.size());

		try {
			int count = 0;
			int index = 0;

			g.tx().begin();

			for (MoctNodeVo nodeVo : nodeVoMap) {
				Point point = nodeVo.getPoint();
				Geoshape janusPoint = Geoshape.point(point.getY(), point.getX());

				Map<Object, Object> properties = new HashMap<>();
				properties.put("NODE_ID", nodeVo.getNodeId());
				properties.put("POINT", janusPoint);
				properties.put("NODE_TYPE", nodeVo.getNodeType());
				properties.put("NODE_NAME", nodeVo.getNodeName());
				properties.put("TURN_P", nodeVo.getTurnP());

				g.addV("node").property(properties);

				++count;
				if (++index % 10000 == 0) {
					g.tx().commit();
					g.tx().begin();
					log.info("add node vertex count: {}", count);
					index = 0;
				}
			}
			g.tx().commit();
		} catch (Exception e) {
			g.tx().rollback();
			log.error("", e);
		}

	}
}
