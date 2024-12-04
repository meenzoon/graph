package kr.co.ncdata.janus;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.io.BufferedReader;
import java.io.FileReader;

@Slf4j
public class JanusgraphManager {
	JanusGraph graph;

	public JanusgraphManager() {
		graph = JanusGraphFactory.open("/home/janus/janusgraph-1.0.0/conf/janusgraph-hbase-es.properties");
	}

	public static void main(String[] args) {
		JanusgraphManager jm = new JanusgraphManager();
		try {
			jm.proc();
		} catch(InterruptedException e) {
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

		JanusGraphTransaction jgt = graph.newTransaction();

		try (BufferedReader reader = new BufferedReader(new FileReader("/home/janus/map/daegu_node.csv"))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] col = line.split(",");

				jgt.addVertex(T.label, "node", "nodeId", col[0], "latitude", col[1], "longitude", col[2]);

				++count;
				if (++index % 10000 == 0) {
					jgt.commit();
					jgt = graph.newTransaction();
					log.info("count: {}", count);
					index = 0;
				}
			}
		} catch (Exception e) {
			log.error("", e);
		}

		if (jgt != null && jgt.isOpen()) {
			jgt.commit();
			jgt.close();
			log.info("end jgt count: {}", count);
		}
		if (graph != null && graph.isOpen()) {
			graph.close();
		}
	}
}
