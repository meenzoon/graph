package kr.co.ncdata.janus;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;

import java.io.BufferedReader;
import java.io.FileReader;

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
			edge.proc();
		} finally {
			if (edge.tx != null && edge.tx.isOpen())
				edge.tx.close();

			if (edge.graph != null && edge.graph.isOpen())
				edge.graph.close();
		}
	}

	public void proc() {
		int index = 0;

		log.info("read csv");
		try (BufferedReader reader = new BufferedReader(new FileReader("/home/janus/map/daegu_way.csv"))) {
			//try (BufferedReader reader = new BufferedReader(new FileReader("C:\\tools\\map\\daegu_way.csv"))) {
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
}
