package kr.co.ncdata.janus;

import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;

import java.io.BufferedReader;
import java.io.FileReader;

@Slf4j
public class JanusGraphEdge {
	JanusGraph graph;
	JanusGraphTransaction jgt;
	GraphTraversalSource g;

	public JanusGraphEdge() {
		System.out.println("graph open");
		graph = JanusGraphFactory.open("/home/janus/janusgraph-1.0.0/conf/janusgraph-hbase-es.properties");
		//graph = JanusGraphFactory.open("C:\\tools\\map\\janusgraph-hbase-es.properties");
		System.out.println("graph new transaction");
		g = graph.traversal();
		jgt = graph.newTransaction();
	}

	public static void main(String[] args) {
		JanusGraphEdge edge = new JanusGraphEdge();
		edge.proc();
	}

	public void proc(){
		int index = 0;

		log.info("read csv");
		try (BufferedReader reader = new BufferedReader(new FileReader("/home/janus/map/daegu_way.csv"))) {
		//try (BufferedReader reader = new BufferedReader(new FileReader("C:\\tools\\map\\daegu_way.csv"))) {
			String line;
			while ((line = reader.readLine()) != null) {
				if(++index == 1)
					continue;

				if(index % 100 == 0){
					log.info("index: {}", index);
				}
				String[] col = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
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
				} catch(Exception e) {
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

				jgt.commit();
				jgt = graph.newTransaction();
				g.tx().commit();
			}
		} catch (Exception e) {
			log.error("", e);
		} finally {
			if(jgt != null && jgt.isOpen()){
				jgt.commit();
				jgt.close();
			}
			if(graph != null && graph.isOpen())
				graph.close();
		}
	}
}
