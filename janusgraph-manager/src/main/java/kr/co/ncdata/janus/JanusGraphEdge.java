package kr.co.ncdata.janus;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;

import java.io.BufferedReader;
import java.io.FileReader;

public class JanusGraphEdge {
	JanusGraph graph;
	JanusGraphTransaction jgt;

	public JanusGraphEdge() {
		System.out.println("graph open");
		graph = JanusGraphFactory.open("janusgraph-hbase.properties");
		System.out.println("graph new transaction");
		jgt = graph.newTransaction();
	}

	public static void main(String[] args) {
		JanusGraphEdge edge = new JanusGraphEdge();
		edge.proc();
	}

	public void proc(){
		int count = 0;
		int index = 0;

		System.out.println("read csv");
		// /home/janus/map/daegu_way.csv
		try (BufferedReader reader = new BufferedReader(new FileReader(""))) {
			String line;
			while ((line = reader.readLine()) != null) {
				if(++count == 1)
					continue;

				String[] col = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
				String[] nodes = col[1].split(",");

				String startNode = nodes[0].replace("\"", "").replace("[", "").trim();
				String endNode = nodes[nodes.length - 1].replace("\"", "").replace("]", "").trim();
				System.out.println("id: " + col[0] + ", startNode: " + startNode + ", endNode: " + endNode);

				//Vertex startVertex = .next();
				//System.out.println("find complete startVetext");
				//Vertex endVertex = .next();

				//Vertex startVertex = jgt.getVertex(startNode);
				//Vertex endVertex = jgt.getVertex(endNode);
				//System.out.println("add Edge");
				jgt.traversal().addE("way")
					.from(__.V().has("id", startNode))
					.to(__.V().has("id", endNode));
				//startVertex.addEdge("way", endVertex);

				//jgt.addVertex(T.label, "osmNode", "id", col[0], "latitude", col[1], "longitude", col[2]);

				if (++index % 10000 == 0) {
					jgt.commit();
					jgt = graph.newTransaction();
					System.out.println("count:" + count);
					index = 0;
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
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
