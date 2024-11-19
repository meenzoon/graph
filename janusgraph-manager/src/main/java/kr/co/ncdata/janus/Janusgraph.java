package kr.co.ncdata.janus;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;

public class Janusgraph {
	public static void main(String[] args) throws Exception {
		JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-hbase.properties");
		//GraphTraversalSource g = graph.traversal();

		JanusGraphTransaction tx = graph.newTransaction();
		try {
			Vertex v1 = tx.addVertex(T.label, "person", "name", "John", "age", 30);
			Vertex v2 = tx.addVertex(T.label, "person", "name", "Jane", "age", 28);
			v1.addEdge("knows", v2, "since", 2010);

			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		} finally {
			graph.close();
		}
	}
}
