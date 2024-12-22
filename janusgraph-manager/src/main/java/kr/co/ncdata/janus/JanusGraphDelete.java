package kr.co.ncdata.janus;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.List;

public class JanusGraphDelete {
	JanusGraph graph;
	GraphTraversalSource g;

	public JanusGraphDelete() {
		graph = JanusGraphFactory.open(JanusManager.PROP_FILE_NAME);
		g = graph.traversal();
	}

	public static void main(String[] args) throws Exception {
		JanusGraphDelete jm = new JanusGraphDelete();
		jm.proc();
	}

	public void proc() {
		System.out.println("start drop");

		int size = 10000;
		int count = 0;
		boolean hasMore = true;

		while (hasMore) {
			count += size;
			System.out.println("count: " + count);
			List<Vertex> vertices = g.V().limit(size).toList();
			if (vertices.isEmpty()) {
				hasMore = false;
			} else {
				for (Vertex v : vertices) {
					v.remove();
				}
				graph.tx().commit();
			}
		}

		/*
		int index = 0;

		while (index < count) {
			System.out.println("index:" + index);
			index += size;
			g.V().limit(size).drop();
			g.tx().commit();
			g.tx().open();
		}

		g.V().limit(size).drop();
		g.tx().commit();
		g.tx().close();
		 */

		System.out.println("count: " + g.V().count().next());
		if (graph != null && graph.isOpen()) {
			graph.close();
		}
		System.out.println("end drop");
	}
}
