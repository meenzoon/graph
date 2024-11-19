package kr.co.ncdata.janus;

import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;

import java.io.BufferedReader;
import java.io.FileReader;

public class JanusgraphManager {
	JanusGraph graph;
	JanusGraphTransaction jgt;

	public JanusgraphManager() {
		graph = JanusGraphFactory.open("conf/janusgraph-hbase.properties");
		jgt = graph.newTransaction();
	}

	public static void main(String[] args) throws Exception {
		JanusgraphManager jm = new JanusgraphManager();
		jm.proc();
	}

	public void proc() {
		int count = 0;
		int index = 0;

		try (BufferedReader reader = new BufferedReader(new FileReader(""))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] col = line.split(",");

				jgt.addVertex(T.label, "osmNode", "id", col[0], "latitude", col[1], "longitude", col[2]);

				++count;
				if (++index % 10000 == 0) {
					jgt.commit();
					jgt = graph.newTransaction();
					System.out.println("count:" + count);
					index = 0;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (jgt != null && jgt.isOpen()) {
			jgt.commit();
			jgt.close();
			System.out.println("count:" + count);
		}
		if (graph != null & graph.isOpen()) {
			graph.close();
		}
	}
}
