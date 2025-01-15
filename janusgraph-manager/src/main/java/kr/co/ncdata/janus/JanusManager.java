package kr.co.ncdata.janus;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class JanusManager {
	@Getter
	private static JanusGraph graph;
	@Getter
	private static GraphTraversalSource traversalSource;

	public static void closeGraph() {
		try {
			traversalSource.close();
		} catch (Exception e) {
			log.error("", e);
		}
		if (graph != null && graph.isOpen()) {
			graph.close();
		}
	}

	public static void initGraph(String fileName) throws RuntimeException {
		// 파일이 있는지 체크
		if (!Files.exists(Paths.get(fileName))) {
			throw new RuntimeException("fileName" + fileName + " is not exist");
		}

		graph = JanusGraphFactory.open(fileName);
		traversalSource = graph.traversal();

		if (graph == null || graph.isClosed()) {
			throw new RuntimeException("graph is not open");
		}
	}
}
