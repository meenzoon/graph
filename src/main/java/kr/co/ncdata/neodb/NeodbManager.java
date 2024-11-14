package kr.co.ncdata.neodb;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Values;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
//import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;

public class NeodbManager {
	public void proc() throws Exception {
		System.out.println("proc");
		File f = new File("");
		PbfReader pr = new PbfReader(f, 1);
		pr.setSink(new Sink() {
			Driver driver;
			Session session;
			Transaction ta;
			int count = 0;
			int exeCount = 0;
			
			@Override
			public void initialize(Map<String, Object> metaData) {
				driver = GraphDatabase.driver("neo4j://localhost:7687", AuthTokens.basic("", ""));
				session = driver.session();
				ta = session.beginTransaction();
				System.out.println("initialize");
			}

			@Override
			public void complete() {
				System.out.println("complete");
			}

			@Override
			public void close() {
				if (ta != null && ta.isOpen()) {
					ta.commit();
					ta.close();
				}
				
				if (session != null && session.isOpen()) {
					session.close();
				}
				
				if (driver != null) {
					driver.close();
				}
				System.out.println("close");
			}

			@Override
			public void process(EntityContainer entityContainer) {
				Entity entity = entityContainer.getEntity();
				exeCount++;
				if (exeCount % 100000 == 0) {
					System.out.println("exeCount: " + exeCount);
				}
				
				switch (entity.getType()) {
					case Node:
						if (true)
							return;
						Node node = (Node) entity;
						createNode(ta, node);
						
						break;
					case Way:
						Way way = (Way) entity;
//						System.out.println("Way: " + entity.toString());
						createWay(ta, way);
						
						break;
					case Relation:
						Relation rel = (Relation) entity;
//						System.out.println("Relation: " + entity.toString());
						createRelation(ta, rel);
						
						break;
					default:
						System.out.println("error");
						break;
                }
				
				if (++count % 1 == 0) {
					ta.commit();
					ta.close();
					System.out.println("count: " + count);
					ta = session.beginTransaction();
//					jgt.rollback();
				}
			}
			
		});
		pr.run();
//		OsmosisReader or = new OsmosisReader(f);
//		or.setSink(new OsmReader());
//		or.run();
	}
	
	private void createNode(Transaction ta, Node osmNode) {
        ta.run("CREATE (n:OSMNode {id: $id, latitude: $lat, longitude: $lon})",
                    Values.parameters("id", osmNode.getId(), 
                                      "lat", osmNode.getLatitude(),
                                      "lon", osmNode.getLongitude()));
    }

    private void createWay(Transaction ta, Way osmWay) {
        // Way를 Neo4j에 저장
//        ta.run("CREATE (w:OSMWay {id: $id})", Values.parameters("id", osmWay.getId()));
        // Way의 노드와 연결
        List<WayNode> nodeIds = osmWay.getWayNodes();
        for (WayNode node : nodeIds) {
//        	System.out.println("wayId: " + osmWay.getId() + ", node: " + node.getNodeId());
            
        	ta.run("MATCH (n:OSMNode {id: $nodeId}) " +
                        "MATCH (w:OSMWay {id: $id}) " +
                        "CREATE (n)-[:PART_OF]->(w)", 
                        Values.parameters("nodeId", node.getNodeId(), "id", osmWay.getId()));
                        
        }
    }

    private void createRelation(Transaction ta, Relation osmRelation) {
        // Relation을 Neo4j에 저장
//        ta.run("CREATE (r:OSMRelation {id: $id})", Values.parameters("id", osmRelation.getId()));
        // Relation의 멤버 노드 및 Way와 연결
        osmRelation.getMembers().forEach(member -> {
            String memberType = member.getMemberType().name();
            long memberId = member.getMemberId();
//            System.out.println("RelId: " + osmRelation.getId() + ", memberType: " + memberType + ", memberId: " + memberId);
            
            ta.run("MATCH (r:OSMRelation {id: $id}) " +
                        "MATCH (m) " + // 멤버가 Node 또는 Way가 될 수 있음
                        "WHERE m.id = $memberId " +
                        "CREATE (m)-[:PART_OF]->(r)",
                        Values.parameters("id", osmRelation.getId(), "memberId", memberId));
                        
        });
    }
	
	public static void main(String[] args) throws Exception {
		System.out.println("test");
		NeodbManager nm = new NeodbManager();
		nm.proc();
	}
}
