package kr.co.ncdata.janus.helper;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.util.Map;

public class OsmReader implements Sink {
	@Override
	public void initialize(Map<String, Object> metaData) {
	}

	@Override
	public void complete() {
	}

	@Override
	public void close() {
	}

	@Override
	public void process(EntityContainer entityContainer) {
		if (entityContainer instanceof NodeContainer) {
			System.out.println("NodeContainer Entity!");
		} else if (entityContainer instanceof WayContainer) {
			Way way = ((WayContainer) entityContainer).getEntity();
			for (Tag tag : way.getTags()) {
				if ("highway".equalsIgnoreCase(tag.getKey())) {
					System.out.println("it's a highway: " + way.getId());
					break;
				}
			}
		} else if (entityContainer instanceof RelationContainer) {
			System.out.println("RelationContainer Entity!");
		} else {
			System.out.println("Unknown Entity!");
		}
	}
}
