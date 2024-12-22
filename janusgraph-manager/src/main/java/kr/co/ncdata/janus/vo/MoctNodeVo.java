package kr.co.ncdata.janus.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.locationtech.jts.geom.Point;

@Getter
@AllArgsConstructor
public class MoctNodeVo {
	String nodeId;
	String nodeType;
	String nodeName;
	String turnP;
	Point point;
}
