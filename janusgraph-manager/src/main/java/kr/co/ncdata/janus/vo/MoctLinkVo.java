package kr.co.ncdata.janus.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.locationtech.jts.geom.MultiLineString;

@Getter
@AllArgsConstructor
public class MoctLinkVo {
	String linkId;
	String startNode;
	String endNode;
	int lanes;
	String roadRank;
	String roadType;
	String roadNo;
	String roadName;
	MultiLineString lineString;
	int maxSpd;
	double length;
}
