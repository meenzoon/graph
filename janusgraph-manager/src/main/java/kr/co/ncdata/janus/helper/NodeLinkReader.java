package kr.co.ncdata.janus.helper;

import kr.co.ncdata.janus.JanusConfig;
import kr.co.ncdata.janus.vo.MoctLinkVo;
import kr.co.ncdata.janus.vo.MoctNodeVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class NodeLinkReader {
	ShapefileDataStore dataStore;

	public static void main(String[] args) {
		NodeLinkReader reader = new NodeLinkReader();
		//reader.readShp(JanusConfig.ITS_LINK_FILE);
		List<MoctNodeVo> result = reader.readNode();
		log.info("result size: {}", result.size());
	}

	private SimpleFeatureIterator readShp(String filePath) throws IOException {
		System.out.println("filePath: " + filePath);
		File file = new File(filePath);
		dataStore = new ShapefileDataStore(file.toURI().toURL());
		String typeName = dataStore.getTypeNames()[0];
		SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);

		return featureSource.getFeatures().features();
	}

	public void close() {
		if (dataStore != null) {
			dataStore.dispose();
		}
	}

	/**
	 * 국가 표준 노드 정보 읽기
	 * @return 노드 목록
	 */
	public List<MoctNodeVo> readNode() {
		List<MoctNodeVo> moctNodeVoList = new ArrayList<>();
		try {
			SimpleFeatureIterator features = this.readShp(JanusConfig.ITS_NODE_FILE);


			while (features.hasNext()) {
				SimpleFeature feature = features.next();
				// 피처 데이터 처리
				String nodeId = feature.getAttribute("NODE_ID").toString();
				String nodeType = feature.getAttribute("NODE_TYPE").toString();
				String nodeName = feature.getAttribute("NODE_NAME").toString();
				String turnP = feature.getAttribute("TURN_P").toString();
				Point point = (Point) feature.getAttribute("the_geom");

				MoctNodeVo nodeVo = new MoctNodeVo(nodeId, nodeType, nodeName, turnP, point);
				moctNodeVoList.add(nodeVo);
			}
		} catch (IOException e) {
			log.error("", e);
		} finally {
			this.close();
		}
		return moctNodeVoList;
	}

	/**
	 * 국가 표준 링크 정보 읽기
	 * @return 링크 목록
	 */
	public List<MoctLinkVo> readLink() {
		List<MoctLinkVo> moctLinkVoMap = new ArrayList<>();
		try {
			SimpleFeatureIterator features = readShp(JanusConfig.ITS_LINK_FILE);

			while (features.hasNext()) {
				SimpleFeature feature = features.next();
				// 피처 데이터 처리
				String linkId = feature.getAttribute("LINK_ID").toString();
				String startNode = feature.getAttribute("F_NODE").toString();
				String endNode = feature.getAttribute("T_NODE").toString();
				int lanes = NumberUtils.toInt(feature.getAttribute("LANES").toString());
				String roadRank = feature.getAttribute("ROAD_RANK").toString();
				String roadType = feature.getAttribute("ROAD_TYPE").toString();
				String roadNo = feature.getAttribute("ROAD_NO").toString();
				String roadName = feature.getAttribute("ROAD_NAME").toString();
				MultiLineString geom = (MultiLineString) feature.getAttribute("the_geom");
				int maxSpd = NumberUtils.toInt(feature.getAttribute("MAX_SPD").toString());
				double length = NumberUtils.toDouble(feature.getAttribute("LENGTH").toString());

				MoctLinkVo linkVo =
					new MoctLinkVo(linkId, startNode, endNode, lanes, roadRank, roadType, roadNo, roadName, geom,
						maxSpd, length);
				moctLinkVoMap.add(linkVo);
			}
		} catch (IOException e) {
			log.error("", e);
		} finally {
			this.close();
		}
		return moctLinkVoMap;
	}
}
