package kr.co.ncdata.janus.helper;

import org.janusgraph.core.attribute.Geoshape;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.spatial4j.shape.ShapeFactory;

public class GeoHelper {
	/**
	 * MultiLineString -> janusgraph Geoshape 형태로 변환
	 * @param multiLineString geotools geometry 형태 MultiLineString
	 * @return janusgraph Geoshape
	 */
	public static Geoshape convertMultiLineString(MultiLineString multiLineString) {
		ShapeFactory.MultiLineStringBuilder builder = Geoshape.getShapeFactory().multiLineString();
		for (int i = 0, size = multiLineString.getNumGeometries(); i < size; i++) {
			LineString lineString = (LineString) multiLineString.getGeometryN(i);
			Coordinate[] coordinates = lineString.getCoordinates();

			ShapeFactory.LineStringBuilder lineStringBuilder = Geoshape.getShapeFactory().lineString();
			for (Coordinate coord : coordinates) {
				lineStringBuilder.pointXY(coord.x, coord.y);
			}
			builder.add(lineStringBuilder);

		}

		return Geoshape.geoshape(builder.build());
	}
}
