package kr.co.ncdata.janus;

import org.janusgraph.core.attribute.Geoshape;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.spatial4j.shape.ShapeFactory;

public class GeoHelper {
	public static Geoshape convertMultiLineString(MultiLineString multiLineString) throws Exception {
		//List<List<double[]>> lineStringList = new ArrayList<>();

		ShapeFactory.MultiLineStringBuilder builder = Geoshape.getShapeFactory().multiLineString();
		for (int i = 0, size = multiLineString.getNumGeometries(); i < size; i++) {
			LineString lineString = (LineString) multiLineString.getGeometryN(i);
			Coordinate[] coordinates = lineString.getCoordinates();
			//List<double[]> points = new ArrayList<>();

			ShapeFactory.LineStringBuilder lineStringBuilder = Geoshape.getShapeFactory().lineString();
			for (Coordinate coord : coordinates) {
				lineStringBuilder.pointXY(coord.x, coord.y);
				//points.add(new double[] {coord.x, coord.y});
			}
			builder.add(lineStringBuilder);

			//lineStringList.add(points);
		}

		return Geoshape.geoshape(builder.build());
	}
}
