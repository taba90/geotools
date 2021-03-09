/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2015, Open Source Geospatial Foundation (OSGeo)
 *    (C) 2014-2015, Boundless
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.omg.IOP.CodecFactory;

import javax.print.Doc;

public class MongoGeometryBuilder {

    GeometryFactory geometryFactory;

    public MongoGeometryBuilder() {
        this(new GeometryFactory());
    }

    public MongoGeometryBuilder(GeometryFactory geomFactory) {
        this.geometryFactory = geomFactory;
    }

    public Geometry toGeometry(BsonDocument document) {
        if (document == null) {
            return null;
        }
        String type = document.getString("type").getValue();
        Geometries g = Geometries.getForName(type);
        if (g == null) {
            throw new IllegalArgumentException("Unable to create geometry of type: " + type);
        }

        BsonArray list = document.getArray("coordinates");
        switch (g) {
            case POINT:
                return toPoint(list);
            case LINESTRING:
                return toLineString(list);
            case POLYGON:
                return toPolygon(list);
            case MULTIPOINT:
                return toMultiPoint(list);
            case MULTILINESTRING:
                return toMultiLineString(list);
            case MULTIPOLYGON:
                return toMultiPolygon(list);
            case GEOMETRYCOLLECTION:
                return toGeometryCollection(document.getArray("geometries"));
            default:
                throw new IllegalArgumentException("Unknown geometry type: " + type);
        }
    }

    public BsonDocument toBsonDocument(Envelope envelope) {
        return toBsonDocument(geometryFactory.toGeometry(envelope));
    }

    public BsonDocument toBsonDocument(Geometry geom) {
        Geometries g = Geometries.get(geom);
        switch (g) {
            case POINT:
                return toBsonDocument((Point) geom);
            case LINESTRING:
                return toBsonDocument((LineString) geom);
            case POLYGON:
                return toBsonDocument((Polygon) geom);
            case MULTIPOINT:
                return toBsonDocument((MultiPoint) geom);
            case MULTILINESTRING:
                return toBsonDocument((MultiLineString) geom);
            case MULTIPOLYGON:
                return toBsonDocument((MultiPolygon) geom);
            case GEOMETRYCOLLECTION:
                return toBsonDocument((GeometryCollection) geom);
            default:
                throw new IllegalArgumentException("Unknown geometry type: " + geom);
        }
    }

    public GeometryCollection toGeometryCollection(BsonArray bsonArray) {
        List<Geometry> geoms = new ArrayList<>();
        for (BsonValue value : bsonArray) {
            geoms.add(toGeometry(value.asDocument()));
        }
        return geometryFactory.createGeometryCollection(geoms.toArray(new Geometry[geoms.size()]));
    }

    public BsonDocument toBsonDocument(GeometryCollection gc) {
        return null;
    }

    public MultiPolygon toMultiPolygon(BsonArray array) {
        List<Polygon> polys = new ArrayList<>();
        for (BsonValue value : array) {
            polys.add(toPolygon(value.asArray()));
        }
        return geometryFactory.createMultiPolygon(polys.toArray(new Polygon[polys.size()]));
    }

    public BsonDocument toBsonDocument(MultiPolygon mp) {
        BsonArray array = new BsonArray();
        for (int i = 0; i < mp.getNumGeometries(); i++) {
            array.add(toBsonArray(((Polygon) mp.getGeometryN(i))));
        }
        return new BsonDocument()
                .append("type", new BsonString("MultiPolygon"))
                .append("coordinates", array);
    }

    public MultiLineString toMultiLineString(BsonArray array) {
        List<LineString> lines = new ArrayList<>();
        for (BsonValue value : array) {
            lines.add(toLineString(value.asArray()));
        }
        return geometryFactory.createMultiLineString(lines.toArray(new LineString[lines.size()]));
    }

    public BsonDocument toBsonDocument(MultiLineString ml) {
        BsonArray array = new BsonArray();
        for (int i = 0; i < ml.getNumGeometries(); i++) {
            array.add(toBsonArray(((LineString) ml.getGeometryN(i)).getCoordinateSequence()));
        }
        return new BsonDocument()
                .append("type", new BsonString("MultiLineString"))
                .append("coordinates", array);
    }

    public MultiPoint toMultiPoint(BsonArray array) {
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            points.add(toPoint(array.get(i).asArray()));
        }
        return geometryFactory.createMultiPoint(points.toArray(new Point[points.size()]));
    }

    public BsonDocument toBsonDocument(MultiPoint mp) {
        BsonDocument document = new BsonDocument();
        return document.append("type", new BsonString("MultiPoint"))
                .append("coordinates", toBsonArray(mp.getCoordinates()));
    }

    public Polygon toPolygon(BsonArray array) {
        LinearRing outer = (LinearRing) toLineString(array.get(0).asArray());
        List<LinearRing> inner = new ArrayList<>();
        for (int i = 1; i < array.size(); i++) {
            inner.add((LinearRing) toLineString(array.get(i).asArray()));
        }
        return geometryFactory.createPolygon(outer, inner.toArray(new LinearRing[inner.size()]));
    }

    public BsonDocument toBsonDocument(Polygon p) {
        return new BsonDocument()
                .append("type", new BsonString("Polygon"))
                .append("coordinates", toBsonArray(p));
    }

    public LineString toLineString(BsonArray array) {
        List<Coordinate> coordList = new ArrayList<>(array.size());
        for (BsonValue value : array) {
            if (value.isArray()) {
                coordList.add(toCoordinate(value.asArray()));
            }
        }
        if (coordList.isEmpty()) coordList.add(toCoordinate(array));

        Coordinate[] coords = coordList.toArray(new Coordinate[coordList.size()]);
        if (coords.length > 3 && coords[0].equals(coords[coords.length - 1])) {
            return geometryFactory.createLinearRing(coords);
        }
        return geometryFactory.createLineString(coords);
    }

    public BsonDocument toBsonDocument(LineString l) {
        return new BsonDocument()
                .append("type", new BsonString("LineString"))
                .append("coordinates", toBsonArray(l.getCoordinateSequence()));
    }

    public Point toPoint(BsonArray array) {
        return geometryFactory.createPoint(toCoordinate(array));
    }

    public BsonDocument toBsonDocument(Point p) {
        BsonDocument document = new BsonDocument();
        return document.append("type", new BsonString("Point"))
                .append("coordinates", toBsonArray(p.getCoordinate()));
    }

    public Coordinate toCoordinate(BsonArray array) {
        double x = array.get(0).asDouble().doubleValue();
        double y = array.get(1).asDouble().doubleValue();
        return new Coordinate(x, y);
    }

    BsonArray toBsonArray(Coordinate c) {
        BsonArray array = new BsonArray();
        array.add(new BsonDouble(c.x));
        array.add(new BsonDouble(c.y));
        return array;
    }

    BsonArray toBsonArray(CoordinateSequence cs) {
        BsonArray array = new BsonArray();
        for (int i = 0; i < cs.size(); i++) {
            BsonArray m = new BsonArray();
            m.add(new BsonDouble(cs.getX(i)));
            m.add(new BsonDouble(cs.getY(i)));
            array.add(m);
        }
        return array;
    }

    BsonArray toBsonArray(Coordinate[] cs) {
        BsonArray array = new BsonArray();
        for (Coordinate c : cs) {
            BsonArray coor = new BsonArray();
            coor.add(new BsonDouble(c.x));
            coor.add(new BsonDouble(c.y));
            array.add(coor);
        }
        return array;
    }

    BsonArray toBsonArray(Polygon p) {
        BsonArray array = new BsonArray();

        if (!Orientation.isCCW(p.getExteriorRing().getCoordinates())) {
            array.add(toBsonArray(p.getExteriorRing().reverse().getCoordinates()));
        } else {
            array.add(toBsonArray(p.getExteriorRing().getCoordinateSequence()));
        }

        for (int i = 0; i < p.getNumInteriorRing(); i++) {
            array.add(toBsonArray(p.getInteriorRingN(i).getCoordinateSequence()));
        }

        return array;
    }
}
