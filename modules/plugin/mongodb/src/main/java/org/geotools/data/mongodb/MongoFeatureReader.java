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

import com.mongodb.DBObject;
import java.io.IOException;
import java.util.NoSuchElementException;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.geotools.data.simple.SimpleFeatureReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import javax.print.Doc;

public class MongoFeatureReader implements SimpleFeatureReader {

    MongoCursor<Document> cursor;
    MongoFeatureSource featureSource;
    CollectionMapper mapper;

    public MongoFeatureReader(MongoCursor<Document> cursor, MongoFeatureSource featureSource) {
        this.cursor = cursor;
        this.featureSource = featureSource;
        mapper = featureSource.getMapper();
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return featureSource.getSchema();
    }

    @Override
    public boolean hasNext() throws IOException {
        return cursor.hasNext();
    }

    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {
        Document obj = cursor.next();

        return mapper.buildFeature(obj, featureSource.getSchema());
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }
}
