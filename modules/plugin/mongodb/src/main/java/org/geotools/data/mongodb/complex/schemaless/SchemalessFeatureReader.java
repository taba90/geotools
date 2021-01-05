package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.geotools.data.FeatureReader;
import org.geotools.data.mongodb.CollectionMapper;
import org.geotools.data.mongodb.MongoFeatureSource;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;

import java.io.IOException;
import java.util.NoSuchElementException;

public abstract class SchemalessFeatureReader implements FeatureReader<FeatureType, Feature> {

}
