package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;

import java.io.IOException;
import java.util.NoSuchElementException;

public class MongoSchemalessFeatureReader extends SchemalessFeatureReader{
    DBCursor cursor;
    SchemalessFeatureSource featureSource;

    public MongoSchemalessFeatureReader (DBCursor cursor, SchemalessFeatureSource featureSource){
        this.cursor=cursor;
        this.featureSource=featureSource;
    }

    @Override
    public FeatureType getFeatureType() {
        return featureSource.getSchema();
    }

    @Override
    public boolean hasNext() throws IOException {
        return cursor.hasNext();
    }

    @Override
    public Feature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {
        DBObject obj = cursor.next();

        return (Feature) new MongoToComplexFeature(featureSource.getSchema()).buildFeature(obj);
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }

}
