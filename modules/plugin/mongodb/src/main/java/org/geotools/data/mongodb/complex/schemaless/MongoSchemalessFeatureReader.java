package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;

public class MongoSchemalessFeatureReader extends SchemalessFeatureReader {
    DBCursor cursor;
    SchemalessFeatureSource featureSource;
    MongoToComplexFeature mongoToComplexFeature;

    public MongoSchemalessFeatureReader(DBCursor cursor, SchemalessFeatureSource featureSource) {
        this.cursor = cursor;
        this.featureSource = featureSource;
        this.mongoToComplexFeature =
                new MongoToComplexFeature((ModifiableComplexFeatureType) featureSource.getSchema());
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
    public Feature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        DBObject obj = cursor.next();

        return mongoToComplexFeature.buildFeature(obj, getFeatureType());
    }

    @Override
    public void close() throws IOException {
        cursor.close();
        if (mongoToComplexFeature.schemaNeedsUpdate()) {
            ModifiableComplexFeatureType featureType = mongoToComplexFeature.getType();
            try {
                SchemalessFeatureTypeCache.getInstance()
                        .updateCache(featureType.getName(), featureType);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
