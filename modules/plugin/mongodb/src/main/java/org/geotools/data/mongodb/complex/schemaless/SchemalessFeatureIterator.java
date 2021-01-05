package org.geotools.data.mongodb.complex.schemaless;

import org.geotools.feature.FeatureIterator;
import org.opengis.feature.Feature;

import java.io.IOException;
import java.util.NoSuchElementException;

public class SchemalessFeatureIterator implements FeatureIterator<Feature> {

    private SchemalessFeatureReader featureReader;



    @Override
    public boolean hasNext() {
        try {
            return featureReader.hasNext();
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public Feature next() throws NoSuchElementException {
        try {
            return featureReader.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            featureReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
