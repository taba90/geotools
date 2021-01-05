package org.geotools.data.mongodb.complex.schemaless;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.Feature;

public class SchemalessFeatureIterator implements FeatureIterator<Feature> {

    private SchemalessFeatureReader featureReader;

    public SchemalessFeatureIterator (SchemalessFeatureReader reader){
        this.featureReader=reader;
    }

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
