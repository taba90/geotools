package org.geotools.data.mongodb;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.Name;

public interface CollectionMapper<T extends FeatureType, F extends Feature> {

    Geometry getGeometry(DBObject obj);

    void setGeometry(DBObject obj, Geometry g);

    DBObject toObject(Geometry g);

    String getGeometryPath();

    String getPropertyPath(String property);

    T buildFeatureType(Name name, DBCollection collection);

    F buildFeature(DBObject obj, T featureType);
}
