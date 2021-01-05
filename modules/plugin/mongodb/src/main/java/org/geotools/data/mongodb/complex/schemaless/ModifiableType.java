package org.geotools.data.mongodb.complex.schemaless;

import org.opengis.feature.type.PropertyDescriptor;

public interface ModifiableType {

    void addPropertyDescriptor(PropertyDescriptor descriptor);
}
