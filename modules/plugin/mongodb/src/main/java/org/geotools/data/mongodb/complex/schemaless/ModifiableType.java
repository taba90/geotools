package org.geotools.data.mongodb.complex.schemaless;

import org.opengis.feature.type.ComplexType;
import org.opengis.feature.type.PropertyDescriptor;

public interface ModifiableType extends Cloneable, ComplexType {

    void addPropertyDescriptor(PropertyDescriptor descriptor);
}
