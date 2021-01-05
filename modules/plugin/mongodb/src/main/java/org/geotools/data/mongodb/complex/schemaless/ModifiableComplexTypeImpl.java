package org.geotools.data.mongodb.complex.schemaless;

import org.geotools.feature.type.ComplexTypeImpl;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.filter.Filter;
import org.opengis.util.InternationalString;

import java.util.Collection;
import java.util.List;

public class ModifiableComplexTypeImpl extends ComplexTypeImpl {
    public ModifiableComplexTypeImpl(Name name, Collection<PropertyDescriptor> properties, boolean identified, boolean isAbstract, List<Filter> restrictions, AttributeType superType, InternationalString description) {
        super(name, properties, identified, isAbstract, restrictions, superType, description);
    }


}
