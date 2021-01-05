package org.geotools.data.mongodb.complex.schemaless;

import org.geotools.data.complex.feature.type.ComplexFeatureTypeImpl;
import org.geotools.feature.NameImpl;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.filter.Filter;
import org.opengis.util.InternationalString;

import java.util.*;

public class ModifiableComplexFeatureType extends ComplexFeatureTypeImpl implements ModifiableType{

    private Collection<PropertyDescriptor> schema;

    private Collection<PropertyDescriptor> properties;

    private Map<Name, PropertyDescriptor> propertyMap;

    public ModifiableComplexFeatureType(Name name, Collection<PropertyDescriptor> schema, GeometryDescriptor defaultGeometry, boolean isAbstract, List<Filter> restrictions, AttributeType superType, InternationalString description) {
        super(name, schema, defaultGeometry, isAbstract, restrictions, superType, description);
        if (schema.isEmpty())
            this.schema= new ArrayList<>();
        else
            this.schema=new ArrayList<>(schema);
        this.properties=schema;
        Map<Name, PropertyDescriptor> localPropertyMap;
        if (properties == null) {
            localPropertyMap = Collections.emptyMap();
        } else {
            localPropertyMap = new HashMap<>();
            for (PropertyDescriptor pd : properties) {
                if (pd == null) {
                    // descriptor entry may be null if a request was made for a property that does
                    // not exist
                    throw new NullPointerException(
                            "PropertyDescriptor is null - did you request a property that does not exist?");
                }
                localPropertyMap.put(pd.getName(), pd);
            }
        }
        this.propertyMap=localPropertyMap;
    }

    @Override
    public PropertyDescriptor getDescriptor(String name) {
        PropertyDescriptor result = getDescriptor(new NameImpl(name));
        if (result == null) {
            // look in the same namespace as the complex type
            result = getDescriptor(new NameImpl(getName().getNamespaceURI(), name));
            if (result == null) {
                // full scan
                for (PropertyDescriptor pd : schema) {
                    if (pd.getName().getLocalPart().equals(name)) {
                        return pd;
                    }
                }
            }
        }
        return result;
    }

    public void addPropertyDescriptor (PropertyDescriptor descriptor){
        this.schema.add(descriptor);
        this.properties.add(descriptor);
        this.propertyMap.put(descriptor.getName(),descriptor);
    }

    @Override
    public PropertyDescriptor getDescriptor(Name name) {
        return propertyMap.get(name);
    }

    @Override
    public Collection<PropertyDescriptor> getDescriptors() {
        return properties;
    }

    /**
     * Return all the descriptors that come from the schema, excluding the system descriptors, such
     * as 'FEATURE_LINK', used for linking features.
     *
     * @return schema descriptors
     */
    @Override
    public Collection<PropertyDescriptor> getTypeDescriptors() {
        return schema;
    }
}
