package org.geotools.data.mongodb.complex.schemaless;

import java.util.*;

import org.geotools.feature.NameImpl;
import org.geotools.feature.type.ComplexTypeImpl;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.filter.Filter;
import org.opengis.util.InternationalString;

public class ModifiableComplexTypeImpl extends ComplexTypeImpl implements ModifiableType{

    private final Collection<PropertyDescriptor> properties;

    private final Map<Name, PropertyDescriptor> propertyMap;


    public ModifiableComplexTypeImpl(
            Name name,
            Collection<PropertyDescriptor> properties,
            boolean identified,
            boolean isAbstract,
            List<Filter> restrictions,
            AttributeType superType,
            InternationalString description) {
        super(name, properties, identified, isAbstract, restrictions, superType, description);
        List<PropertyDescriptor> localProperties;
        Map<Name, PropertyDescriptor> localPropertyMap;
        if (properties == null) {
            localProperties = Collections.emptyList();
            localPropertyMap = Collections.emptyMap();
        } else {
            localProperties = new ArrayList<>(properties);
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
        this.properties = localProperties;
        this.propertyMap = localPropertyMap;
    }

    public void addPropertyDescriptor (PropertyDescriptor pd){
        if (!properties.contains(pd)){
            properties.add(pd);
            propertyMap.put(pd.getName(),pd);
        }
    }

    @Override
    public Collection<PropertyDescriptor> getDescriptors() {
        return properties;
    }

    @Override
    public PropertyDescriptor getDescriptor(Name name) {
        return propertyMap.get(name);
    }

    @Override
    public PropertyDescriptor getDescriptor(String name) {
        PropertyDescriptor result = getDescriptor(new NameImpl(name));
        if (result == null) {
            // look in the same namespace as the complex type
            result = getDescriptor(new NameImpl(getName().getNamespaceURI(), name));
            if (result == null) {
                // full scan
                for (PropertyDescriptor pd : properties) {
                    if (pd.getName().getLocalPart().equals(name)) {
                        return pd;
                    }
                }
            }
        }
        return result;
    }
}
