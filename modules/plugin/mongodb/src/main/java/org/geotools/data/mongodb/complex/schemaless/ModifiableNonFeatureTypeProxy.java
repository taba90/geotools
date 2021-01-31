package org.geotools.data.mongodb.complex.schemaless;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.geotools.data.complex.feature.type.ComplexTypeProxy;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.ComplexType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class ModifiableNonFeatureTypeProxy extends ComplexTypeProxy
        implements FeatureType, ModifiableType {
    /** The attribute descriptors */
    private Collection<PropertyDescriptor> descriptors;

    /** The real type */
    private AttributeType subject;

    /**
     * Sole constructor
     *
     * @param type The underlying non feature type
     */
    public ModifiableNonFeatureTypeProxy(final AttributeType type) {
        super(type.getName(), null);
        this.subject = type;
        this.descriptors = new ArrayList<>();
        if (subject instanceof ComplexType) {
            descriptors.addAll(((ComplexType) subject).getDescriptors());
        }
    }

    /** @see ComplexTypeProxy#getSubject() */
    @Override
    public AttributeType getSubject() {
        return subject;
    }

    @Override
    public PropertyDescriptor getDescriptor(Name name) {
        PropertyDescriptor descriptor = null;
        for (PropertyDescriptor d : descriptors) {
            if (d.getName().equals(name)) {
                descriptor = d;
                break;
            }
        }
        return descriptor;
    }

    @Override
    public Collection<PropertyDescriptor> getDescriptors() {
        return descriptors;
    }

    /** Return only the schema descriptors */
    public Collection<PropertyDescriptor> getTypeDescriptors() {
        if (subject instanceof ComplexType) {
            return ((ComplexType) subject).getDescriptors();
        } else {
            return Collections.emptyList();
        }
    }

    public CoordinateReferenceSystem getCoordinateReferenceSystem() {
        return null;
    }

    public GeometryDescriptor getGeometryDescriptor() {
        return null;
    }

    @Override
    public void addPropertyDescriptor(PropertyDescriptor descriptor) {
        descriptors.add(descriptor);
    }
}
