package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.*;
import org.geotools.data.complex.feature.type.ComplexFeatureTypeFactoryImpl;
import org.geotools.data.mongodb.CollectionMapper;
import org.geotools.data.mongodb.complex.MongoComplexUtilities;
import org.geotools.feature.*;
import org.geotools.gml3.v3_2.GMLSchema;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.*;
import org.opengis.feature.type.*;

public class MongoToComplexFeature implements CollectionMapper<FeatureType, Feature> {

    private ModifiableComplexFeatureType type;

    private AttributeBuilder attributeBuilder;

    private AttributeTypeBuilder typeBuilder;

    private boolean updateSchema = false;

    public MongoToComplexFeature(ModifiableComplexFeatureType featureType) {
        type = featureType;
        typeBuilder = new AttributeTypeBuilder(new ComplexFeatureTypeFactoryImpl());
        attributeBuilder = new AttributeBuilder(new ValidatingFeatureFactoryImpl());
    }

    @Override
    public Feature buildFeature(DBObject rootDBO, FeatureType featureType) {
        if (!(featureType instanceof ModifiableComplexFeatureType))
            throw new UnsupportedOperationException(
                    "For schemaless features generation featureType should be of type "
                            + ModifiableComplexFeatureType.class.getName());
        List<Property> attributes = new ArrayList<>();
        attributeBuilder.init();
        attributes = getNestedAttributes(rootDBO, type);
        ComplexFeatureBuilder featureBuilder = new ComplexFeatureBuilder(type);
        GeometryAttribute geometryAttribute = null;
        for (Property p : attributes) {
            featureBuilder.append(p.getName(), p);
            if (p instanceof GeometryAttribute) {
                GeometryAttribute geom = (GeometryAttribute) p;
                if (p.getName().equals(type.getGeometryDescriptor().getName()))
                    geometryAttribute = geom;
            }
        }
        Feature f = featureBuilder.buildFeature(rootDBO.get("_id").toString());
        f.setDefaultGeometryProperty(geometryAttribute);
        return f;
    }

    private List<Property> getNestedAttributes(DBObject rootDBO, AttributeType parentType) {
        Set<String> keys = rootDBO.keySet();
        String namespaceURI = type.getName().getNamespaceURI();
        List<Property> attributes = new ArrayList<>();
        for (String key : keys) {
            Object value = rootDBO.get(key);
            value = MongoComplexUtilities.convertGeometry(value, null);
            if (value instanceof Geometry) {
                GeometryAttribute geometryAttribute =
                        createGeometryAttribute((Geometry) value, namespaceURI, key, parentType);
                attributes.add(geometryAttribute);
            } else if (value != null) {
                if (value instanceof BasicDBList) {
                    BasicDBList list = (BasicDBList) value;
                    attributes.addAll(
                            getListTypeAndAttributes(namespaceURI, key, list, parentType));
                } else if (value instanceof DBObject) {
                    attributes.add(
                            getComplexAttributeTypeAndValues(
                                    namespaceURI,
                                    key,
                                    (DBObject) value,
                                    (ModifiableType) parentType,
                                    false));
                } else {
                    attributes.add(getLeafTypeAndAttribute(namespaceURI, key, value, parentType));
                }
            }
        }
        return attributes;
    }

    private Attribute getComplexAttributeTypeAndValues(
            String namespaceURI,
            String attrName,
            DBObject dbobject,
            ModifiableType parentType,
            boolean isCollection) {
        PropertyDescriptor propertyDescriptor = parentType.getDescriptor(attrName);
        PropertyDescriptor descriptor =
                propertyDescriptor != null
                        ? ((ComplexType) ((AttributeDescriptor) propertyDescriptor).getType())
                                .getDescriptors()
                                .iterator()
                                .next()
                        : null;
        ModifiableType complexPropertyType = null;
        ModifiableType complexType = null;
        if (propertyDescriptor != null) {
            complexPropertyType = (ModifiableType) propertyDescriptor.getType();
            complexType =
                    (ModifiableType)
                            complexPropertyType.getDescriptors().iterator().next().getType();
        } else {
            this.updateSchema = true;
            complexPropertyType = createComplexType(namespaceURI, attrName + "PropertyType");
            propertyDescriptor = createDescriptor(isCollection, attrName, complexPropertyType);
            parentType.addPropertyDescriptor(propertyDescriptor);
            complexType = createComplexType(namespaceURI, attrName + "Type");
            String nameCapitalized = attrName.substring(0, 1).toUpperCase() + attrName.substring(1);
            descriptor = createDescriptor(false, nameCapitalized, complexType);
            complexPropertyType.addPropertyDescriptor(descriptor);
        }
        List<Property> attributes = getNestedAttributes(dbobject, complexType);
        ComplexAttribute attribute =
                attributeBuilder.createComplexAttribute(
                        attributes, complexType, (AttributeDescriptor) descriptor, null);
        ComplexAttribute propertyType =
                attributeBuilder.createComplexAttribute(
                        Arrays.asList((Property) attribute),
                        complexPropertyType,
                        (AttributeDescriptor) propertyDescriptor,
                        null);
        return propertyType;
    }

    private List<Property> getListTypeAndAttributes(
            String namespaceURI, String attrName, BasicDBList value, AttributeType parentType) {
        List<Property> attributes = new ArrayList<>();
        for (int i = 0; i < value.size(); i++) {
            Object obj = value.get(i);
            if (obj != null && obj instanceof DBObject) {
                Attribute attribute =
                        getComplexAttributeTypeAndValues(
                                namespaceURI,
                                attrName,
                                (DBObject) obj,
                                (ModifiableType) parentType,
                                true);
                attributes.add(attribute);
            } else {
                if (obj!=null)
                    attributes.add(getLeafTypeAndAttribute(namespaceURI, attrName, obj, parentType));
            }
        }
        return attributes;
    }

    private Attribute getLeafTypeAndAttribute(
            String namespaceURI, String attrName, Object value, AttributeType parentType) {
        PropertyDescriptor attrDescriptor = ((ComplexType) parentType).getDescriptor(attrName);
        if (attrDescriptor == null) {
            this.updateSchema = true;
            typeBuilder.setBinding(value.getClass());
            typeBuilder.setName(attrName);
            typeBuilder.setNamespaceURI(namespaceURI);
            typeBuilder.setMaxOccurs(1);
            typeBuilder.setMinOccurs(0);
            AttributeType attrType = typeBuilder.buildType();
            attrDescriptor = typeBuilder.buildDescriptor(attrType.getName(), attrType);
            if (parentType instanceof ModifiableType) {
                ((ModifiableType) parentType).addPropertyDescriptor(attrDescriptor);
            }
        }
        attributeBuilder.setDescriptor((AttributeDescriptor) attrDescriptor);
        return attributeBuilder.buildSimple(null, value);
    }

    private GeometryAttribute createGeometryAttribute(
            Geometry geom, String namespaceURI, String name, AttributeType parentType) {
        ComplexType complexType = (ComplexType) parentType;
        AttributeDescriptor descriptor = (AttributeDescriptor) complexType.getDescriptor(name);
        if (descriptor == null) {
            typeBuilder.setBinding(geom.getClass());
            typeBuilder.setName(name);
            typeBuilder.setNamespaceURI(namespaceURI);
            typeBuilder.setMaxOccurs(1);
            typeBuilder.setMinOccurs(0);
            typeBuilder.setAbstract(false);
            GeometryType attrType = typeBuilder.buildGeometryType();
            descriptor = typeBuilder.buildDescriptor(attrType.getName(), attrType);
            if (parentType instanceof ModifiableType)
                ((ModifiableType) parentType).addPropertyDescriptor(descriptor);
        }
        attributeBuilder.setDescriptor(descriptor);
        return (GeometryAttribute) attributeBuilder.buildSimple(null, geom);
    }

    public boolean schemaNeedsUpdate() {
        return updateSchema;
    }

    public ModifiableComplexFeatureType getType() {
        return type;
    }

    public void setType(ModifiableComplexFeatureType type) {
        this.type = type;
    }

    private AttributeDescriptor createDescriptor(
            boolean isCollection, String attrName, ComplexType complexType) {
        typeBuilder.setMinOccurs(0);
        typeBuilder.setMaxOccurs(isCollection ? Integer.MAX_VALUE : 1);
        AttributeDescriptor descriptor = typeBuilder.buildDescriptor(attrName, complexType);
        return descriptor;
    }

    private ModifiableType createComplexType(String namespaceURI, String typeName) {
        String nameCapitalized = typeName.substring(0, 1).toUpperCase() + typeName.substring(1);
        return new ModifiableComplexTypeImpl(
                new NameImpl(namespaceURI, nameCapitalized),
                Collections.emptyList(),
                false,
                false,
                Collections.emptyList(),
                GMLSchema.ABSTRACTGMLTYPE_TYPE,
                null);
    }

    @Override
    public Geometry getGeometry(DBObject obj) {
        return null;
    }

    @Override
    public void setGeometry(DBObject obj, Geometry g) {}

    @Override
    public DBObject toObject(Geometry g) {
        return null;
    }

    @Override
    public String getGeometryPath() {
        GeometryDescriptor descriptor = type.getGeometryDescriptor();
        if (descriptor != null) {
            return descriptor.getType().getUserData().get("GEOMETRY_PATH").toString();
        }
        return null;
    }

    @Override
    public String getPropertyPath(String property) {
        String[] splittedPn = property.split("/");
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < splittedPn.length; i++) {
            String xpathStep = splittedPn[i];
            if (xpathStep.indexOf(":") != -1) xpathStep = xpathStep.split(":")[1];
            sb.append(xpathStep);
            if (i != splittedPn.length - 1) sb.append(".");
        }
        return sb.toString();
    }

    @Override
    public FeatureType buildFeatureType(Name name, DBCollection collection) {
        return null;
    }
}
