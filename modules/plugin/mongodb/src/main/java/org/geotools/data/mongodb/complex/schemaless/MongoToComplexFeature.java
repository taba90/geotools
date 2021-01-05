package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.geotools.data.complex.feature.type.ComplexFeatureTypeFactoryImpl;
import org.geotools.data.mongodb.complex.MongoComplexUtilities;
import org.geotools.feature.*;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.Attribute;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.Property;
import org.opengis.feature.type.*;
import org.w3c.dom.Attr;

public class MongoToComplexFeature {

    private FeatureTypeFactory ftFactory;

    private FeatureType type;

    public MongoToComplexFeature(FeatureType featureType) {
        ftFactory = new ComplexFeatureTypeFactoryImpl();
        type = featureType;
    }

    public Object buildFeature(DBObject rootDBO) {
        Set<String> keys = rootDBO.keySet();
        List<Property> attributes = new ArrayList<>();
        String namespaceURI = type.getName().getNamespaceURI();
        ModifiableComplexFeatureType featureType=new ModifiableComplexFeatureType(type.getName(),new ArrayList<>(type.getDescriptors()),type.getGeometryDescriptor(),
                false,Collections.emptyList(), null,null);
        GeometryAttribute geometryAttribute=null;
        for (String key : keys) {
            Object value = rootDBO.get(key);
            value=MongoComplexUtilities.convertGeometry(value,null);
            if (value instanceof Geometry){
                geometryAttribute=createGeometryAttribute((Geometry)value,namespaceURI,type.getGeometryDescriptor().getName().getLocalPart(), featureType);
                attributes.add(geometryAttribute);
            } else {
                if (value instanceof DBObject) {
                    attributes.add(getComplexAttributeTypeAndValues(namespaceURI, key, (DBObject) value, featureType));
                } else if (value instanceof BasicDBList)
                    attributes.add(getListTypeAndAttributes(namespaceURI, key, (BasicDBList) value, featureType));
                else if (!(value instanceof ObjectId)) {
                    attributes.add(getLeafTypeAndAttribute(namespaceURI, key, value, featureType));
                }
            }
        }
        ComplexFeatureBuilder featureBuilder = new ComplexFeatureBuilder(featureType);
        for (Property p:attributes){
            featureBuilder.append(p.getName(),p);
        }
        Feature f=featureBuilder.buildFeature(rootDBO.get("_id").toString());
        f.setDefaultGeometryProperty(geometryAttribute);
        return f;
    }

    private List<Property> getNestedAttributes(DBObject rootDBO, AttributeType parentType) {
        Set<String> keys = rootDBO.keySet();
        String namespaceURI = type.getName().getNamespaceURI();
        List<Property> attributes = new ArrayList<>();
        for (String key : keys) {
            Object value = rootDBO.get(key);
            if (value !=null) {
                if (value instanceof BasicDBList) {
                    BasicDBList list = (BasicDBList) value;
                    attributes.add(getListTypeAndAttributes(namespaceURI, key, list,parentType));
                } else if (value instanceof DBObject) {
                            attributes.add(getComplexAttributeTypeAndValues(namespaceURI, key, (DBObject) value, parentType));
                } else {
                    attributes.add(getLeafTypeAndAttribute(namespaceURI, key, value,parentType));
                }
            }
        }
        return attributes;
    }

    private Attribute getComplexAttributeTypeAndValues(
            String namespaceURI, String attrName, DBObject dbobject, AttributeType parentType) {
        ComplexType complexType =
                new ModifiableComplexTypeImpl(
                        new NameImpl(namespaceURI, attrName),
                        Collections.emptyList(),
                        false,
                        false,
                        Collections.emptyList(),
                        null,
                        null);
        List<Property> attributes = getNestedAttributes(dbobject, complexType);
        AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder(ftFactory);
        attributeBuilder.setMinOccurs(0);
        attributeBuilder.setMaxOccurs(1);
        AttributeDescriptor descriptor = attributeBuilder.buildDescriptor(attrName, complexType);
        AttributeBuilder builder = new AttributeBuilder(new ValidatingFeatureFactoryImpl());
        builder.setDescriptor(descriptor);
        if (parentType instanceof ModifiableType)
            ((ModifiableType)parentType).addPropertyDescriptor(descriptor);
        return builder.createComplexAttribute(attributes,complexType,descriptor,null);
    }

    private Attribute getListTypeAndAttributes(
            String namespaceURI, String attrName, BasicDBList value, AttributeType parentType) {
        AttributeBuilder builder = new AttributeBuilder(new ValidatingFeatureFactoryImpl());
        ComplexType complexType =
                new ModifiableComplexTypeImpl(
                        new NameImpl(namespaceURI, attrName),
                        Collections.emptyList(),
                        false,
                        false,
                        Collections.emptyList(),
                        null,
                        null);
        AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder(ftFactory);
        attributeBuilder.setMinOccurs(0);
        attributeBuilder.setMaxOccurs(Integer.MAX_VALUE);
        AttributeDescriptor descriptor = attributeBuilder.buildDescriptor(attrName, complexType);
        if (parentType instanceof ModifiableComplexTypeImpl)
            ((ModifiableType)parentType).addPropertyDescriptor(descriptor);
        List<Property> attributes=new ArrayList<>();
        for (int i = 0; i < value.size(); i++) {
            Object obj = value.get(i);
            if (obj instanceof DBObject) {
                Attribute attribute =
                        getComplexAttributeTypeAndValues(namespaceURI, attrName, (DBObject) obj, complexType);
                attributes.add(attribute);
            } else {
                attributes.add(getLeafTypeAndAttribute(namespaceURI,attrName,obj,complexType));
            }
            //builder.add(typesAndValue.getRight(), typesAndValue.getLeft().getName());
        }
        builder.setDescriptor(descriptor);
        return builder.createComplexAttribute(attributes,complexType,descriptor,null);
    }

    private Attribute getLeafTypeAndAttribute(
            String namespaceURI, String attrName, Object value,AttributeType parentType) {
        AttributeTypeBuilder typeBuilder = new AttributeTypeBuilder(ftFactory);
        typeBuilder.setBinding(value.getClass());
        typeBuilder.setName(attrName);
        typeBuilder.setNamespaceURI(namespaceURI);
        typeBuilder.setMaxOccurs(1);
        typeBuilder.setMinOccurs(0);
        AttributeType attrType = typeBuilder.buildType();
        AttributeDescriptor attrDescriptor = typeBuilder.buildDescriptor(attrType.getName(), attrType);
        if (parentType instanceof ModifiableType){
            ((ModifiableType)parentType).addPropertyDescriptor(attrDescriptor);
        }
        AttributeBuilder builder = new AttributeBuilder(new ValidatingFeatureFactoryImpl());
        builder.setDescriptor(attrDescriptor);
        return builder.buildSimple(null,value);
    }

    private GeometryAttribute createGeometryAttribute(Geometry geom, String namespaceURI,String name, AttributeType parentType){
        AttributeTypeBuilder typeBuilder = new AttributeTypeBuilder(ftFactory);
        typeBuilder.setBinding(geom.getClass());
        typeBuilder.setName(name);
        typeBuilder.setNamespaceURI(namespaceURI);
        typeBuilder.setMaxOccurs(1);
        typeBuilder.setMinOccurs(0);
        GeometryType attrType = typeBuilder.buildGeometryType();
        AttributeDescriptor attrDescriptor = typeBuilder.buildDescriptor(attrType.getName(), attrType);
        if (parentType instanceof ModifiableType)
            ((ModifiableType)parentType).addPropertyDescriptor(attrDescriptor);
        AttributeBuilder builder = new AttributeBuilder(new ValidatingFeatureFactoryImpl());
        builder.setDescriptor(attrDescriptor);
        return (GeometryAttribute)builder.buildSimple(null,geom);
    }

}
