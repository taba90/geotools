package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BSONObject;
import org.geotools.appschema.feature.AppSchemaAttributeBuilder;
import org.geotools.data.complex.feature.type.ComplexFeatureTypeFactoryImpl;
import org.geotools.data.complex.feature.type.ComplexFeatureTypeImpl;
import org.geotools.feature.*;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.geotools.feature.type.ComplexTypeImpl;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.Converters;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.Attribute;
import org.opengis.feature.Feature;
import org.opengis.feature.type.*;

import java.util.*;
import java.util.stream.Collectors;

public class MongoToComplexFeature {

    private FeatureTypeFactory ftFactory;

    private FeatureType type;

    public MongoToComplexFeature (FeatureType featureType){
        ftFactory=new ComplexFeatureTypeFactoryImpl();
        type=featureType;
    }


    public Object buildFeature (DBObject rootDBO){
        Set<String> keys=rootDBO.keySet();
        List<PropertyDescriptor> descriptorList=new ArrayList<>();
        List<Attribute> attributes=new ArrayList<>();
        for (String key:keys ){
            Object value= rootDBO.get(key);
            List<Pair<AttributeDescriptor,Object>> nestedAttributes=getNestedAttributes((DBObject)value);
            List<PropertyDescriptor> descriptors=nestedAttributes.stream().map(tAndV->(PropertyDescriptor)tAndV.getLeft()).collect(Collectors.toList());
            String namespaceURI= type.getName().getNamespaceURI();
            ComplexType complexType=ftFactory.createComplexType(new NameImpl(namespaceURI,key),descriptors,false,false,Collections.emptyList(),null,null);
            AttributeTypeBuilder attributeBuilder= new AttributeTypeBuilder(ftFactory);
            attributeBuilder.setMinOccurs(0);
            attributeBuilder.setMaxOccurs(1);
            AttributeDescriptor descriptor=attributeBuilder.buildDescriptor(key,complexType);
            AttributeBuilder builder=new AttributeBuilder(new ValidatingFeatureFactoryImpl());
            builder.setDescriptor(descriptor);
            descriptorList.add(descriptor);
            nestedAttributes.stream().forEach(tAndV->builder.add(tAndV.getRight(),tAndV.getLeft().getName()));
            Attribute attribute=builder.build();
            attributes.add(attribute);
        }
        FeatureType featureType=ftFactory.createFeatureType(type.getName(),descriptorList,type.getGeometryDescriptor(),false,Collections.emptyList(),null,null);
        ComplexFeatureBuilder featureBuilder=new ComplexFeatureBuilder(featureType);
        for (Attribute attribute:attributes){
            featureBuilder.append(attribute.getName(),attribute);
        }
        return featureBuilder.buildFeature(rootDBO.get("_id").toString());
    }

    private List<Pair<AttributeDescriptor,Object>> getNestedAttributes(DBObject rootDBO){
        Set<String> keys=rootDBO.keySet();
        String namespaceURI= type.getName().getNamespaceURI();
        List<Pair<AttributeDescriptor,Object>> typesAndValues=new ArrayList<>();
        for (String key:keys ){
            Object value= rootDBO.get(key);
            if (value instanceof BasicDBList){
                BasicDBList list=(BasicDBList) value;
                typesAndValues.add(getListTypeAndAttributes(namespaceURI,key,list));
            } else if (value instanceof DBObject){
                typesAndValues.add(getComplexAttributeTypeAndValues(namespaceURI,key,(DBObject) value));
            } else {
                Pair<AttributeDescriptor,Object> simpleAttribute= getLeafTypeAndAttribute(namespaceURI,key, value);
                typesAndValues.add(simpleAttribute);
            }

        }
        return typesAndValues;
    }

    private Pair<AttributeDescriptor,Object> getComplexAttributeTypeAndValues(String namespaceURI,String attrName, DBObject dbobject){
        List<Pair<AttributeDescriptor,Object>> typesAndValues=getNestedAttributes(dbobject);
        List<PropertyDescriptor> descriptors=typesAndValues.stream().map(tAndV->(PropertyDescriptor)tAndV.getLeft()).collect(Collectors.toList());
        ComplexType complexType=ftFactory.createComplexType(new NameImpl(namespaceURI,attrName),descriptors,false,false,Collections.emptyList(),null,null);
        AttributeTypeBuilder attributeBuilder= new AttributeTypeBuilder(ftFactory);
        attributeBuilder.setMinOccurs(0);
        attributeBuilder.setMaxOccurs(1);
        AttributeDescriptor descriptor=attributeBuilder.buildDescriptor(attrName,complexType);
        AttributeBuilder builder=new AttributeBuilder(new ValidatingFeatureFactoryImpl());
        builder.setDescriptor(descriptor);
        typesAndValues.stream().forEach(tAndV->builder.add(tAndV.getRight(),tAndV.getLeft().getName()));
        Attribute attribute=builder.build();
        return new MutablePair<>(descriptor,attribute);
    }

    private Pair<AttributeDescriptor, Object> getListTypeAndAttributes(String namespaceURI, String attrName, BasicDBList value){
        List<AttributeDescriptor> descriptors = new ArrayList<>();
        AttributeBuilder builder=new AttributeBuilder(new ValidatingFeatureFactoryImpl());
        for (int i=0; i<value.size(); i ++){
            DBObject dbObject=(DBObject)value.get(i);
            Pair<AttributeDescriptor,Object> typesAndValue=getComplexAttributeTypeAndValues(namespaceURI,attrName,dbObject);
            descriptors.add(typesAndValue.getLeft());
            builder.add(typesAndValue.getRight(),typesAndValue.getLeft().getName());
        }
        AttributeDescriptor descriptor=mergeDescriptors(namespaceURI,attrName,descriptors);
        builder.setDescriptor(descriptor);
        Attribute attribute=builder.build();
        return  new MutablePair<>(descriptor,attribute);
    }

    private AttributeDescriptor mergeDescriptors(String namespaceURI, String attrName,List<AttributeDescriptor> descriptors){
        Set<PropertyDescriptor> descriptorSet=new HashSet<PropertyDescriptor>();
        for (AttributeDescriptor descriptor: descriptors){
            AttributeType type= descriptor.getType();
            if (type instanceof ComplexType)
                descriptorSet.addAll(((ComplexType)type).getDescriptors());
        }
        Name name=new NameImpl(namespaceURI,attrName);
        ComplexType complexType=ftFactory.createComplexType(name,new ArrayList<>(descriptors),false,false,Collections.emptyList(),null,null);
        return ftFactory.createAttributeDescriptor(complexType,name,0,Integer.MAX_VALUE,true,null);
    }

    private Pair<AttributeDescriptor,Object> getLeafTypeAndAttribute (String namespaceURI, String attrName, Object value) {
        AttributeTypeBuilder typeBuilder= new AttributeTypeBuilder(ftFactory);
        typeBuilder.setBinding(value.getClass());
        typeBuilder.setName(attrName);
        typeBuilder.setNamespaceURI(namespaceURI);
        typeBuilder.setMaxOccurs(1);
        typeBuilder.setMinOccurs(0);
        AttributeType attrType=typeBuilder.buildType();
        AttributeDescriptor attrDescriptor=typeBuilder.buildDescriptor(namespaceURI,attrType);
        AttributeBuilder builder=new AttributeBuilder(new ValidatingFeatureFactoryImpl());
        builder.setDescriptor(attrDescriptor);
        Attribute attribute= builder.add(value,attrDescriptor.getName());
        return new MutablePair<>(attrDescriptor, attribute);
    }




}
