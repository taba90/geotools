package org.geotools.data.mongodb.complex.schemaless;

import org.geotools.data.complex.feature.type.ComplexFeatureTypeFactoryImpl;
import org.geotools.feature.AttributeBuilder;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.FeatureTypes;
import org.geotools.feature.NameImpl;
import org.geotools.geometry.jts.CoordinateSequenceTransformer;
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer;
import org.geotools.gml3.v3_2.GMLSchema;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class SchemalessReprojectFeatureReader extends SchemalessFeatureReader {

    GeometryCoordinateSequenceTransformer transformer=new GeometryCoordinateSequenceTransformer();
    SchemalessFeatureReader delegate;
    FeatureType featureType;
    public SchemalessReprojectFeatureReader (SchemalessFeatureReader schemalessFeatureReader, CoordinateReferenceSystem sourceCRS, CoordinateReferenceSystem targetCRS) throws FactoryException {
        this.delegate=schemalessFeatureReader;
        transformer.setMathTransform(CRS.findMathTransform(sourceCRS, targetCRS, true));
        FeatureType type=schemalessFeatureReader.getFeatureType();
        GeometryDescriptor descriptor=type.getGeometryDescriptor();
        AttributeTypeBuilder typeBuilder=new AttributeTypeBuilder();
        typeBuilder.setName(descriptor.getName().getLocalPart());
        typeBuilder.setBinding(descriptor.getType().getBinding());
        typeBuilder.setMaxOccurs(1);
        typeBuilder.setMinOccurs(0);
        typeBuilder.setCRS(targetCRS);
        GeometryType geomType=typeBuilder.buildGeometryType();
        GeometryDescriptor geomDesc=typeBuilder.buildDescriptor(new NameImpl(descriptor.getLocalName()), geomType);
        Collection<PropertyDescriptor> descriptors=type.getDescriptors().stream().filter(d->!d.getName().equals(geomDesc.getName())).collect(Collectors.toList());
        descriptors.add(geomDesc);
        this.featureType=new ModifiableComplexFeatureType(type.getName(),descriptors, geomDesc,false, Collections.emptyList(), GMLSchema.ABSTRACTFEATURETYPE_TYPE,null);
    }
    @Override
    public FeatureType getFeatureType() {
        return delegate.getFeatureType();
    }

    @Override
    public Feature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        Feature feature = delegate.next();
        GeometryDescriptor geomDesc=this.featureType.getGeometryDescriptor();
        Collection<PropertyDescriptor> descriptors=feature.getType().getDescriptors().stream().filter(d->!d.getName().equals(geomDesc.getName())).collect(Collectors.toList());
        descriptors.removeAll(featureType.getDescriptors());
        descriptors.addAll(featureType.getDescriptors());
        GeometryAttribute defGeom = feature.getDefaultGeometryProperty();
        Geometry geom= null;
        try {
            geom = transformer.transform(Geometry.class.cast(defGeom.getValue()));
        } catch (TransformException e) {
            e.printStackTrace();
        }
        feature.getDefaultGeometryProperty().setValue(geom);
        return feature;
    }
    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
