/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2015, Open Source Geospatial Foundation (OSGeo)
 *    (C) 2014-2015, Boundless
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.mongodb;


import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.geotools.data.mongodb.complex.MongoComplexUtilities;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;

import javax.print.Doc;

/** @author tkunicki@boundlessgeo.com */
public class MongoInferredMapper extends AbstractCollectionMapper {

    public static final Logger LOG = Logging.getLogger(MongoInferredMapper.class);

    MongoGeometryBuilder geomBuilder = new MongoGeometryBuilder();

    SimpleFeatureType schema;
    /** Schema generation parameters, not null */
    private MongoSchemaInitParams schemainitParams;

    public MongoInferredMapper() {
        this.schemainitParams = MongoSchemaInitParams.builder().build();
    }

    public MongoInferredMapper(MongoSchemaInitParams schemainitParams) {
        if (schemainitParams != null) this.schemainitParams = schemainitParams;
        else this.schemainitParams = MongoSchemaInitParams.builder().build();
    }

    @Override
    public String getGeometryPath() {
        String gdName = schema.getGeometryDescriptor().getLocalName();
        return (String) schema.getDescriptor(gdName).getUserData().get(MongoDataStore.KEY_mapping);
    }

    @Override
    public String getPropertyPath(String property) {
        AttributeDescriptor descriptor = schema.getDescriptor(property);
        return descriptor == null
                ? null
                : (String) descriptor.getUserData().get(MongoDataStore.KEY_mapping);
    }

    @Override
    public Geometry getGeometry(Document dbo) {
        Object o = MongoUtil.getDBOValue(dbo, getGeometryPath());
        // TODO legacy coordinate pair
        return o == null ? null : geomBuilder.toGeometry(((Document) o).toBsonDocument(BsonDocument.class,MongoUtil.registry));
    }

    @Override
    public Document toDocument(Geometry g) {
        return MongoUtil.toDocument(geomBuilder.toBsonDocument(g));
    }

    @Override
    public void setGeometry(Document dbo, Geometry g) {
        MongoUtil.setDBOValue(dbo, getGeometryPath(), toDocument(g));
    }

    @Override
    public SimpleFeatureType buildFeatureType(Name name, MongoCollection<Document> collection) {

        Set<String> indexedGeometries = MongoUtil.findIndexedGeometries(collection);
        Set<String> indexedFields = MongoUtil.findIndexedFields(collection);
        // Map<String, Class<?>> mappedFields = MongoUtil.findMappableFields(collection);
        // if we have valid schemainitParams use a DB cursor for inferring schema. Else use the
        // first object as default.
        Map<String, Class> mappedFields =
                schemainitParams.getIds().isEmpty() && schemainitParams.getMaxObjects() == 1
                        ? MongoComplexUtilities.findMappings(collection.find().first())
                        : generateMappedFields(collection);

        // don't need to worry about indexed properties we've found in our scan...
        indexedFields.removeAll(mappedFields.keySet());

        // remove geometries from indexed and mapped sets
        indexedFields.removeAll(indexedGeometries);
        for (String mappedProperty : new ArrayList<>(mappedFields.keySet())) {
            for (String indexedGeometry : indexedGeometries) {
                if (mappedProperty.startsWith(indexedGeometry)) {
                    mappedFields.remove(mappedProperty);
                    break;
                }
            }
        }

        // Examine the DBO and remove any invalid indexed fields (such as arrays)
        Document dbo = collection.find().first();
        if (dbo != null) {
            Iterator<String> indexedIterator = indexedFields.iterator();
            while (indexedIterator.hasNext()) {
                Object value = MongoUtil.getDBOValue(dbo, indexedIterator.next());
                if (value == null) {
                    indexedIterator.remove();
                }
            }
        }

        SimpleFeatureTypeBuilder ftBuilder = new SimpleFeatureTypeBuilder();
        ftBuilder.setName(name);

        // NOTE: for now we just use first (hopefully only) indexed geometry we find
        String geometryField = indexedGeometries.iterator().next();
        if (indexedGeometries.size() > 1) {
            LOG.log(
                    Level.WARNING,
                    "More than one indexed geometry field found for type {0}, selecting {1} (first one encountered with index search of collection {2})",
                    new Object[] {name, geometryField, collection.getNamespace().getFullName()});
        }
        ftBuilder.userData(MongoDataStore.KEY_mapping, geometryField);
        ftBuilder.userData(MongoDataStore.KEY_encoding, "GeoJSON");
        ftBuilder.add(geometryField, Geometry.class, DefaultGeographicCRS.WGS84);
        LOG.log(
                Level.INFO,
                "building type {0}: mapping geometry field {1} from collection {2}",
                new Object[] {name, geometryField, collection.getNamespace().getFullName()});

        for (Map.Entry<String, Class> mappedField : mappedFields.entrySet()) {
            String field = mappedField.getKey();
            Class<?> binding = mappedField.getValue();
            ftBuilder.userData(MongoDataStore.KEY_mapping, field);
            ftBuilder.add(field, binding);
            LOG.log(
                    Level.INFO,
                    "building type \"{0}\": mapping field \"{1}\" with binding {2} from collection {3}",
                    new Object[] {name, field, binding.getName(), collection.getNamespace().getFullName()});
        }

        for (String field : indexedFields) {
            ftBuilder.userData(MongoDataStore.KEY_mapping, field);
            ftBuilder.add(field, String.class);
            LOG.log(
                    Level.INFO,
                    "building type \"{0}\": mapping indexed field \"{1}\" with default binding, {2}, from collection {3}",
                    new Object[] {name, field, String.class.getName(), collection.getNamespace().getFullName()});
        }

        SimpleFeatureType featureType = ftBuilder.buildFeatureType();
        featureType.getUserData().put(MongoDataStore.KEY_collection, collection.getNamespace().getFullName());

        this.schema = featureType;

        return featureType;
    }

    private Map<String, Class> generateMappedFields(MongoCollection<Document> collection) {
        final Map<String, Class> resultMap = new HashMap<>();
        @SuppressWarnings("PMD.CloseResource") // closed in findMappings
        final MongoCursor idsCursor = obtainCursorByIds(collection);
        Map<String, Class> idsMappings =
                idsCursor != null
                        ? MongoComplexUtilities.findMappings(idsCursor)
                        : Collections.emptyMap();
        int max = schemainitParams.getMaxObjects() - idsMappings.size();
        @SuppressWarnings("PMD.CloseResource") // closed in findMappings
        final MongoCursor maxObjectsCursor = obtainCursorByMaxObjects(collection, max);
        if (maxObjectsCursor != null)
            resultMap.putAll(MongoComplexUtilities.findMappings(maxObjectsCursor));
        if (!idsMappings.isEmpty()) resultMap.putAll(idsMappings);
        return resultMap;
    }

    private MongoCursor<Document> obtainCursorByIds(MongoCollection<Document> collection) {
        List<String> ids = schemainitParams.getIds();
        if (!ids.isEmpty()) {
            LOG.info("Using IDs list for schema generation.");
            logIdsOnCursor(queryByIds(collection, ids), ids);
            return queryByIds(collection, ids);
        } else {
            LOG.info("IDs list for schema generation not available.");
            return null;
        }
    }

    private MongoCursor<Document> queryByIds(MongoCollection<Document> collection, List<String> ids) {
        List<ObjectId> oidList =
                ids.stream().map(id -> new ObjectId(id)).collect(Collectors.toList());
        Bson query = Filters.in("_id",oidList.toArray(new ObjectId[] {}));
        LOG.log(Level.INFO, "IDs query for execute: {0}", query);
        return collection.find(query).cursor();
    }

    private void logIdsOnCursor(MongoCursor<Document> cursor, List<String> ids) {
        final Set<String> idsOnCursor = new HashSet<>();
        try {
            cursor.forEachRemaining(
                    dbo -> {
                        ObjectId oid = (ObjectId) dbo.get("_id");
                        idsOnCursor.add(oid.toHexString());
                    });
        } finally {
            cursor.close();
        }
        // compare ids on list, log not found ids
        for (String eid : ids) {
            if (!idsOnCursor.contains(eid)) {
                LOG.log(Level.WARNING, "ObjectId with value = '{0}' not found in collection.", eid);
            }
        }
    }

    private MongoCursor<Document> obtainCursorByMaxObjects(MongoCollection<Document> collection, int maxObects) {
        // if configured max object is -1, we should use all collection
        if (schemainitParams.getMaxObjects() == -1) {
            LOG.info("Using all collection objects for schema generation.");
            return collection.find().cursor();
        } else if (maxObects > 0) {
            LOG.info("Using objects max num for schema generation.");
            // else use max num of objects
            LOG.log(Level.INFO, "Max objects limit: {0}", schemainitParams.getMaxObjects());
            return collection.find().limit(maxObects).cursor();
        } else {
            return null;
        }
    }
}
