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

import com.mongodb.ConnectionString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.operation.Projection;

import javax.print.Doc;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

/** @author tkunicki@boundlessgeo.com */
@SuppressWarnings("deprecation") // DB was replaced by MongoDatabase but API is not the same
public class MongoSchemaDBStore implements MongoSchemaStore {

    static final String DEFAULT_databaseName = "geotools";
    static final String DEFAULT_collectionName = "schemas";

    final MongoClient client;
    final MongoCollection<Document> collection;

    public MongoSchemaDBStore(String uri) throws IOException {
        this(new ConnectionString(uri));
    }

    public MongoSchemaDBStore(ConnectionString uri) throws IOException {
        client = MongoClients.create(uri);

        String databaseName = uri.getDatabase();
        if (databaseName == null) {
            databaseName = DEFAULT_databaseName;
        }
        MongoDatabase database = client.getDatabase(databaseName);

        String collectionName = uri.getCollection();
        if (collectionName == null) {
            collectionName = DEFAULT_collectionName;
        }
        collection = database.getCollection(collectionName);
        BsonDocument bsonDoc=new BsonDocument();
        bsonDoc.append(FeatureTypeDBObject.KEY_typeName,new BsonInt64( 1))
                .append("unique", new BsonBoolean(true));
        collection.createIndex(bsonDoc);
    }

    @Override
    public void storeSchema(SimpleFeatureType schema) throws IOException {
        if (schema != null) {
            String typeName = schema.getTypeName();
            UpdateOptions options= new UpdateOptions();
            options.upsert(true);
            if (typeName != null) {
                collection.updateOne(
                        new BsonDocument(FeatureTypeDBObject.KEY_typeName, new BsonString(schema.getTypeName())),
                         FeatureTypeDBObject.convert(schema),
                        options);
            }
        }
    }

    @Override
    public SimpleFeatureType retrieveSchema(Name name) throws IOException {
        if (name == null) {
            return null;
        }
        String typeName = name.getLocalPart();
        if (typeName == null) {
            return null;
        }
        Document document =
                collection.find(new BsonDocument(FeatureTypeDBObject.KEY_typeName, new BsonString(typeName))).first();
        SimpleFeatureType featureType = null;
        if (document != null) {
            try {
                featureType = FeatureTypeDBObject.convert(document, name);
            } catch (RuntimeException e) {
                // bah, maybe should use typed exception here...
            }
        }
        return featureType;
    }

    @Override
    public void deleteSchema(Name name) throws IOException {
        if (name == null) {
            return;
        }
        String typeName = name.getLocalPart();
        if (typeName == null) {
            return;
        }
        collection.deleteOne(new BsonDocument(FeatureTypeDBObject.KEY_typeName, new BsonString(typeName)));
    }

    @Override
    public List<String> typeNames() {
        Bson projection=fields(include(FeatureTypeDBObject.KEY_typeName));
        Long count=collection.countDocuments();
        try (MongoCursor<Document> cursor=collection.find(projection).cursor()){
            List<String> typeNames = new ArrayList<>(count.intValue());
            while (cursor.hasNext()) {
                Document document = cursor.next();
                if (document != null) {
                    Object typeName = document.get(FeatureTypeDBObject.KEY_typeName);
                    if (typeName instanceof String) {
                        typeNames.add((String) typeName);
                    }
                }
            }
            return typeNames;
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
