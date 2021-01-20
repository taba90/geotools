package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.*;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.geotools.data.DataAccess;
import org.geotools.data.FeatureSource;
import org.geotools.data.ServiceInfo;
import org.geotools.data.mongodb.complex.JsonSelectAllFunction;
import org.geotools.data.mongodb.complex.JsonSelectFunction;
import org.geotools.data.ows.HTTPClient;
import org.geotools.feature.NameImpl;
import org.geotools.filter.FilterCapabilities;
import org.geotools.util.logging.Logging;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.*;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Within;

public class SchemalessDataAccess implements DataAccess<FeatureType, Feature> {

    private static final Logger LOGGER = Logging.getLogger(SchemalessDataAccess.class);

    final MongoClient dataStoreClient;
    final DB dataStoreDB;

    final boolean deactivateOrNativeFilter;

    // for reading schema from hosted files
    private HTTPClient httpClient;

    @SuppressWarnings("deprecation")
    FilterCapabilities filterCapabilities;

    protected String namespaceURI;

    private List<Name> typeNames;

    public SchemalessDataAccess(
            String dataStoreURI, boolean createDatabaseIfNeeded, HTTPClient httpClient) {
        MongoClientURI dataStoreClientURI = createMongoClientURI(dataStoreURI);
        dataStoreClient = createMongoClient(dataStoreClientURI);
        dataStoreDB =
                createDB(
                        dataStoreClient, dataStoreClientURI.getDatabase(), !createDatabaseIfNeeded);

        if (dataStoreDB == null) {
            dataStoreClient.close(); // This smells bad...
            throw new IllegalArgumentException(
                    "Unknown mongodb database, \"" + dataStoreClientURI.getDatabase() + "\"");
        }

        this.deactivateOrNativeFilter = isMongoVersionLessThan2_6(dataStoreClientURI);
        this.httpClient = httpClient;

        filterCapabilities = createFilterCapabilties();
    }

    final FilterCapabilities createFilterCapabilties() {
        FilterCapabilities capabilities = new FilterCapabilities();

        if (deactivateOrNativeFilter) {
            /*
             * disable FilterCapabilities.LOGICAL_OPENGIS since it contains Or.class (in
             * additions to And.class and Not.class. MongodB 2.4 doesn't support '$or' with
             * spatial operations.
             */
            capabilities.addType(And.class);
            capabilities.addType(Not.class);
        } else {
            // default behavior, '$or' is fully supported from MongoDB 2.6.0 version
            capabilities.addAll(FilterCapabilities.LOGICAL_OPENGIS);
        }

        capabilities.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        capabilities.addType(PropertyIsNull.class);
        capabilities.addType(PropertyIsBetween.class);
        capabilities.addType(PropertyIsLike.class);

        capabilities.addType(BBOX.class);
        capabilities.addType(Intersects.class);
        capabilities.addType(Within.class);

        capabilities.addType(Id.class);
        capabilities.addType(JsonSelectFunction.class);
        capabilities.addType(JsonSelectAllFunction.class);

        /*
        capabilities.addType(IncludeFilter.class);
        capabilities.addType(ExcludeFilter.class);

        //temporal filters
        capabilities.addType(After.class);
        capabilities.addType(Before.class);
        capabilities.addType(Begins.class);
        capabilities.addType(BegunBy.class);
        capabilities.addType(During.class);
        capabilities.addType(Ends.class);
        capabilities.addType(EndedBy.class);*/

        return capabilities;
    }

    final MongoClient createMongoClient(MongoClientURI mongoClientURI) {
        try {
            return new MongoClient(mongoClientURI);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown mongodb host(s)", e);
        }
    }

    final DB createDB(MongoClient mongoClient, String databaseName, boolean databaseMustExist) {
        if (databaseMustExist
                && !StreamSupport.stream(mongoClient.listDatabaseNames().spliterator(), false)
                        .anyMatch(name -> databaseName.equalsIgnoreCase(name))) {
            return null;
        }
        return mongoClient.getDB(databaseName);
    }

    final MongoClientURI createMongoClientURI(String dataStoreURI) {
        if (dataStoreURI == null) {
            throw new IllegalArgumentException("dataStoreURI may not be null");
        }
        if (!dataStoreURI.startsWith("mongodb://")) {
            throw new IllegalArgumentException(
                    "incorrect scheme for URI, expected to begin with \"mongodb://\", found URI of \""
                            + dataStoreURI
                            + "\"");
        }
        return new MongoClientURI(dataStoreURI.toString());
    }

    private boolean isMongoVersionLessThan2_6(MongoClientURI dataStoreClientURI) {
        boolean deactivateOrAux = false;
        // check server version
        if (dataStoreClient != null
                && dataStoreClientURI != null
                && dataStoreClientURI.getDatabase() != null) {
            Document result =
                    dataStoreClient
                            .getDatabase(dataStoreClientURI.getDatabase())
                            .runCommand(new BsonDocument("buildinfo", new BsonString("")));
            if (result.containsKey("versionArray")) {
                @SuppressWarnings("unchecked")
                List<Integer> versionArray = (List) result.get("versionArray");
                // if MongoDB server version < 2.6.0 disable native $or operator
                if (versionArray.get(0) < 2
                        || (versionArray.get(0) == 2 && versionArray.get(1) < 6)) {
                    deactivateOrAux = true;
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown Mongo Client");
        }
        return deactivateOrAux;
    }

    @Override
    public ServiceInfo getInfo() {
        return null;
    }

    @Override
    public void createSchema(FeatureType featureType) throws IOException {}

    @Override
    public void updateSchema(Name typeName, FeatureType featureType) throws IOException {}

    @Override
    public void removeSchema(Name typeName) throws IOException {}

    @Override
    public List<Name> getNames() throws IOException {
        if (typeNames == null) {
            typeNames = createTypeNames();
        }
        return typeNames;
    }

    @Override
    public FeatureType getSchema(Name name) throws IOException {
        return getFeatureSource(name).getSchema();
    }

    @Override
    public FeatureSource<FeatureType, Feature> getFeatureSource(Name typeName) throws IOException {
        DBCollection collection = dataStoreDB.getCollection(typeName.getLocalPart());
        return new SchemalessFeatureSource(typeName, collection, this);
    }

    public FilterCapabilities getFilterCapabilities() {
        return filterCapabilities;
    }

    @Override
    public void dispose() {}

    public void setNamespaceURI(String uri) {
        this.namespaceURI = uri;
    }

    protected List<Name> createTypeNames() throws IOException {

        Set<String> collectionNames = new LinkedHashSet<>(dataStoreDB.getCollectionNames());
        return collectionNames.stream().map(s -> name(s)).collect(Collectors.toList());
    }

    protected final Name name(String typeName) {
        return new NameImpl(namespaceURI, typeName);
    }
}
