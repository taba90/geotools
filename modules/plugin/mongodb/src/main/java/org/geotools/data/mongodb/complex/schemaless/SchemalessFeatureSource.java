package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.data.*;
import org.geotools.data.complex.feature.type.Types;
import org.geotools.data.mongodb.FilterToMongo;
import org.geotools.data.mongodb.MongoUtil;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.NameImpl;
import org.geotools.filter.visitor.PostPreProcessFilterSplittingVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.gml3.v3_2.GMLSchema;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.factory.Hints;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.xml.sax.helpers.NamespaceSupport;

public class SchemalessFeatureSource implements FeatureSource<FeatureType, Feature> {

    private SchemalessDataAccess store;

    private Name name;

    private DBCollection collection;

    protected Set<Hints.Key> hints;

    public static final Logger LOG = Logging.getLogger(SchemalessFeatureSource.class);

    public SchemalessFeatureSource(Name name, DBCollection collection, SchemalessDataAccess store) {
        this.store = store;
        this.name = name;
        this.collection = collection;
        // set up hints
        hints = new HashSet<>();
        hints.add(Hints.JTS_GEOMETRY_FACTORY);
        hints.add(Hints.JTS_COORDINATE_SEQUENCE_FACTORY);

        // add subclass specific hints
        addHints(hints);

        // make hints unmodifiable
        hints = Collections.unmodifiableSet(hints);
    }

    @Override
    public Name getName() {
        return name;
    }

    @Override
    public ResourceInfo getInfo() {
        return new ResourceInfo() {
            final Set<String> words = new HashSet<>();

            {
                words.add("features");
                words.add(SchemalessFeatureSource.this.getSchema().getName().toString());
            }

            public ReferencedEnvelope getBounds() {
                try {
                    return SchemalessFeatureSource.this.getBounds();
                } catch (IOException e) {
                    return null;
                }
            }

            public CoordinateReferenceSystem getCRS() {
                return SchemalessFeatureSource.this.getSchema().getCoordinateReferenceSystem();
            }

            public String getDescription() {
                return null;
            }

            public Set<String> getKeywords() {
                return words;
            }

            public String getName() {
                return SchemalessFeatureSource.this.getSchema().getName().toString();
            }

            public URI getSchema() {
                Name name = SchemalessFeatureSource.this.getSchema().getName();
                URI namespace;
                try {
                    namespace = new URI(name.getNamespaceURI());
                    return namespace;
                } catch (URISyntaxException e) {
                    return null;
                }
            }

            public String getTitle() {
                Name name = SchemalessFeatureSource.this.getSchema().getName();
                return name.getLocalPart();
            }
        };
    }

    @Override
    public SchemalessDataAccess getDataStore() {
        return store;
    }

    @Override
    public QueryCapabilities getQueryCapabilities() {
        return new QueryCapabilities();
    }

    @Override
    public void addFeatureListener(FeatureListener listener) {}

    @Override
    public void removeFeatureListener(FeatureListener listener) {}

    @Override
    public FeatureCollection<FeatureType, Feature> getFeatures(Filter filter) throws IOException {
        Query q = new Query(getName().toString(), filter);
        return new SchemalessFeatureCollection(q, this);
    }

    @Override
    public FeatureCollection<FeatureType, Feature> getFeatures(Query query) throws IOException {
        return new SchemalessFeatureCollection(query, this);
    }

    @Override
    public FeatureCollection<FeatureType, Feature> getFeatures() throws IOException {
        return new SchemalessFeatureCollection(Query.ALL, this);
    }

    @Override
    public FeatureType getSchema() {
        SchemalessFeatureTypeCache cache = SchemalessFeatureTypeCache.getInstance();
        FeatureType featureType = null;
        try {
            featureType = cache.getFeatureType(name);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        if (featureType == null) {
            GeometryDescriptor descriptor = getGeometryDescriptorIfPresent();
            featureType =
                    new ModifiableComplexFeatureType(
                            name,
                            Collections.emptyList(),
                            descriptor,
                            false,
                            Collections.emptyList(),
                            GMLSchema.ABSTRACTFEATURETYPE_TYPE,
                            null);
            featureType.getUserData().put(Types.SKIP_STYLE_VALIDATION, true);
        }
        return featureType;
    }

    private GeometryDescriptor getGeometryDescriptorIfPresent() {
        Set<String> geometries = MongoUtil.findIndexedGeometries(collection);
        String geometryPath = null;
        if (geometries == null || geometries.isEmpty()) geometryPath = "geometry";
        else geometryPath = geometries.iterator().next();

        if (geometries.size() > 1) {
            LOG.log(
                    Level.WARNING,
                    "More than one indexed geometry field found for type {0}, selecting {1} (first one encountered with index search of collection {2})",
                    new Object[] {geometryPath, geometryPath, collection.getFullName()});
        }
        AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
        attributeBuilder.setBinding(Geometry.class);
        String geometryAttributeName;
        if (geometryPath.indexOf(".") != -1) {
            String[] splitted = geometryPath.split("\\.");
            geometryAttributeName = splitted[splitted.length - 1];
        } else {
            geometryAttributeName = geometryPath;
        }
        attributeBuilder.setName(geometryAttributeName);
        attributeBuilder.setCRS(DefaultGeographicCRS.WGS84);
        GeometryType type = attributeBuilder.buildGeometryType();
        type.getUserData().put("GEOMETRY_PATH", geometryPath);
        return attributeBuilder.buildDescriptor(name(geometryPath), type);
    }

    protected final Name name(String typeName) {
        return new NameImpl(typeName);
    }

    @Override
    public ReferencedEnvelope getBounds() throws IOException {
        return getBounds(Query.ALL);
    }

    @Override
    public ReferencedEnvelope getBounds(Query query) throws IOException {
        return getBoundsInternal(query);
    }

    private ReferencedEnvelope getBoundsInternal(Query q) throws IOException {
        try (FeatureReader<FeatureType, Feature> r = getReader(q)) {
            ReferencedEnvelope e = new ReferencedEnvelope();
            if (r.hasNext()) {
                Feature f = r.next();
                e.init(f.getBounds());
            }
            while (r.hasNext()) {
                e.include(r.next().getBounds());
            }
            return e;
        }
    }

    @Override
    public int getCount(Query query) throws IOException {
        return 0;
    }

    @Override
    public Set<RenderingHints.Key> getSupportedHints() {
        return (Set<RenderingHints.Key>) (Set<?>) hints;
    }

    public SchemalessFeatureReader getReader(Query query) {
        List<Filter> postFilterList = new ArrayList<>();
        List<String> postFilterAttributes = new ArrayList<>();
        DBCursor cursor = toCursor(query, postFilterList, postFilterAttributes);
        return new MongoSchemalessFeatureReader(cursor, this);
    }

    DBCursor toCursor(Query q, java.util.List<Filter> postFilter, List<String> postFilterAttrs) {
        DBObject query = new BasicDBObject();

        Filter f = q.getFilter();
        if (!isAll(f)) {
            Filter[] split = splitFilter(f);
            query = toQuery(split[0]);
            if (!isAll(split[1])) {
                postFilter.add(split[1]);
            }
        }

        DBCursor c;
        if (q.getPropertyNames() != Query.ALL_NAMES) {
            BasicDBObject keys = new BasicDBObject();
            for (String p : q.getPropertyNames()) {
                keys.put(toMongoPath(p), 1);
            }
            // add properties from post filters
            for (Filter filter : postFilter) {
                String[] attributeNames = DataUtilities.attributeNames(filter);
                for (String attrName : attributeNames) {
                    if (attrName != null && !attrName.isEmpty() && !keys.containsField(attrName)) {
                        keys.put(toMongoPath(attrName), 1);
                        postFilterAttrs.add(attrName);
                    }
                }
            }
            keys.put(MongoUtil.findIndexedGeometries(collection).iterator().next(), 1);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("find(%s, %s)", query, keys));
            }
            c = collection.find(query, keys);
        } else {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("find(%s)", query));
            }
            c = collection.find(query);
        }

        if (q.getStartIndex() != null && q.getStartIndex() != 0) {
            c = c.skip(q.getStartIndex());
        }
        if (q.getMaxFeatures() != Integer.MAX_VALUE) {
            c = c.limit(q.getMaxFeatures());
        }

        if (q.getSortBy() != null) {
            BasicDBObject orderBy = new BasicDBObject();
            for (SortBy sortBy : q.getSortBy()) {
                if (sortBy.getPropertyName() != null) {
                    String propName = sortBy.getPropertyName().getPropertyName();
                    String property = toMongoPath(propName);
                    orderBy.append(property, sortBy.getSortOrder() == SortOrder.ASCENDING ? 1 : -1);
                }
            }
            c = c.sort(orderBy);
        }

        return c;
    }

    private String toMongoPath(String pn) {
        String[] splittedPn = pn.split("/");
        StringBuilder sb = new StringBuilder("");
        String prev = null;
        for (int i = 0; i < splittedPn.length; i++) {
            String xpathStep = splittedPn[i];
            if (xpathStep.indexOf(":") != -1) xpathStep = xpathStep.split(":")[1];
            int index = xpathStep.indexOf("Index");
            if (index != -1) {
                xpathStep = xpathStep.substring(0, index);
            }
            String nameCapitalized =
                    prev != null ? prev.substring(0, 1).toUpperCase() + prev.substring(1) : null;
            if (!xpathStep.equals(prev) && !xpathStep.equals(nameCapitalized)) {
                sb.append(xpathStep);
                if (i != splittedPn.length - 1) sb.append(".");
            }
            prev = xpathStep;
        }
        return sb.toString();
    }

    DBObject toQuery(Filter f) {
        if (isAll(f)) {
            return new BasicDBObject();
        }

        FilterToMongo v = new FilterToMongo(null);
        v.setPropertyTypesMap(new MongoDataTypesFinder(collection).findPropertiesTypes(f));
        v.setFeatureType(getSchema());

        return (DBObject) f.accept(v, null);
    }

    boolean isAll(Filter f) {
        return f == null || f == Filter.INCLUDE;
    }

    @SuppressWarnings("deprecation")
    Filter[] splitFilter(Filter f) {
        PostPreProcessFilterSplittingVisitor splitter =
                new PostPreProcessFilterSplittingVisitor(
                        getDataStore().getFilterCapabilities(), null, null) {

                    public Object visit(PropertyIsLike filter, Object notUsed) {
                        if (original == null) original = filter;

                        if (!fcs.supports(PropertyIsLike.class)) {
                            // MongoDB can only encode like expressions using propertyName
                            postStack.push(filter);
                            return null;
                        }
                        if (!(filter.getExpression() instanceof PropertyName)) {
                            // MongoDB can only encode like expressions using propertyName
                            postStack.push(filter);
                            return null;
                        }

                        int i = postStack.size();
                        filter.getExpression().accept(this, null);

                        if (i < postStack.size()) {
                            postStack.pop();
                            postStack.push(filter);

                            return null;
                        }

                        preStack.pop(); // value
                        preStack.push(filter);
                        return null;
                    }

                    @Override
                    public Object visit(PropertyIsNull filter, Object notUsed) {
                        preStack.push(filter);
                        return null;
                    }
                };
        f.accept(splitter, null);
        return new Filter[] {splitter.getFilterPre(), splitter.getFilterPost()};
    }

    protected void addHints(Set<Hints.Key> hints) {}

    private Map<String, String> getNamespacesMap(NamespaceSupport namespaces) {
        final Map<String, String> namespacesMap = new HashMap<>();
        final Enumeration prefixes = namespaces.getPrefixes();
        while (prefixes.hasMoreElements()) {
            final String prefix = (String) prefixes.nextElement();
            final String uri = namespaces.getURI(prefix);
            namespacesMap.put(prefix, uri);
        }
        return namespacesMap;
    }
}
