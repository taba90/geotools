package org.geotools.data.mongodb.complex.schemaless;

import com.mongodb.*;
import java.util.*;
import org.geotools.filter.FilterAttributeExtractor;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.PropertyName;

public class MongoDataTypesFinder {

    DBCollection collection;

    public MongoDataTypesFinder(DBCollection collection) {
        this.collection = collection;
    }

    public Map<String, Class<?>> findPropertiesTypes(Filter filter) {
        FilterAttributeExtractor extractor = new FilterAttributeExtractor();
        filter.accept(extractor, null);
        Set<PropertyName> propertyNameSet = extractor.getPropertyNameSet();
        List<String> strPn = new ArrayList<>();
        for (PropertyName pn : propertyNameSet) {
            strPn.add(getJsonPathFromPropertyPath(pn.getPropertyName()));
        }
        Map<String, Class<?>> propertyTypes = new HashMap<>();
        for (String str : strPn) {
            propertyTypes.put(str, getQueryTypeResult(str));
        }

        return propertyTypes;
    }

    private Class<?> getQueryTypeResult(String attribute) {
        BasicDBObject projection = new BasicDBObject();
        projection.put(attribute, 1);
        projection.put("_id", 0);
        try (Cursor cursor =
                collection
                        .find(
                                new BasicDBObject(attribute, new BasicDBObject("$ne", "null")),
                                projection)
                        .limit(1)) {
            Class<?> result = null;
            if (cursor.hasNext()) {
                DBObject dbRes = cursor.next();
                result = getFieldType(dbRes);
            }

            return result;
        }
    }

    private Class<?> determineClassType(String mongoType) {
        if (mongoType == null || mongoType.equals("string")) {
            return String.class;
        } else if (mongoType.equals("double")) return Double.class;
        else if (mongoType.equals("bool")) return Boolean.class;
        else if (mongoType.equals("int")) return Integer.class;
        else if (mongoType.equals("Date")) return Date.class;
        return String.class;
    }

    private String getJsonPathFromPropertyPath(String propertyName) {
        String[] splittedPn = propertyName.split("/");
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
                    prev != null
                            ? prev.substring(0, 1).toUpperCase() + prev.substring(1)
                            : null;
            if (!xpathStep.equals(prev) && !xpathStep.equals(nameCapitalized)) {
                sb.append(xpathStep);
                if (i != splittedPn.length - 1) sb.append(".");
            }
            prev = xpathStep;
        }
        return sb.toString();
    }

    private Class<?> getFieldType(Object document) {
        if (document instanceof BasicDBList) {
            BasicDBList list = (BasicDBList) document;
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                return getFieldType(element);
            }
        } else if (document instanceof BasicDBObject) {
            DBObject object = (DBObject) document;
            Set<String> keys = object.keySet();
            for (String k : keys) {
                return getFieldType(object.get(k));
            }
        } else {
            return document.getClass();
        }

        return null;
    }
}
