/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2020, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.process.vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.collection.DecoratingFeatureCollection;
import org.geotools.feature.collection.DecoratingFeatureIterator;
import org.geotools.feature.collection.PushBackFeatureIterator;
import org.geotools.feature.type.Types;
import org.geotools.filter.AttributeExpressionImpl;
import org.geotools.metadata.i18n.ErrorKeys;
import org.geotools.metadata.i18n.Errors;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.util.factory.GeoTools;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
import org.xml.sax.helpers.NamespaceSupport;

@DescribeProcess(
    title = "Filtering Features",
    description =
            "Given a collection of features for each group defined only the feature having the MIN or MAX value for the chosen attribute will be included in the final output"
)
public class FilteringVectorProcess implements VectorProcess {

    protected FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(GeoTools.getDefaultHints());

    public FeatureCollection execute(
            @DescribeParameter(name = "data", description = "Input feature collection")
                    FeatureCollection features,
            @DescribeParameter(
                        name = "aggregation",
                        description = "The aggregate operation to be computed, it can be MAX or MIN"
                    )
                    String aggregation,
            @DescribeParameter(
                        name = "operationAttribute",
                        description =
                                "The feature's attribute to be used to compute the aggregation"
                    )
                    String operationAttribute,
            @DescribeParameter(
                        name = "groupingAttributes",
                        description =
                                "The feature's attributes defining groups for which perform the filtering based on the aggregation operation and the operation attribute."
                                        + "Consistent results are guaranteed only if the vector process is fed with features already sorted  by these attributes"
                    )
                    List<String> groupingAttributes) {
        try {
            if (features == null) {
                throw new ProcessException(Errors.format(ErrorKeys.NULL_ARGUMENT_$1, "features"));
            }
            if (operationAttribute == null) {
                throw new ProcessException(
                        Errors.format(ErrorKeys.NULL_ARGUMENT_$1, "operationAttribute"));
            }
            if (groupingAttributes == null || groupingAttributes.size() == 0) {
                throw new ProcessException(
                        Errors.format(ErrorKeys.NULL_ARGUMENT_$1, "groupingAttributes"));
            }
            if (aggregation == null) {
                throw new ProcessException(
                        Errors.format(ErrorKeys.NULL_ARGUMENT_$1, "aggregation"));
            }
            Operations op = Operations.valueOf(aggregation);
            FeatureType schema = features.getSchema();
            NamespaceSupport ns = declareNamespaces(schema);
            List<PropertyName> groupingPn =
                    groupingAttributes
                            .stream()
                            .map(
                                    g ->
                                            validatePropertyName(
                                                    new AttributeExpressionImpl(g, ns), schema))
                            .collect(Collectors.toList());
            PropertyName opValue =
                    validatePropertyName(ff.property(operationAttribute, ns), schema);
            return new GroupingFeatureCollection(features, groupingPn, opValue, op);
        } catch (IllegalArgumentException e) {
            throw new ProcessException(
                    Errors.format(ErrorKeys.BAD_PARAMETER_$2, "aggregation", aggregation));
        }
    }

    public Query invertQuery(Query targetQuery, GridGeometry gridGeometry) {
        // add sortBy if found in the renderQuery
        if (targetQuery.getSortBy() != null) {
            Query q = new Query();
            q.setSortBy(targetQuery.getSortBy());
            return q;
        } else return null;
    }

    private PropertyName validatePropertyName(PropertyName pn, FeatureType schema) {
        // checks propertyName against the schema
        if (pn.evaluate(schema) == null)
            throw new ProcessException(
                    "Unable to resolve " + pn.getPropertyName() + " against the FeatureType");
        return pn;
    }

    private NamespaceSupport declareNamespaces(FeatureType type) {
        // retrieves Namespaces for complex features
        NamespaceSupport namespaceSupport = null;
        Map namespaces = (Map) type.getUserData().get(Types.DECLARED_NAMESPACES_MAP);
        if (namespaces != null) {
            namespaceSupport = new NamespaceSupport();
            for (Iterator it = namespaces.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry entry = (Map.Entry) it.next();
                String prefix = (String) entry.getKey();
                String namespace = (String) entry.getValue();
                namespaceSupport.declarePrefix(prefix, namespace);
            }
        }
        return namespaceSupport;
    }

    static class GroupingFeatureCollection
            extends DecoratingFeatureCollection<FeatureType, Feature> {

        List<PropertyName> groupByAttributes;

        PropertyName operationAttribute;

        Operations aggregation;

        public GroupingFeatureCollection(
                FeatureCollection<FeatureType, Feature> delegate,
                List<PropertyName> groupByAttributes,
                PropertyName operationAttribute,
                Operations aggregation) {
            super(delegate);
            this.groupByAttributes = groupByAttributes;
            this.operationAttribute = operationAttribute;
            this.aggregation = aggregation;
        }

        @Override
        public FeatureIterator<Feature> features() {
            return new GroupingFeatureIterator(
                    new PushBackFeatureIterator(delegate.features()),
                    groupByAttributes,
                    operationAttribute,
                    aggregation);
        }
    }

    static class GroupingFeatureIterator extends DecoratingFeatureIterator<Feature> {

        private List<PropertyName> groupByAttributes;

        private PropertyName operationAttribute;

        private Operations aggregation;

        private Feature next;

        /**
         * Wrap the provided FeatureIterator.
         *
         * @param iterator Iterator to be used as a delegate.
         */
        public GroupingFeatureIterator(
                PushBackFeatureIterator iterator,
                List<PropertyName> groupByAttributes,
                PropertyName operationValue,
                Operations aggregation) {
            super(iterator);
            this.groupByAttributes = groupByAttributes;
            this.operationAttribute = operationValue;
            this.aggregation = aggregation;
        }

        @Override
        public boolean hasNext() {
            List<Object> groupingValues = new ArrayList<>(groupByAttributes.size());
            Map<Object, Feature> beingFiltered = new HashMap<>();
            List<Double> valuesToCompare = new ArrayList<>();
            while (super.hasNext()) {
                Feature f = super.next();
                if (beingFiltered.size() == 0) {
                    // no features in the list this is the first of the group
                    // takes the values to check the following features if belong to the same group
                    addGroupingValues(groupingValues, f);
                    addDoubleValueFromFeature(f, valuesToCompare, beingFiltered);
                } else {
                    // is feature in the group?
                    if (featureComparison(groupingValues, f)) {
                        addDoubleValueFromFeature(f, valuesToCompare, beingFiltered);
                    } else {
                        ((PushBackFeatureIterator) delegate).pushBack();
                        break;
                    }
                }
            }
            next = doFiltering(beingFiltered, valuesToCompare);
            return next != null;
        }

        private Feature doFiltering(
                Map<Object, Feature> beingFiltered, List<Double> beingEvaluated) {
            Object key;
            // searches the min or max inside the values list and retrieve the feature
            // to be returned from the Map.
            if (beingFiltered.size() > 0 && beingEvaluated.size() > 0) {
                if (this.aggregation.equals(Operations.MIN)) key = computeMin(beingEvaluated);
                else key = computeMax(beingEvaluated);

                return beingFiltered.get(key);
            } else {
                return null;
            }
        }

        private Number computeMin(List<Double> beingEvaluated) {
            return Collections.min(beingEvaluated);
        }

        private Number computeMax(List<Double> beingEvaluated) {
            return Collections.max(beingEvaluated);
        }

        private boolean featureComparison(List<Object> groupingValues, Feature f) {
            List<Object> toCompareValues = new ArrayList<>(groupingValues.size());
            for (PropertyName p : groupByAttributes) {
                toCompareValues.add(p.evaluate(f));
            }
            if (groupingValues.equals(toCompareValues)) return true;
            return false;
        }

        private void addGroupingValues(List<Object> groupingValues, Feature f) {
            for (PropertyName p : groupByAttributes) {
                groupingValues.add(p.evaluate(f));
            }
        }

        private void addDoubleValueFromFeature(
                Feature f, List<Double> valuesToCompare, Map<Object, Feature> beingFiltered) {
            Object result = this.operationAttribute.evaluate(f, Number.class);
            if (result instanceof Number) {
                Double value = ((Number) result).doubleValue();
                valuesToCompare.add(value);
                beingFiltered.put(value, f);

            } else {
                // not a numeric value. Throwing exception
                throw new ProcessException(
                        "Grouping vector process can handle operationAttribute parameter only if pointing to numeric value ");
            }
        }

        @Override
        public Feature next() throws NoSuchElementException {
            if (next == null && !this.hasNext()) {
                throw new NoSuchElementException();
            }
            Feature f = next;
            next = null;
            return f;
        }

        @Override
        public void close() {
            delegate.close();
            delegate = null;
            next = null;
        }
    }

    enum Operations {
        MAX("MAX"),
        MIN("MIN");

        private String operation;

        Operations(String operation) {
            this.operation = operation;
        }

        public String getOperation() {
            return operation;
        }
    }
}
