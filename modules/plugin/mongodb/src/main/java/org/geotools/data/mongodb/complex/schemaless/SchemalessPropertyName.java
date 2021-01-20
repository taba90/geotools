package org.geotools.data.mongodb.complex.schemaless;

import org.opengis.filter.expression.ExpressionVisitor;
import org.opengis.filter.expression.PropertyName;
import org.xml.sax.helpers.NamespaceSupport;

public class SchemalessPropertyName implements PropertyName {

    PropertyName delegate;

    public SchemalessPropertyName(PropertyName pn) {
        this.delegate = pn;
    }

    @Override
    public String getPropertyName() {
        return delegate.getPropertyName();
    }

    @Override
    public NamespaceSupport getNamespaceContext() {
        return delegate.getNamespaceContext();
    }

    @Override
    public Object evaluate(Object object) {
        Object result = delegate.evaluate(object);
        if (result == null && object instanceof ModifiableComplexFeatureType) return object;
        return result;
    }

    @Override
    public <T> T evaluate(Object object, Class<T> context) {
        return delegate.evaluate(object, context);
    }

    @Override
    public Object accept(ExpressionVisitor visitor, Object extraData) {
        return null;
    }
}
