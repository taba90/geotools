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
package org.geotools.data.mongodb.complex.schemaless;

import java.io.IOException;
import java.util.Map;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.ows.HTTPClient;
import org.geotools.data.ows.SimpleHttpClient;

public class SchemalessMongoDataStoreFactory implements DataAccessFactory {

    public static final Param NAMESPACE =
            new Param("namespace", String.class, "Namespace prefix", false);
    public static final Param DATASTORE_URI =
            new Param(
                    "data_store",
                    String.class,
                    "MongoDB URI",
                    true,
                    "mongodb://localhost/<database name>");
    public static final Param HTTP_USER =
            new Param(
                    "http_user",
                    String.class,
                    "(Optional)If Schema file is hosted behind a password protected URL",
                    false,
                    null);
    public static final Param HTTP_PASSWORD =
            new Param(
                    "http_pass",
                    String.class,
                    "(Optional)If Schema file is hosted behind a password protected URL",
                    false,
                    null);

    @Override
    public String getDisplayName() {
        return "MongoDB Schemaless";
    }

    @Override
    public String getDescription() {
        return getDisplayName();
    }

    @Override
    public Param[] getParametersInfo() {
        return new Param[] {NAMESPACE, DATASTORE_URI, HTTP_USER, HTTP_PASSWORD};
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public SchemalessDataAccess createDataStore(Map<String, ?> params) throws IOException {
        // retrieve schema generation parameters
        // instance datastore
        SchemalessDataAccess dataStore =
                new SchemalessDataAccess(
                        (String) DATASTORE_URI.lookUp(params), true, getHTTPClient(params));
        String uri = (String) NAMESPACE.lookUp(params);
        if (uri != null) {
            dataStore.setNamespaceURI(uri);
        }
        return dataStore;
    }

    private HTTPClient getHTTPClient(Map<String, ?> params) throws IOException {

        SimpleHttpClient simpleHttpClient = new SimpleHttpClient();
        // check for credentials
        if (HTTP_USER.lookUp(params) == null || HTTP_PASSWORD.lookUp(params) == null)
            return simpleHttpClient;

        simpleHttpClient.setUser((String) HTTP_USER.lookUp(params));
        simpleHttpClient.setPassword((String) HTTP_PASSWORD.lookUp(params));

        return simpleHttpClient;
    }
}
