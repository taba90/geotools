package org.geotools.data.teradata;

import org.geotools.jdbc.JDBCSkipColumnTestSetup;

public class TeradataSkipColumnTestSetup extends JDBCSkipColumnTestSetup {

    protected TeradataSkipColumnTestSetup() {
        super(new TeradataTestSetup());
    }

    @Override
    protected void createSkipColumnTable() throws Exception {
        run("CREATE TYPE dollar AS DECIMAL(8,2) FINAL");
        run("CREATE TABLE \"skipcolumn\"(\"fid\" PRIMARY KEY not null generated always as identity (start with 0)  integer, \"id\" integer, \"geom\" ST_GEOMETRY, \"weirdproperty\" dollar,\"name\" varchar(200))");
        run("INSERT INTO SYSSPATIAL.GEOMETRY_COLUMNS (F_TABLE_CATALOG, F_TABLE_SCHEMA, F_TABLE_NAME, F_GEOMETRY_COLUMN, COORD_DIMENSION, SRID, GEOM_TYPE) VALUES ('', '" + fixture.getProperty("schema") + "', 'skipcolumn', 'geom', 2, 1619, 'POINT')");
//        run("CREATE INDEX SKIPCOLUMN_GEOM_INDEX ON \"skipcolumn\" USING GIST (\"geom\") ");

        run("INSERT INTO \"skipcolumn\" VALUES(0, 0, 'POINT(0 0)', null, 'GeoTools')");

    }

    @Override
    protected void dropSkipColumnTable() throws Exception {
        runSafe("DELETE FROM SYSSPATIAL.GEOMETRY_COLUMNS WHERE F_TABLE_NAME = 'skipcolumn'");
        runSafe("DROP TABLE \"skipcolumn\"");
        runSafe("drop type dollar");
    }

}
