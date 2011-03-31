package org.geotools.data.teradata;

import org.geotools.jdbc.JDBCPrimaryKeyTestSetup;
import org.geotools.jdbc.JDBCTestSetup;

public class TeradataPrimaryKeyTestSetup extends JDBCPrimaryKeyTestSetup {

    public TeradataPrimaryKeyTestSetup(JDBCTestSetup delegate) {
        super(delegate);
    }

    @Override
    protected void createAutoGeneratedPrimaryKeyTable() throws Exception {
        run("CREATE TABLE \"auto\"(\"key\" PRIMARY KEY not null generated always as identity (start with 1) integer, \"name\" varchar(200), geom ST_Geometry)");
        run("INSERT INTO SYSSPATIAL.GEOMETRY_COLUMNS (F_TABLE_CATALOG, F_TABLE_SCHEMA, F_TABLE_NAME, F_GEOMETRY_COLUMN, COORD_DIMENSION, SRID, GEOM_TYPE) VALUES ('','test','auto', 'geom', 2, 1619, 'ST_Geometry')");
        run("INSERT INTO \"auto\" (\"name\",\"geom\" ) VALUES ('one',NULL)");
        run("INSERT INTO \"auto\" (\"name\",\"geom\" ) VALUES ('two',NULL)");
        run("INSERT INTO \"auto\" (\"name\",\"geom\" ) VALUES ('three',NULL)");
    }

    @Override
    protected void createMultiColumnPrimaryKeyTable() throws Exception {
        run("CREATE TABLE \"multi\" ( \"key1\" int NOT NULL, \"key2\" varchar(200) NOT NULL, "
                + "\"name\" varchar(200), geom ST_Geometry)");
        run("INSERT INTO SYSSPATIAL.GEOMETRY_COLUMNS (F_TABLE_CATALOG, F_TABLE_SCHEMA, F_TABLE_NAME, F_GEOMETRY_COLUMN, COORD_DIMENSION, SRID, GEOM_TYPE) VALUES ('','test','multi', 'geom', 2, 1619, 'ST_Geometry')");
        run("ALTER TABLE \"multi\" ADD PRIMARY KEY (\"key1\",\"key2\")");

        run("INSERT INTO \"multi\" VALUES (1, 'x', 'one', NULL)");
        run("INSERT INTO \"multi\" VALUES (2, 'y', 'two', NULL)");
        run("INSERT INTO \"multi\" VALUES (3, 'z', 'three', NULL)");
    }

    @Override
    protected void createNonIncrementingPrimaryKeyTable() throws Exception {
        run("CREATE TABLE \"noninc\"(\"key\" PRIMARY KEY NOT NULL integer, \"name\" varchar(200), geom ST_Geometry)");
        run("INSERT INTO SYSSPATIAL.GEOMETRY_COLUMNS (F_TABLE_CATALOG, F_TABLE_SCHEMA, F_TABLE_NAME, F_GEOMETRY_COLUMN, COORD_DIMENSION, SRID, GEOM_TYPE) VALUES ('','test','noninc', 'geom', 2, 1619, 'ST_Geometry')");

        run("INSERT INTO \"noninc\" VALUES (1, 'one', NULL)");
        run("INSERT INTO \"noninc\" VALUES (2, 'two', NULL)");
        run("INSERT INTO \"noninc\" VALUES (3, 'three', NULL)");
    }

    @Override
    protected void createSequencedPrimaryKeyTable() throws Exception {
        run("CREATE TABLE \"seq\" ( \"key\" generated always as identity (start with 1)  integer, \"name\" varchar(200), geom ST_Geometry)");
        run("INSERT INTO SYSSPATIAL.GEOMETRY_COLUMNS (F_TABLE_CATALOG, F_TABLE_SCHEMA, F_TABLE_NAME, F_GEOMETRY_COLUMN, COORD_DIMENSION, SRID, GEOM_TYPE) VALUES ('','test','seq', 'geom', 2, 1619, 'ST_Geometry')");
//        run("CREATE SEQUENCE SEQ_KEY_SEQUENCE START WITH 1 OWNED BY \"seq\".\"key\"");

/*        run("INSERT INTO \"seq\" (\"key\", \"name\",\"geom\" ) VALUES (" +
"(SELECT NEXTVAL('SEQ_KEY_SEQUENCE')),'one',NULL)");
run("INSERT INTO \"seq\" (\"key\", \"name\",\"geom\" ) VALUES (" +
"(SELECT NEXTVAL('SEQ_KEY_SEQUENCE')),'two',NULL)");
run("INSERT INTO \"seq\" (\"key\", \"name\",\"geom\" ) VALUES (" +
"(SELECT NEXTVAL('SEQ_KEY_SEQUENCE')),'three',NULL)");*/

        run("INSERT INTO \"seq\" (\"name\",\"geom\") VALUES ('one',NULL)");
        run("INSERT INTO \"seq\" (\"name\",\"geom\") VALUES ('two',NULL)");
        run("INSERT INTO \"seq\" (\"name\",\"geom\") VALUES ('three',NULL)");
    }

    @Override
    protected void createNullPrimaryKeyTable() throws Exception {
        run("CREATE TABLE \"nokey\" ( \"name\" varchar(200))");
        run("INSERT INTO \"nokey\" VALUES ('one')");
        run("INSERT INTO \"nokey\" VALUES ('two')");
        run("INSERT INTO \"nokey\" VALUES ('three')");
    }

    @Override
    protected void createUniqueIndexTable() throws Exception {
        run("CREATE TABLE \"uniq\"(\"key\" UNIQUE NOT NULL int, \"name\" varchar(200), geom ST_Geometry)");
        run("INSERT INTO SYSSPATIAL.GEOMETRY_COLUMNS (F_TABLE_CATALOG, F_TABLE_SCHEMA, F_TABLE_NAME, F_GEOMETRY_COLUMN, COORD_DIMENSION, SRID, GEOM_TYPE) VALUES ('','test','uniq', 'geom', 2, 1619, 'ST_Geometry')");

//        run("CREATE UNIQUE INDEX \"uniq_key_index\" ON \"uniq\"(\"key\")");
        run("INSERT INTO \"uniq\" VALUES (1,'one',NULL)");
        run("INSERT INTO \"uniq\" VALUES (2,'two',NULL)");
        run("INSERT INTO \"uniq\" VALUES (3,'three',NULL)");
    }

    @Override
    protected void dropAutoGeneratedPrimaryKeyTable() throws Exception {
        runSafe("DELETE FROM SYSSPATIAL.GEOMETRY_COLUMNS WHERE F_TABLE_NAME = 'auto'");
        runSafe("DROP TABLE \"auto\"");
    }

    @Override
    protected void dropMultiColumnPrimaryKeyTable() throws Exception {
        runSafe("DELETE FROM SYSSPATIAL.GEOMETRY_COLUMNS WHERE F_TABLE_NAME = 'multi'");
        runSafe("DROP TABLE \"multi\"");
    }

    @Override
    protected void dropNonIncrementingPrimaryKeyTable() throws Exception {
        runSafe("DELETE FROM SYSSPATIAL.GEOMETRY_COLUMNS WHERE F_TABLE_NAME = 'noninc'");
        runSafe("DROP TABLE \"noninc\"");
    }

    @Override
    protected void dropSequencedPrimaryKeyTable() throws Exception {
        runSafe("DELETE FROM SYSSPATIAL.GEOMETRY_COLUMNS WHERE F_TABLE_NAME = 'seq'");
        runSafe("DROP SEQUENCE SEQ_KEY_SEQUENCE");
        runSafe("DROP TABLE \"seq\"");
    }

    @Override
    protected void dropNullPrimaryKeyTable() throws Exception {
        runSafe("DROP TABLE \"nokey\"");
    }

    @Override
    protected void dropUniqueIndexTable() throws Exception {
        runSafe("DELETE FROM SYSSPATIAL.GEOMETRY_COLUMNS WHERE F_TABLE_NAME = 'uniq'");
        runSafe("DROP TABLE \"uniq\"");
    }
}
