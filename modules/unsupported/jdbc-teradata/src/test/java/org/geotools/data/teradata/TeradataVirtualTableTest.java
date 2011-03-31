package org.geotools.data.teradata;

import org.geotools.jdbc.JDBCDataStoreAPITestSetup;
import org.geotools.jdbc.JDBCVirtualTableTest;

public class TeradataVirtualTableTest extends JDBCVirtualTableTest {

    @Override
    protected JDBCDataStoreAPITestSetup createTestSetup() {
        return new TeradataDataStoreAPITestSetup(new TeradataTestSetup());
    }

}
