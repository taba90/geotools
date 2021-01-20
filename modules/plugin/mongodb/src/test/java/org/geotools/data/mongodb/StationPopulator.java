package org.geotools.data.mongodb;

import com.mongodb.*;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import org.junit.Before;
import org.junit.Test;

public class StationPopulator {

    MongoClient client;

    @Before
    public void setUp() {
        this.client = new MongoClient("localhost", 27017);
    }

    @Test
    public void insertStation1() throws URISyntaxException, IOException {
        insertStation("stations1.json");
    }

    @Test
    public void insertStation2() throws URISyntaxException, IOException {
        insertStation("stations2.json");
    }

    @Test
    public void insertStation3() throws URISyntaxException, IOException {
        insertStation("stations3.json");
    }

    @Test
    public void insertStation4() throws URISyntaxException, IOException {
        insertStation("stations4.json");
    }

    @Test
    public void insertStationAll() throws URISyntaxException, IOException {
        insertStation("stations1.json");
        insertStation("stations2.json");
        insertStation("stations3.json");
        insertStation("stations4.json");
    }

    private void insertStation(String station) throws IOException, URISyntaxException {
        // insert stations data
        File file = new File(getClass().getResource(station).toURI());
        String stationsContent1 = new String(Files.readAllBytes(file.toPath()));
        org.bson.Document document = org.bson.Document.parse(stationsContent1);
        client.getDatabase("datex3DB").getCollection("Stations").insertOne(document);
    }
}
