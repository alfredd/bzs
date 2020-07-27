package edu.ucsc.edgelab.grpcdemo;

import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;

import java.io.IOException;

public class ROTBenchmark {

    public ROTBenchmark() throws IOException {
        BZStoreProperties properties = new BZStoreProperties();
        String host00Str = properties.getProperty(0, 0, BZStoreProperties.Configuration.host);
        String port00Str = properties.getProperty(0, 0, BZStoreProperties.Configuration.port);
        String host10Str = properties.getProperty(1, 0, BZStoreProperties.Configuration.host);
        String port10Str = properties.getProperty(1, 0, BZStoreProperties.Configuration.port);
        String host20Str = properties.getProperty(2, 0, BZStoreProperties.Configuration.host);
        String port20Str = properties.getProperty(2, 0, BZStoreProperties.Configuration.port);



    }

    public static void main(String[] args) {
        System.out.println(System.getProperty("user.dir"));
    }
}
