package edu.ucsc.edgelab.db.bzs.replica;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Hello world!
 */
public class BZStoreServer {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello World!");
        Properties p = new Properties();
        p.load (
                new FileInputStream (
                        new File(
                                System.getProperty("user.dir") + "/src/main/resources/config.properties"
                        )
                )
        );

        for (Map.Entry entry: p.entrySet()) {
            System.out.println(entry.getKey() +": "+ entry.getValue());
        }
    }
}

