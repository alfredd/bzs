package edu.ucsc.edgelab.db.bzs.configuration;

import com.sun.javafx.fxml.PropertyNotFoundException;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class BZStoreProperties {

    public static final String CONFIG_PROPERTIES = "config.properties";

    public enum Configuration {
        server_port,
    }

    //    public static final String SERVER_START_PORT = "server_port";
    Properties bzsProperties;

    public BZStoreProperties() throws IOException {

        bzsProperties = new Properties();
        bzsProperties.load(
                new FileInputStream(
                        new File(
                                System.getProperty("user.dir") + "/src/main/resources/" + CONFIG_PROPERTIES
                        )
                )
        );

        for (Map.Entry entry : bzsProperties.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    public String getProperty(Configuration property) throws UnknownConfiguration {
        String property_value = bzsProperties.getProperty(property.name());
        if (property_value==null) {
            throw new UnknownConfiguration(String.format("Property '%s' not found in configurations.", property.name()));
        }
        return property_value;
    }


}

