package edu.ucsc.edgelab.db.bzs.configuration;

import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class BZStoreProperties {

    public static final String CONFIG_PROPERTIES = "config.properties";

    public enum Configuration {
        port, host, leader, epoch_time_ms
    }

    private Properties bzsProperties;

    public BZStoreProperties() throws IOException {

        bzsProperties = new Properties();
        bzsProperties.load(
                new FileInputStream(
                        new File(
                                System.getProperty("user.dir") + "/src/main/resources/" + CONFIG_PROPERTIES
                        )
                )
        );
    }

    public String getProperty(final String id, Configuration property) throws UnknownConfiguration {
        String idname = getIdName(id);
        String property_value = bzsProperties.getProperty(idname+"."+property.name());
        if (property_value == null) {
            throw new UnknownConfiguration(String.format("Property '%s' not found in configurations.", property.name()));
        }
        return property_value;
    }

    public String getIdName(String id) {
        return "z"+id;
    }


    public String getProperty(Configuration property) throws UnknownConfiguration {
        String property_value = bzsProperties.getProperty(property.name());
        if (property_value == null) {
            throw new UnknownConfiguration(String.format("Property '%s' not found in configurations.", property.name()));
        }
        return property_value;
    }

}

