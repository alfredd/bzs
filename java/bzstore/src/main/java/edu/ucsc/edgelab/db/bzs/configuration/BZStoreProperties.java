package edu.ucsc.edgelab.db.bzs.configuration;

import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

public class BZStoreProperties {

    public static final String CONFIG_PROPERTIES = "config.properties";
    private static final Logger LOGGER = Logger.getLogger(BZStoreProperties.class.getName());
    public enum Configuration {
        port, host, leader, epoch_time_ms, epoch_batch_size, delay_start, data
    }

    private Properties bzsProperties;

    public BZStoreProperties() throws IOException {

        bzsProperties = new Properties();
        String pathname = System.getProperty("user.dir") + "/" + CONFIG_PROPERTIES;
        LOGGER.info("Properties path: "+pathname);
        bzsProperties.load(
                new FileInputStream(
                        new File(
                                pathname
                        )
                )
        );
    }

    public String getProperty(final Integer clusterID, final Integer replicaID, Configuration property) throws UnknownConfiguration {
        String idName = getIdName(clusterID, replicaID, property);
        String property_value = bzsProperties.getProperty(idName);
        if (property_value == null) {
            throw new UnknownConfiguration(String.format("Property '%s' not found in configurations.", property.name()));
        }
        return property_value;
    }

    String getIdName(Integer clusterID, Integer replicaID, Configuration property) {
        return String.format("c.%d.%d.%s",clusterID,replicaID,property.name());
    }


    public String getProperty(Integer clusterID, Configuration property) throws UnknownConfiguration {
        String property_value = bzsProperties.getProperty(property.name()+"."+clusterID);
        if (property_value == null) {
            throw new UnknownConfiguration(String.format("Property '%s' not found in configurations.", property.name()));
        }
        return property_value;
    }
    public String getProperty(Configuration property) throws UnknownConfiguration {
        String property_value = bzsProperties.getProperty(property.name());
        if (property_value == null) {
            throw new UnknownConfiguration(String.format("Property '%s' not found in configurations.", property.name()));
        }
        return property_value;
    }

}

