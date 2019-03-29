package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;

import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EpochManager {

    private static final Logger LOGGER = Logger.getLogger(EpochManager.class.getName());
    private Integer epochTimeInMS;

    public EpochManager() {
        try {
            BZStoreProperties properties = new BZStoreProperties();
            this.epochTimeInMS = Integer.decode(properties.getProperty(BZStoreProperties.Configuration.epoch_time_ms));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception occurred while getting epoch time. " + e.getLocalizedMessage());
            this.epochTimeInMS = Configuration.getDefaultEpochTimeInMS();
        }
    }

    public void setTransactionProcessor(TransactionProcessor processor) {
        EpochMaintainer epochMaintainer = new EpochMaintainer();
        epochMaintainer.setProcessor(processor);
        Timer epochTimer = new Timer("EpochMaintainer", true);
        epochTimer.scheduleAtFixedRate(epochMaintainer, epochTimeInMS, epochTimeInMS);
    }
}
