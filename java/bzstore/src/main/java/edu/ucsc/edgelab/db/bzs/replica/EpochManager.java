package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;

import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EpochManager {

    private static final Logger LOGGER = Logger.getLogger(EpochManager.class.getName());
    private Integer epochTimeInMS;
    private TransactionProcessor transactionProcessor;

    public EpochManager(TransactionProcessor transactionProcessor) {
        this();
        this.transactionProcessor = transactionProcessor;
    }

    private EpochManager() {
        this.epochTimeInMS = getEpochTimeInMS();
    }

    public static Integer getEpochTimeInMS() {
        int epochTime;
        try {
            BZStoreProperties properties = new BZStoreProperties();
            epochTime = Integer.decode(properties.getProperty(BZStoreProperties.Configuration.epoch_time_ms));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception occurred while getting epoch time. " + e.getLocalizedMessage());
            epochTime = edu.ucsc.edgelab.db.bzs.configuration.Configuration.getDefaultEpochTimeInMS();
        }
        return epochTime;
    }

    public void startEpochMaintenance() {
        TimedEpochMaintainer epochMaintainer = new TimedEpochMaintainer();
        epochMaintainer.setProcessor(transactionProcessor);
        Timer epochTimer = new Timer("TimedEpochMaintainer", true);
        LOGGER.info(String.format("Timer rate = %dms and delay %dms", epochTimeInMS, epochTimeInMS));
        epochTimer.scheduleAtFixedRate(epochMaintainer, epochTimeInMS, epochTimeInMS);
    }
}
