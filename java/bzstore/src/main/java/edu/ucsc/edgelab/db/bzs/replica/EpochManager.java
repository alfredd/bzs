package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.configuration.Configuration;

import java.util.Timer;
import java.util.TimerTask;
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
        this.epochTimeInMS = Configuration.getEpochTimeInMS();
    }



    public void startEpochMaintenance() {
        TimedEpochMaintainer epochMaintainer = new TimedEpochMaintainer();
        epochMaintainer.setProcessor(transactionProcessor);
        Timer epochTimer = new Timer("TimedEpochMaintainer", true);
        LOGGER.info(String.format("Timer rate = %dms and delay %dms", epochTimeInMS, epochTimeInMS));
        epochTimer.scheduleAtFixedRate(epochMaintainer, epochTimeInMS, epochTimeInMS);
    }

    public void startScheduledTimerTask(TimerTask task, String name, double timeMultiplier) {
        Timer timer = new Timer(name, true);
        double scheduledTime = epochTimeInMS * timeMultiplier;
        long period = (long) scheduledTime;
        LOGGER.info("Scheduling timer task: "+ name+" with a period of "+period+"ms");
        timer.scheduleAtFixedRate(task, epochTimeInMS * 5, period);
    }
}



