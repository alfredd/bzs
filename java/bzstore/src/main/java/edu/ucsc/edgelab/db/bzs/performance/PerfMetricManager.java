package edu.ucsc.edgelab.db.bzs.performance;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class PerfMetricManager {

    private static final PerfMetricManager perfMetricManager = new PerfMetricManager();

    public static PerfMetricManager getInstance() {
        return perfMetricManager;
    }

    private ConcurrentHashMap<Integer, PerfData> performanceInfo;

    private PerfMetricManager() {
        performanceInfo = new ConcurrentHashMap<>();
    }

    public void insertLocalPerfData (
            Integer epochNumber,
            int lrwtCount,
            int drwtLPrepareCount,
            int drwtLCommitCount,
            int numBytes,
            long epochProcessingTimeMS,
            long localPrepareTime,
            long localCommitTime
            ) {
        synchronized (epochNumber) {

            PerfData performanceData = getPerfData(epochNumber);
            performanceData.setLrwtCount(lrwtCount);
            performanceData.setDrwtLPrepareCount(drwtLPrepareCount);
            performanceData.setDrwtLCommitCount(drwtLCommitCount);
            performanceData.setNumBytes(numBytes);
            performanceData.setEpochProcessingTimeMS(epochProcessingTimeMS);
            performanceData.setLocalCommitTime(localCommitTime);
            performanceData.setLocalPrepareTime(localPrepareTime);
        }
    }

    private PerfData getPerfData(Integer epochNumber) {
        if (!performanceInfo.containsKey(epochNumber)) {
            performanceInfo.put(epochNumber, new PerfData());
        }

        return performanceInfo.get(epochNumber);
    }

    public void insertDTxnPerfData (Integer epochNumber, long dRWT2PCTime, long dRWT2PCCount) {
        synchronized (epochNumber) {
            PerfData performanceData = getPerfData(epochNumber);
            performanceData.setdRWT2PCCount(dRWT2PCCount);
            performanceData.setdRWT2PCTime(dRWT2PCTime);
        }
    }

    public void logMetrics (Integer epochNumber, Logger logger) {
        synchronized (epochNumber) {
            if (performanceInfo.containsKey(epochNumber)) {
                PerfData perfData = performanceInfo.get(epochNumber);
                logger.info(String.format("Epoch Metrics: " +
                        "(#Epoch, #Bytes, #lRWT, #dRWTLPrepare, #dRWTLCommit, EPxngTime, LPrepTime, LCommitTime, #dRWT2PC, dRWT2PCTime) = " +
                                "( %d, %d, %d, %d, %d, %d, %d, %d, %d, %d )",
                        epochNumber.intValue(),
                        perfData.getNumBytes(),
                        perfData.getLrwtCount(),
                        perfData.getDrwtLPrepareCount(),
                        perfData.getDrwtLCommitCount(),
                        perfData.getEpochProcessingTimeMS(),
                        perfData.getLocalPrepareTime(),
                        perfData.getLocalCommitTime(),
                        perfData.getdRWT2PCCount(),
                        perfData.getdRWT2PCTime()
                        )

                );
            }
        }
    }

}


class PerfData {
    private int lrwtCount = 0;
    private int drwtLPrepareCount = 0;
    private int drwtLCommitCount = 0;
    private int numBytes = 0;
    private long epochProcessingTimeMS = 0;
    private long localPrepareTime = 0;
    private long localCommitTime = 0;
    private long dRWT2PCTime = 0;
    private long dRWT2PCCount = 0;
    private int dRWT2PCTimeCounter=0;


    public int getLrwtCount() {
        return lrwtCount;
    }

    public void setLrwtCount(int lrwtCount) {
        this.lrwtCount += lrwtCount;
    }

    public int getDrwtLPrepareCount() {
        return drwtLPrepareCount;
    }

    public void setDrwtLPrepareCount(int drwtLPrepareCount) {
        this.drwtLPrepareCount += drwtLPrepareCount;
    }

    public int getDrwtLCommitCount() {
        return drwtLCommitCount;
    }

    public void setDrwtLCommitCount(int drwtLCommitCount) {
        this.drwtLCommitCount += drwtLCommitCount;
    }

    public int getNumBytes() {
        return numBytes;
    }

    public void setNumBytes(int numBytes) {
        this.numBytes += numBytes;
    }

    public long getEpochProcessingTimeMS() {
        return epochProcessingTimeMS;
    }

    public void setEpochProcessingTimeMS(long epochProcessingTimeMS) {
        this.epochProcessingTimeMS = epochProcessingTimeMS;
    }

    public long getLocalPrepareTime() {
        return localPrepareTime;
    }

    public void setLocalPrepareTime(long localPrepareTime) {
        this.localPrepareTime = localPrepareTime;
    }

    public long getLocalCommitTime() {
        return localCommitTime;
    }

    public void setLocalCommitTime(long localCommitTime) {
        this.localCommitTime = localCommitTime;
    }

    public long getdRWT2PCTime() {
        return dRWT2PCTime;//((double) dRWT2PCTime)/dRWT2PCTimeCounter;
    }

    public void setdRWT2PCTime(long dRWT2PCTime) {
        this.dRWT2PCTime += dRWT2PCTime;
//        this.dRWT2PCTimeCounter+=1;
    }

    public long getdRWT2PCCount() {
        return dRWT2PCCount;
    }

    public void setdRWT2PCCount(long dRWT2PCCount) {
        this.dRWT2PCCount += dRWT2PCCount;
    }
}
