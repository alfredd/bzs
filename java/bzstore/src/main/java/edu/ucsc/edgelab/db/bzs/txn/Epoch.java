package edu.ucsc.edgelab.db.bzs.txn;

public class Epoch {
    private static int epochNumber;
    private static int epochUnderExecution;

    public static void setEpochNumber(final int epochNumber) {
        Epoch.epochNumber = epochNumber;
    }

    public static void setEpochUnderExecution(final int epochUnderExecution) {
        Epoch.epochUnderExecution = epochUnderExecution;
    }

    public static int getEpochNumber() {
        return epochNumber;
    }

    public static int getEpochUnderExecution() {
        return epochUnderExecution;
    }
}
