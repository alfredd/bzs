package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class BZDatabaseController {
    private BackendDb db;
    private static final Logger LOGGER = Logger.getLogger(BZDatabaseController.class.getName());
    private static BZDatabaseController BZ_DATABASE_CONTROLLER;

    public static void initDB(Integer cid, Integer rid) throws RocksDBException {
        BZ_DATABASE_CONTROLLER = new BZDatabaseController(cid, rid);
        Integer latestEpochCount = BZ_DATABASE_CONTROLLER.db.getEpochNumber();
        LOGGER.info("Printing SMR log");
        for (int e = 0; e <= latestEpochCount; e++) {
            Bzs.SmrLogEntry smrEntry = getSmrBlock(e);
            if (smrEntry != null) {
                LOGGER.log(Level.INFO, String.format("Smr Log entry #%d: %s", e, smrEntry.toString()));
            } else {
                LOGGER.log(Level.WARNING, String.format("Error. SMRLOG[%d] is null or not present in the log.", e));
            }
        }
    }

    private BZDatabaseController(Integer cid, Integer rid) throws RocksDBException {
        db = new BackendDb(cid, rid);
    }

    public static void commitDBData(String key, Bzs.DBData dbData) throws InvalidCommitException {
        BZ_DATABASE_CONTROLLER.db.commitDBData(key, dbData);
    }

    public static void commit(String key, BZStoreData data) throws InvalidCommitException {
//        LOGGER.info("Committing data with key: {"+key+"}");
        BZ_DATABASE_CONTROLLER.db.commit(key, data);
//        LOGGER.info("Committed data with key: {"+key+"}");
    }

    public static void setEpochCount(Integer epochNumber) {
        try {
            BZ_DATABASE_CONTROLLER.db.commitEpochNumber(epochNumber);
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
        }
    }

    public static Integer getEpochCount() {
        return BZ_DATABASE_CONTROLLER.db.getEpochNumber();
    }

    public static BZStoreData getlatest(String key) {
        BZStoreData dataHistory = BZ_DATABASE_CONTROLLER.db.getBZStoreData(key);
        if (dataHistory == null) {
//            String message = String.format("No data available for key=%s.", key);
//            LOGGER.log(Level.WARNING, message);
            return new BZStoreData();
        }
        return dataHistory;
    }

    public static boolean containsKey(String key) {
        boolean status = false;
        BZ_DATABASE_CONTROLLER.db.containsKey(key);
        return status;
    }

    public static void initializeDb(ByteArrayInputStream dbIOStream) {
//        ObjectInput objIn = new ObjectInputStream(dbIOStream);
//        BZ_DATABASE_CONTROLLER.db = (BpTree) objIn.readObject();
    }

    public static byte[] getDBSnapshot() {
        // TODO: Need a better way to replicate data.
//        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
//             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
//            objOut.writeObject(BZ_DATABASE_CONTROLLER.db);
//            return byteOut.toByteArray();
//        } catch (IOException e) {
//            LOGGER.log(Level.SEVERE, "Error while taking snapshot", e);
//        }

        //Need to check this. Can this be replaced by a runtime exception.
        return new byte[0];
    }

    public static void commitSmrBlock(Integer epochNumber, Bzs.SmrLogEntry logEntry) throws InvalidCommitException {
        BZ_DATABASE_CONTROLLER.db.commit(getSMRLogKey(epochNumber), logEntry);
    }

    public static Bzs.SmrLogEntry getSmrBlock(Integer epochNumber) {
        return BZ_DATABASE_CONTROLLER.db.getSmrBlock(getSMRLogKey(epochNumber));
    }

//    public static Bzs.SmrLogEntry getLatestSmrBlock

    private static String getSMRLogKey(Integer epochNumber) {
        return String.format("S.%d", epochNumber.intValue());
    }

    public static void commitDepVector(Map<Integer, Integer> depVec) {
        Bzs.DVec dVec = Bzs.DVec.newBuilder().putAllDepVector(depVec).build();
        try {
            BZ_DATABASE_CONTROLLER.db.commit("DV", dVec.toByteArray());
        } catch (RocksDBException e) {
            LOGGER.log(Level.SEVERE, String.format("Could not commit dependency vector information to Database: %s", dVec.toString()));
        }
    }

    public static Bzs.DVec getDepVector() {
        Bzs.DVec depVec = null;
        try {
            depVec = Bzs.DVec.parseFrom(BZ_DATABASE_CONTROLLER.db.getData("DV"));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not parse dependency vector data from DB. "+e.getLocalizedMessage(), e);
        }
        return depVec;
    }

    public static void close() {
        BZ_DATABASE_CONTROLLER.db.close();
    }
//    public static void rollbackForKeys(List<String> keys) {
//        synchronized (BZ_DATABASE_CONTROLLER) {
//            for(String key: keys) {
//                List<BZStoreData> data = BZ_DATABASE_CONTROLLER.db.getBZStoreData(key);
//                data.remove(0);
//            }
//        }
//    }

//    public static void clearDatabase() {
//        BZ_DATABASE_CONTROLLER.db.clear();
//    }
}