package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.util.logging.Logger;

public final class BZDatabaseController {
    private BackendDb db;
    private static final Logger LOGGER = Logger.getLogger(BZDatabaseController.class.getName());
    private static BZDatabaseController BZ_DATABASE_CONTROLLER;


    public static void initDB(Integer cid,Integer rid) throws RocksDBException {
        BZ_DATABASE_CONTROLLER = new BZDatabaseController(cid,rid);
    }
    private BZDatabaseController(Integer cid,Integer rid) throws RocksDBException {
        db = new BackendDb(cid,rid);
    }

    public static void commit (String key, BZStoreData data) throws InvalidCommitException {
//        LOGGER.info("Committing data with key: {"+key+"}");
        BZ_DATABASE_CONTROLLER.db.commit(key,data);
//        LOGGER.info("Committed data with key: {"+key+"}");
    }

    public static BZStoreData getlatest(String key) {
        BZStoreData dataHistory = BZ_DATABASE_CONTROLLER.db.get(key);
        if (dataHistory == null) {
//            String message = String.format("No data available for key=%s.", key);
//            LOGGER.log(Level.WARNING, message);
           return new BZStoreData();
        }
        return dataHistory;
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

//    public static void rollbackForKeys(List<String> keys) {
//        synchronized (BZ_DATABASE_CONTROLLER) {
//            for(String key: keys) {
//                List<BZStoreData> data = BZ_DATABASE_CONTROLLER.db.get(key);
//                data.remove(0);
//            }
//        }
//    }

//    public static void clearDatabase() {
//        BZ_DATABASE_CONTROLLER.db.clear();
//    }
}
