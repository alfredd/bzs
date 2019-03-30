package edu.ucsc.edgelab.db.bzs.data;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.logging.Level;
import java.util.logging.Logger;

class BackendDb {

    private RocksDB db;

    public static final Logger LOGGER = Logger.getLogger(BackendDb.class.getName());

    BackendDb(int id) throws RocksDBException {
        // TODO: Setup column families to store version number.
        RocksDB.loadLibrary();
        String dbPath = "BZS_data_" + id;
        Options options = new Options();
        options.setCreateIfMissing(true);
        db = RocksDB.open(options, dbPath);

        LOGGER.info("RocksDB database file opened: " + dbPath);
    }

    void commit(String key, String value) throws RocksDBException {

        boolean loggable = LOGGER.isLoggable(Level.FINE);
        byte[] keyBytes = key.getBytes();
        String prevValue = get(key);
        if (loggable)
            LOGGER.log(Level.FINE, "Previous value: " + prevValue);
        long start = System.nanoTime();
        db.put(keyBytes, value.getBytes());
        if (loggable)
            LOGGER.log(Level.FINE, "Total time to write data to DB: " + (System.nanoTime() - start));
    }

    String get(String key) throws RocksDBException {
        long start = System.nanoTime();
        byte[] value = db.get(key.getBytes());
        if (LOGGER.isLoggable(Level.FINE))
            LOGGER.log(Level.FINE, "Total time to get data from DB: " + (System.nanoTime() - start));

        return value == null ? "" : new String(value);

    }

    void close() {
        db.close();
    }

    public static void main(String[] args) throws RocksDBException {
        BackendDb db = new BackendDb(0);
        db.commit("X", "3");
        LOGGER.info("X=" + db.get("X"));
        db.close();
    }
}
