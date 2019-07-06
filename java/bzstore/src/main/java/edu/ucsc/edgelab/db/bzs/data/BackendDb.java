package edu.ucsc.edgelab.db.bzs.data;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.logging.Level;
import java.util.logging.Logger;

class BackendDb {

    public static final String EPOCH = "Epoch";
    private RocksDB db;

    public static final Logger LOGGER = Logger.getLogger(BackendDb.class.getName());

    BackendDb(Integer clusterID, Integer replicaID) throws RocksDBException {
        RocksDB.loadLibrary();
        String dbPath = "BZS_data_" + clusterID + "_" + replicaID;
        Options options = new Options();
        options.setCreateIfMissing(true);
        db = RocksDB.open(options, dbPath);

        LOGGER.info("RocksDB database file opened: " + dbPath);
    }

    void commitEpochNumber(Integer epochNumber) throws RocksDBException {
        db.put(EPOCH.getBytes(), epochNumber.toString().getBytes());
    }

    Integer getEpochNumber() {
        Integer epoch = 0;
        try {
            byte[] value = db.get(EPOCH.getBytes());
            epoch = Integer.decode(new String(value));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not load epoch number from database");
        }
        return epoch;
    }

    void commit(String key, BZStoreData dataList) throws InvalidCommitException {
        Bzs.DBData dbData = Bzs.DBData.newBuilder()
                .setValue(dataList.value)
                .setVersion(dataList.version)
                .build();
        commitDBData(key, dbData);
    }

    void commitDBData(String key, Bzs.DBData dbData) throws InvalidCommitException {
        try {
            db.put(key.getBytes(), dbData.toByteArray());
        } catch (RocksDBException e) {
            throw new InvalidCommitException("Cannot commit key:" + key + ", " + dbData.toString() + e.getMessage(), e);
        }
    }

    void commit(String key, Bzs.SmrLogEntry smrEntry) throws InvalidCommitException {
        try {
            db.put(key.getBytes(), smrEntry.toByteArray());
        } catch (RocksDBException e) {
            throw new InvalidCommitException("Cannot commit key:" + key + ", " + smrEntry.toString() + e.getMessage(), e);
        }
    }

    byte[] getData(String key) {
        byte[] value = null;
        try {
            value = db.get(key.getBytes());
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Could not read data from db for key: " + key, e);
        }
        return value;
    }

    BZStoreData getBZStoreData(String key) {
        byte[] value = getData(key);
        if (value != null) {
            try {
                Bzs.DBData data = Bzs.DBData.parseFrom(value);
                BZStoreData bzStoreData = new BZStoreData();
                bzStoreData.value = data.getValue();
                bzStoreData.version = data.getVersion();
                return bzStoreData;
            } catch (InvalidProtocolBufferException e) {
                LOGGER.log(Level.WARNING,
                        "Could not parse data from db for key: " + key + ". " + e.getLocalizedMessage(), e);
            }
        }

        return new BZStoreData();
    }

    void close() {
        db.close();
    }

    public boolean containsKey(String key) {
        boolean found = true;
        try {
            found = db.get(key.getBytes()) != null;
        } catch (RocksDBException e) {
            found = false;
        }
        return false;
    }

    public Bzs.SmrLogEntry getSmrBlock(String epochNumber) {
        byte[] data = getData(epochNumber);
        if (data != null) {
            try {
                return Bzs.SmrLogEntry.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                LOGGER.log(Level.WARNING, "Could not parse data from database: " + e.getLocalizedMessage(), e);
            }
        }
        return null;
    }

    public void commit(String dv, byte[] data) throws RocksDBException {
        db.put(dv.getBytes(), data);
    }
}
