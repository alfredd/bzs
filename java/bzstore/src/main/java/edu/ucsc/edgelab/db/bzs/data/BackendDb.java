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

    private RocksDB db;

    public static final Logger LOGGER = Logger.getLogger(BackendDb.class.getName());

    BackendDb(Integer clusterID, Integer replicaID) throws RocksDBException {
        RocksDB.loadLibrary();
        String dbPath = "BZS_data_" + clusterID+"_"+replicaID;
        Options options = new Options();
        options.setCreateIfMissing(true);
        db = RocksDB.open(options, dbPath);

        LOGGER.info("RocksDB database file opened: " + dbPath);
    }


    void commit(String key, BZStoreData dataList) throws InvalidCommitException {
        Bzs.DBData dbData = Bzs.DBData.newBuilder()
                .setValue(dataList.value)
                .setVersion(dataList.version)
                .build();
        try {
            db.put(key.getBytes(), dbData.toByteArray());
        } catch (RocksDBException e) {
            throw new InvalidCommitException("Cannot commit key:" + key + ", " + dbData.toString() + e.getMessage(), e);
        }
    }


    BZStoreData get(String key) {
        byte[] value = new byte[0];
        try {
            value = db.get(key.getBytes());
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Could not read data from db for key: " + key, e);
        }
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
}
