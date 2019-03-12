package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidDataAccessException;

import java.io.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class BZDatabaseController {
    private BpTree db;
    private static final BZDatabaseController BZ_DATABASE_CONTROLLER = new BZDatabaseController();
    private static final Logger LOGGER = Logger.getLogger(BZDatabaseController.class.getName());

    private BZDatabaseController() {
        db = new BpTree();
    }

    public static void commit (String key, BZStoreData data) throws InvalidCommitException {
        LOGGER.info("Committing data with key: {"+key+"}");
        BZ_DATABASE_CONTROLLER.db.commit(key,data.value,data.digest);
        LOGGER.info("Committed data with key: {"+key+"}");
    }

    public static BZStoreData getlatest(String key) throws InvalidDataAccessException {
        List<BZStoreData> dataHistory = BZ_DATABASE_CONTROLLER.db.get(key);
        if (dataHistory == null) {
            String message = String.format("No data available for key=%s.", key);
            LOGGER.log(Level.WARNING, message);
            throw new InvalidDataAccessException(message);
        }
        return dataHistory.get(0);
    }

    public static void initializeDb(ByteArrayInputStream dbIOStream) throws IOException, ClassNotFoundException {
        ObjectInput objIn = new ObjectInputStream(dbIOStream);
        BZ_DATABASE_CONTROLLER.db = (BpTree) objIn.readObject();
    }

    public static byte[] getDBSnapshot() {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(BZ_DATABASE_CONTROLLER.db);
            return byteOut.toByteArray();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error while taking snapshot", e);
        }

        //Need to check this. Can this be replaced by a runtime exception.
        return new byte[0];
    }
}
