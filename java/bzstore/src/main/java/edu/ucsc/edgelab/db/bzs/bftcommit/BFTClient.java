package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.ServiceProxy;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.ID;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTClient {
    protected ServiceProxy serviceProxy;

    private static final Logger LOGGER = Logger.getLogger(BFTClient.class.getName());

    private static BFTClient bftClient = null;

    public static final void createConnection() {
        bftClient = new BFTClient(ID.getReplicaID());
    }

    public static final BFTClient getInstance() {
        if (bftClient==null)
            createConnection();
        return bftClient;
    }

    public BFTClient(int ClientId) {
        LOGGER.info("Trying to connect to BFT server: " + ClientId);
        serviceProxy = new ServiceProxy(ClientId);
    }

    public Bzs.TransactionBatchResponse performCommitPrepare(Bzs.TransactionBatch batch) {
        try {
            byte[] reply = sendBytesToBFTServer(batch.toByteArray());
            return Bzs.TransactionBatchResponse.parseFrom(reply);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception generated while committing transaction" + e.getLocalizedMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not parse response. Might be due to consensus failure. Transaction will be aborted.");
        }
        return null;
    }

    public byte[] sendBytesToBFTServer(byte[] data) {
//        LOGGER.info("Sending request to BFTServer. Data length: "+ data.length+" bytes");
        byte[] reply;
//        long smrCommitStartTime = System.currentTimeMillis();
        reply = serviceProxy.invokeOrdered(data);
//        long duration = System.currentTimeMillis()-smrCommitStartTime;

//        LOGGER.info("Time to Receive response from BFT Server: "+(duration)+"ms.");
        return reply;//Bzs.TransactionBatchResponse.parseFrom(reply);
    }


    public boolean performRead(Collection<Bzs.ROTransaction> roTransactions) {
        LOGGER.info("Received RO transaction batch to perform commit consensus.");
        boolean status;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            List<byte[]> transactionInBytes = new LinkedList<>();
            for (Bzs.ROTransaction roTransaction : roTransactions) {
                transactionInBytes.add(roTransaction.toByteArray());
            }
            objOut.writeObject(1);
            objOut.writeObject(transactionInBytes);
            objOut.flush();
            byteOut.flush();
            byte[] reply;
            try {
                reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
                if (reply.length == 0)
                    throw new IOException();
                status = true;
            } catch (RuntimeException e) {
                LOGGER.log(Level.SEVERE, "Commit Failed: " + e.getLocalizedMessage(), e);
                status = false;
                return status;
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception generated while committing transaction" + e.getLocalizedMessage(), e);
            status = false;
        }
        return status;
    }

    public int performDbCommit(Bzs.TransactionBatchResponse batchResponse) {
        Bzs.TransactionBatch batch = Bzs.TransactionBatch.newBuilder()
                .setID(batchResponse.getID())
                .setOperation(Bzs.Operation.BFT_COMMIT)
                .build();
        return dbCommit(batch);
    }

    public int dbCommit(Bzs.TransactionBatch batch) {
        return sendBytesToBftServer(batch);
    }

    @Deprecated
    private int sendBytesToBftServer(Bzs.TransactionBatch batch) {
        byte[] reply = sendBytesToBFTServer(batch.toByteArray());
        int id = -10;
        try {
            id = ByteBuffer.wrap(reply).getInt();
        } catch (Exception e) {
            LOGGER.warning("Could not get correct commit replicaID from response.");
        }
        return id;
    }

    public int abort(Bzs.TransactionBatch batch) {
        return sendBytesToBftServer(batch);
    }

    public int prepareSmrLogEntry(final Bzs.SmrLogEntry logEntry) {
        String id = Integer.toString(logEntry.getEpochNumber());
        Bzs.TransactionBatch batch =
                Bzs.TransactionBatch.newBuilder().setID(id).setOperation(Bzs.Operation.BFT_SMR_PREPARE).setSmrLogEntry(logEntry).build();
        byte[] response = sendBytesToBFTServer(batch.toByteArray());

        // TODO: How to know when the BFT consensus has failed?
        if (response == null) {
            LOGGER.log(Level.SEVERE, "Response returned null for transaction batch: "+ batch);
            return  -10;
        }
        return response.length;
    }

    public void commitSMR(final Integer epochNumber) {
        String id = Integer.toString(epochNumber);
        Bzs.TransactionBatch batch = Bzs.TransactionBatch.newBuilder().setOperation(Bzs.Operation.BFT_SMR_COMMIT).setID(id).build();
        sendBytesToBFTServer(batch.toByteArray());
    }
}
