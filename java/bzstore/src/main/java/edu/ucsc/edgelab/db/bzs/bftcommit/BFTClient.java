package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.ServiceProxy;
import edu.ucsc.edgelab.db.bzs.Bzs;

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

    public BFTClient(int ClientId) {
        LOGGER.info("Trying to connect to server: "+ClientId);
        serviceProxy = new ServiceProxy(ClientId);
    }

    public Bzs.TransactionBatchResponse performCommitPrepare(Bzs.TransactionBatch batch) {
//        LOGGER.info("Received transaction batch to perform commit consensus.");

        try {
            byte[] reply = sendBytesToBFTServer(batch.toByteArray());
            return Bzs.TransactionBatchResponse.parseFrom(reply);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception generated while committing transaction" + e.getLocalizedMessage(), e);
        } catch (Exception e ) {
            LOGGER.log(Level.WARNING, "Could not parse response. Might be due to consensus failure. Transaction will be aborted.");
        }
        return null;
    }

    public byte[] sendBytesToBFTServer(byte[] data) {
        LOGGER.info("Starting db commit from client.");
        byte[] reply;
        reply = serviceProxy.invokeOrdered(data);
        return reply;//Bzs.TransactionBatchResponse.parseFrom(reply);
/*        try {
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception generated while committing transaction" + e.getLocalizedMessage(), e);
        } catch (Exception e ) {
            LOGGER.log(Level.WARNING, "Could not parse response. Might be due to consensus failure. Transaction will be aborted.");
        }
        LOGGER.info("Commit consensus FAILED for transaction. Returning NULL.");
        return null;*/
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
//        Bzs.BFTCommit commitData = Bzs.BFTCommit.newBuilder().addAllTransactions(batchResponse.getResponsesList()).build();
//        Bzs.TransactionBatch batch = Bzs.TransactionBatch.newBuilder().setBftCommit(commitData).setID(batchResponse.getID()).build();
        Bzs.TransactionBatch batch = Bzs.TransactionBatch.newBuilder()
                .setID(batchResponse.getID())
                .setOperation(Bzs.Operation.BFT_COMMIT)
                .build();
        byte[] reply = sendBytesToBFTServer(batch.toByteArray());
        int id=-10;
        try {
            id = ByteBuffer.wrap(reply).getInt();
        } catch (Exception e) {
            LOGGER.warning("Could not get correct commit id from response.");
        }
        return id;
    }
}
