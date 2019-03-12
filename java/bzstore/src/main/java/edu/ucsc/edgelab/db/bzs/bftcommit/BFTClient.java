package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.ServiceProxy;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTClient {
    private ServiceProxy serviceProxy;

    private static final Logger LOGGER = Logger.getLogger(BFTClient.class.getName());

    public BFTClient(int ClientId) {
        serviceProxy = new ServiceProxy(ClientId);
    }

    public List<Bzs.TransactionResponse> performCommit(Collection<Bzs.Transaction> transactions) {
        LOGGER.info("Received transaction batch to perform commit consensus.");
        LinkedList<Long> result = new LinkedList<>();

        Bzs.TransactionBatch.Builder batchBuilder = Bzs.TransactionBatch.newBuilder();

        for (Bzs.Transaction transaction : transactions) {
            batchBuilder.addTransactions(transaction);
        }
        Bzs.TransactionBatch batch = batchBuilder.build();

        List<Bzs.TransactionResponse> transactionResponses = new LinkedList<>();

        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            objOut.write(batch.toByteArray());
            objOut.flush();
            byteOut.flush();

            byte[] reply;
            reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            Bzs.TransactionBatchResponse response = Bzs.TransactionBatchResponse.parseFrom(reply);
            return response.getResponsesList();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception generated while committing transaction" + e.getLocalizedMessage(), e);
        }
        LOGGER.info("Commit consensus completed for transaction.");
        return null;
    }

    public boolean performRead(Collection<Bzs.ROTransaction> roTransactions) {
        LOGGER.info("Received RO transaction batch to perform commit consensus.");
        boolean status;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            List<byte[]> transactionInBytes = new LinkedList<>();
            for (Bzs.ROTransaction i : roTransactions) {
                transactionInBytes.add(i.toByteArray());
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
}
