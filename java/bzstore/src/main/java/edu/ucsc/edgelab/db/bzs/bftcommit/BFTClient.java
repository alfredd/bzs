package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.ServiceProxy;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.io.*;
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

    public List<Long> performCommit(Collection<Bzs.Transaction> transactions) {
        LOGGER.info("Received transaction batch to perform commit consensus.");
        LinkedList<Long> result = new LinkedList<>();
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            List<byte[]> transactionInBytes = new LinkedList<>();
            for (Bzs.Transaction i : transactions) {
                transactionInBytes.add(i.toByteArray());
            }
            objOut.writeObject(0);
            objOut.writeObject(transactionInBytes);
            objOut.flush();
            byteOut.flush();

            byte[] reply;
            try {
                reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
                if (reply.length == 0)
                    throw new IOException();
            } catch (RuntimeException e) {
                LOGGER.log(Level.SEVERE, "Commit Failed: " + e.getLocalizedMessage(), e);
                return result;
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (LinkedList<Long>) objIn.readObject();
                LOGGER.info("Result response: " + result.toString());
            }
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Exception generated while committing transaction" + e.getLocalizedMessage(), e);
        }
        LOGGER.info("Commit consensus completed for transaction.");
        return result;
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
