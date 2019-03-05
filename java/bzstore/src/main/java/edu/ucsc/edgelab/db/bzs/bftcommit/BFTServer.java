package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidDataAccessException;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTServer extends DefaultSingleRecoverable {

    private Logger logger;
    private String transaction_hash;

    public BFTServer(int id) {
        logger = Logger.getLogger(BFTServer.class.getName());
        transaction_hash = "";
        new ServiceReplica(id, this, this);

    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteOrdered(byte[] transactions, MessageContext msgCtx) {
        // TODO: Needs explanation.
        byte[] reply = null;
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(transactions);
             ObjectInput objIn = new ObjectInputStream(byteIn);
             ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            Integer type = (Integer) objIn.readObject();
            if (type == 0) {//Performing BFT commit
                System.out.println("=========Performing BFT Transactions=========");
                LinkedList<byte[]> batch = (LinkedList<byte[]>) objIn.readObject();
                List<String> hashes = new LinkedList<>();
                for (byte[] b : batch) {
                    Bzs.Transaction t = Bzs.Transaction.newBuilder().mergeFrom(b).build();
                    transaction_hash = generateHash(transaction_hash + t.toString());
                    hashes.add(transaction_hash);

                    // COMMIT IS NOT REQUIRED HERE
/*                    for (Bzs.Write i : t.getWriteOperationsList()) {
                        try {
                            BZStoreData data = new BZStoreData();
                            data.
                                    BZDatabaseController.commit(data);
                            db.commit(i.getKey(), i.getValue(), "");
                        } catch (InvalidCommitException e) {
                            System.out.println("Commit did not happen");
                        }
                    }*/
                }
                System.out.println("HASHES :::::" + hashes.toString());
                boolean bReply = Math.random() < 0.5;
                objOut.writeObject(hashes);
                objOut.flush();
                byteOut.flush();
                reply = byteOut.toByteArray();
            } else {
                LinkedList<byte[]> batch = (LinkedList<byte[]>) objIn.readObject();
                List<String> hashes = new LinkedList<>();
                for (byte[] b : batch) {
                    String b_hash = "";
                    Bzs.ROTransaction t = Bzs.ROTransaction.newBuilder().mergeFrom(b).build();
                    for (Bzs.Read i : t.getReadOperationsList()) {
                        String value;
                        long version;
                        value = "";
                        version = 0;
                        if (i.getKey() != null) {
                            try {
                                BZStoreData storeData = BZDatabaseController.getlatest(i.getKey());
                                value = storeData.value;
                                version = storeData.version;
                            } catch (InvalidDataAccessException e) {
                                e.printStackTrace();
                            }
                        }
                        b_hash = generateHash(b_hash + value + version);
                    }
                    hashes.add(b_hash);
                }
                System.out.println("HASHES :::::" + hashes.toString());
                boolean bReply = Math.random() < 0.5;
                objOut.writeObject(hashes);
                objOut.flush();
                byteOut.flush();
                reply = byteOut.toByteArray();
            }

        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Occured during db operations executions", e);
        }
        return reply;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteUnordered(byte[] transactions, MessageContext msgCtx) {
        byte[] reply = null;
        return reply;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] getSnapshot() {
        // TODO: Needs explanation.
/*        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(db);
            return byteOut.toByteArray();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error while taking snapshot", e);
        }*/
        return new byte[0];
    }

    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
        try {
            BZDatabaseController.initializeDb(byteIn);
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error while installing snapshot", e);
        } finally {
            try {
                byteIn.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error while closing byte stream during  installSnapshot", e);
            }
        }
    }

    private static String generateHash(String input) {
        String hash = DigestUtils.md5Hex(input).toUpperCase();
        return hash;
    }

}
