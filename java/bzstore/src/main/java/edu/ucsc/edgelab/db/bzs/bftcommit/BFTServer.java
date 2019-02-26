package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.data.BpTree;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTServer extends DefaultSingleRecoverable {
    private BpTree db;
    private Logger logger;
    private long hashCode;

    public BFTServer(int id, BpTree db){
        this.db = db;
        logger = Logger.getLogger(BFTServer.class.getName());
        hashCode = 0;
        new ServiceReplica(id, this, this);

    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteOrdered(byte[] transactions, MessageContext msgCtx){
        byte[] reply = null;
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(transactions);
             ObjectInput objIn = new ObjectInputStream(byteIn);
             ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            LinkedList<byte[]> batch = (LinkedList<byte[]>) objIn.readObject();
            List<Long> hashes = new LinkedList<>();
            for(byte[] b : batch){
                Bzs.Transaction t = Bzs.Transaction.newBuilder().mergeFrom(b).build();
                hashCode += t.toByteString().hashCode();
                hashes.add(hashCode);
                for(Bzs.Write i : t.getWriteOperationsList()){
                    try {
                        db.commit(i.getKey(), i.getValue(),"");
                    }
                    catch (InvalidCommitException e){
                        System.out.println("Commit did not happen");
                    }
                }
            }
            System.out.println("HASHES :::::"+ hashes.toString());
            boolean bReply = Math.random() < 0.5;
            objOut.writeObject(hashes);
            objOut.flush();
            byteOut.flush();
            reply = byteOut.toByteArray();
        }
        catch (IOException | ClassNotFoundException e){
            logger.log(Level.SEVERE, "Occured during db operations executions", e);
        }
        return reply;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteUnordered(byte[] transactions, MessageContext msgCtx){
        byte[] reply = null;
        return  reply;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] getSnapshot(){
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(db);
            return byteOut.toByteArray();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error while taking snapshot", e);
        }
        return new byte[0];
    }

    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state){
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
             ObjectInput objIn = new ObjectInputStream(byteIn)) {
             db = (BpTree) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error while installing snapshot", e);
        }
    }



}
