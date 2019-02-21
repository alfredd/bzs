package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.data.BpTree;

import java.io.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTServer extends DefaultSingleRecoverable {
    private BpTree db;
    private Logger logger;

    public BFTServer(int id, BpTree db){
        this.db = db;
        logger = Logger.getLogger(BFTServer.class.getName());
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
            List<Bzs.Transaction> batch = (List<Bzs.Transaction>) objIn.readObject();
            for(Bzs.Transaction t : batch){
                for(Bzs.Write operation : t.getWriteOperationsList()){
                    db.commit(operation.getKey(), operation.getValue());
                }
            }
            objOut.writeObject(true);
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
