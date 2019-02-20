package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import com.google.j2objc.annotations.ObjectiveCName;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BpTree;
import sun.rmi.runtime.Log;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class bftServer extends DefaultSingleRecoverable {
    private BpTree db;
    private Logger logger;

    public bftServer(int id, BpTree db){
        this.db = db;
        logger = Logger.getLogger(bftServer.class.getName());
        new ServiceReplica(id, this, this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx){
        byte[] reply = null;
        return reply;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx){
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
