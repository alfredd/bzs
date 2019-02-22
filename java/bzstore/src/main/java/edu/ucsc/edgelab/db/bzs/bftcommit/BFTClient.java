package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.ServiceProxy;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BFTClient {
    private ServiceProxy serviceProxy;


    public BFTClient(int ClientId){
        serviceProxy = new ServiceProxy(ClientId);
    }

    public boolean performCommit(List<Bzs.Transaction> t){
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)){
            objOut.writeObject(t);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if(reply.length == 0)
                return  false;
            try(ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                ObjectInput objIn = new ObjectInputStream(byteIn)){
                return (boolean)objIn.readObject();
            }
        }
        catch (IOException | ClassNotFoundException e){
            System.out.print("Exception generated while committing transaction"+e.getMessage());
        }
        return true;
    }
}
