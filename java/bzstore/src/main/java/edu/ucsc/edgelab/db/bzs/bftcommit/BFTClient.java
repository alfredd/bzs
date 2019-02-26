package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.ServiceProxy;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class BFTClient {
    private ServiceProxy serviceProxy;


    public BFTClient(int ClientId){
        serviceProxy = new ServiceProxy(ClientId);
    }

    public List performCommit(List<Bzs.Transaction> t){
        LinkedList<Long> result = new LinkedList<>();
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)){
            List<byte[]> b = new LinkedList<>();
            for(Bzs.Transaction i : t){
                b.add(i.toByteArray());
            }
            objOut.writeObject(b);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if(reply.length == 0)
                throw new IOException();
            try(ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                ObjectInput objIn = new ObjectInputStream(byteIn)){
                result = (LinkedList<Long>)objIn.readObject();
                System.out.println(result.toString());
            }
        }
        catch (IOException | ClassNotFoundException e){
            System.out.print("Exception generated while committing transaction"+e.getMessage());
        }
        return result;
    }
}
