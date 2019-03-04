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
            objOut.writeObject(new Integer(0));
            objOut.writeObject(b);
            objOut.flush();
            byteOut.flush();

            byte[] reply;
            try {
                reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
                if (reply.length == 0)
                    throw new IOException();
            }
            catch (RuntimeException e){
                System.out.println("Commit Failed: " + e.getMessage());
                return result;
            }
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

    public boolean performRead(List<Bzs.ROTransaction> t){
        boolean status = false;
        try(ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutput objOut = new ObjectOutputStream(byteOut)){
            List<byte[]> b = new LinkedList<>();
            for(Bzs.ROTransaction i : t){
                b.add(i.toByteArray());
            }
            objOut.writeObject(new Integer(1));
            objOut.writeObject(b);
            objOut.flush();
            byteOut.flush();
            byte[] reply;
            try {
                reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
                if (reply.length == 0)
                    throw new IOException();
                status = true;
            }
            catch (RuntimeException e){
                System.out.println("Commit Failed" + e.getMessage());
                status = false;
                return status;
            }
        }
        catch (IOException e){
            System.out.print("Exception generated while committing transaction"+e.getMessage());
            status = false;
        }
        return status;
    }
}
