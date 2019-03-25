package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.exceptions.CommitAbortedException;

import java.io.IOException;

public class ClientDemo {

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerInfo leader = Configuration.getLeaderInfo();



//        Thread f1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//
//            }
//        });

        BZStoreClient client = new BZStoreClient(leader.host,leader.port);
/*
        Transaction t1 = new Transaction();
        t1.setClient(client);
        t1.write("x","10");
        t1.write("y","1");
        t1.write("z","3");
        try {
            t1.commit();
        } catch (CommitAbortedException e) {
            e.printStackTrace();
        }

        Transaction t2 = new Transaction();
        t2.setClient(client);
        String vx = t2.read("x");
        t2.read("y");
        t2.read("z");
        t2.write("x",vx+45);
        t2.write("z","5");
        try {
            t2.commit();
        } catch (CommitAbortedException e) {
            e.printStackTrace();
        }*/

        BZStoreProperties properties = new BZStoreProperties();
        Integer port1 = Integer.decode(properties.getProperty("1", BZStoreProperties.Configuration.port));
        String host1 = properties.getProperty("1", BZStoreProperties.Configuration.host);
        Transaction t3 = new Transaction();
        t3.setClient(client);
        t3.read("twinkle");
        t3.read("honour");
        t3.read("earth");
        client.shutdown();
//        t3.write("x",vx+45);
//        t3.write("z","5");
//        t3.commit();

//        Thread f2 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//
//            }
//        });

    }
}
