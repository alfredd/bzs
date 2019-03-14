package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;

import java.io.IOException;

public class ClientDemo {

    public static void main(String[] args) throws IOException {
        ServerInfo leader = Configuration.getLeaderInfo();



//        Thread f1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//
//            }
//        });

        Transaction t1 = new Transaction(leader.host,leader.port);
        t1.write("x","10");
        t1.write("y","1");
        t1.write("z","3");
        t1.commit();

        Transaction t2 = new Transaction(leader.host,leader.port);
        String vx = t2.read("x");
        t2.write("x",vx+45);
        t2.write("z","5");
        t2.commit();

        BZStoreProperties properties = new BZStoreProperties();
        Integer port1 = Integer.decode(properties.getProperty("1", BZStoreProperties.Configuration.port));
        String host1 = properties.getProperty("1", BZStoreProperties.Configuration.host);
        Transaction t3 = new Transaction(host1,port1);
        t3.read("x");
        t3.read("y");
        t3.read("z");

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
