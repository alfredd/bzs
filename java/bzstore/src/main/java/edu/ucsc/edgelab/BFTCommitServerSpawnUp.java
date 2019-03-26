package edu.ucsc.edgelab;

import edu.ucsc.edgelab.db.bzs.bftcommit.BftBPServer;

import java.util.LinkedList;

public class BFTCommitServerSpawnUp {
    public static void main(String args[]){
//        int serverIds[] = {0, 1, 2, 3};
//
//        BFTServer[] servers =  new BFTServer[serverIds.length];
//
//        int i = 0;
//        while(i < serverIds.length){
//            try {
//                servers[i] = new BFTServer(serverIds[i], new BpTree());
//            }
//            catch (Exception e){
//                i++;
//            }
//        }
        //new BFTServer(Integer.parseInt(args[0]), new BpTree());
        LinkedList<Thread> threads = new LinkedList<>();
        for (int i = 0; i < 4; i++) {
            int finalI = i;
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    new BftBPServer(finalI);
                }
            }));
        }

        for (Thread t: threads)
            t.start();

        for (Thread t: threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
