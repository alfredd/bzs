package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class BFTCommitClient {
    public static void main(String args[]){
        int clientIds[] = {5, 6, 7};
        BFTClient[] clients = new BFTClient[clientIds.length];
        for(int i = 0; i < clientIds.length; i++){
            clients[i] = new BFTClient(clientIds[i]);
        }

        Bzs.Transaction transaction = Bzs.Transaction.newBuilder().build();
        Bzs.Write operation = Bzs.Write.newBuilder().setKey("test_key").setValue("test_value").build();
        System.out.println("Key "+ operation.getKey() + " Val "+ operation.getValue());
        transaction = transaction.toBuilder().addWriteOperations(operation).build();

        System.out.println("Performing write"+transaction.toString());
        List<Bzs.Transaction> list = new LinkedList<>();
        list.add(transaction);
        list.add(transaction);
        for(Bzs.Transaction t : list){
            for(Bzs.Write op : t.getWriteOperationsList()){
                System.out.println("Key ::"+ operation.getKey() + " Value: "+operation.getValue());
            }
        }
        System.out.println(clients[0].performCommit(list));
        System.out.println("Write Performed");

    }
}
