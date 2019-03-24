package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;

import java.nio.charset.Charset;
import java.util.*;

public class BenchmarkExecutor implements Runnable {

    private Set<String> keys = new LinkedHashSet<>();

    private final TransactionProcessor transactionProcessor;

    public BenchmarkExecutor(TransactionProcessor transactionProcessor) {
        this.transactionProcessor = transactionProcessor;
    }

    public Bzs.Transaction generateWriteSet() {
        Bzs.Transaction t = null;

        return t;
    }

    @Override
    public void run() {

    }

    public static void main(String args[]) {
        int clientIds[] = {5, 6, 7};
        BFTClient[] clients = new BFTClient[clientIds.length];
        for (int i = 0; i < clientIds.length; i++) {
            clients[i] = new BFTClient(clientIds[i]);
        }


        for (int j = 1; j <= 10000; j = j * 10) {
            System.out.println("Total Transactions: " + j);
            long startTime = System.currentTimeMillis();
            for (int i = 1; i < j; i++) {
                String[] test = new String[5];
                for (int z = 0; z < 5; z++) {
                    byte[] array = new byte[7]; // length is bounded by 7
                    new Random().nextBytes(array);
                    String generatedString = new String(array, Charset.forName("UTF-8"));
                    test[z] = generatedString;
                }
                List<Bzs.Transaction> list = new LinkedList<>();
                for (int k = 0; k < randInt(5, 10); k++) {
                    Bzs.Transaction transaction = Bzs.Transaction.newBuilder().build();
                    Bzs.Write operation = Bzs.Write.newBuilder().setKey(test[randInt(0, 4)]).setValue(test[randInt(0,
                            4)]).build();
                    //System.out.println("Key " + operation.getKey() + " Val " + operation.getValue());
                    transaction = transaction.toBuilder().addWriteOperations(operation).build();
                    //System.out.println("Performing write" + transaction.toString());
                    list.add(transaction);
                }
                clients[0].performCommit(list);
                //             System.out.println("Write Performed");
            }
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);
        }

//        Bzs.ROTransaction transaction = Bzs.ROTransaction.newBuilder().build();
//        Bzs.Read operation = Bzs.Read.newBuilder().setKey("test_key").build();
//        System.out.println("Key "+ operation.getKey());
//        transaction = transaction.toBuilder().addReadOperations(operation).build();
//
//        System.out.println("Performing write"+transaction.toString());
//        List<Bzs.ROTransaction> list = new LinkedList<>();
//        list.add(transaction);
//        list.add(transaction);
//        for(Bzs.ROTransaction t : list){
//            for(Bzs.Read op : t.getReadOperationsList()){
//                System.out.println("Key ::"+ operation.getKey());
//            }
//        }
//        System.out.println(clients[0].performRead(list));
//        System.out.println("Read Performed");


    }

    public static int randInt(int min, int max) {

        // NOTE: This will (intentionally) not run as written so that folks
        // copy-pasting have to think about how to initialize their
        // Random instance.  Initialization of the Random instance is outside
        // the main scope of the question, but some decent options are to have
        // a field that is initialized once and then re-used as needed or to
        // use ThreadLocalRandom (if using at least Java 1.7).
        //
        // In particular, do NOT do 'Random rand = new Random()' here or you
        // will get not very good / not very random results.
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }


}
