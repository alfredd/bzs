package edu.ucsc.edgelab.db.bzs.clientlib;

interface TransactionInterface {

    String read (String key);

    void write (String key, String value);

    void commit ();
}
