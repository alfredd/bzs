package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.exceptions.CommitAbortedException;

interface TransactionInterface {

    String read (String key);

    void commit () throws CommitAbortedException;
}
