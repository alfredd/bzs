package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.CommitAbortedException;

interface TransactionInterface {

    BZStoreData read (String key);

    void commit () throws CommitAbortedException;
}
