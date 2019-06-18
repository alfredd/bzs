package edu.ucsc.edgelab.db.bzs.replica;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SMRData {
    int epoch;
    Set<TransactionID> lRWTs = new LinkedHashSet<>();
    Set<TransactionID> preparedDRWTs = new LinkedHashSet<>();
    Set<TransactionID> committedDRWTs = new LinkedHashSet<>();
    int lastCommittedEpoch;
    List<Integer> dependencyVector;
}
