package edu.ucsc.edgelab.db.bzs.txnproof;

import edu.ucsc.edgelab.db.bzs.Bzs;

public class ProofValidator {

    boolean isValid(Bzs.TransactionResponse response) {
        boolean isValid = false;

        if (response.getStatus().equals(Bzs.TransactionStatus.COMMITTED)) {

        }
        return isValid;
    }
}
