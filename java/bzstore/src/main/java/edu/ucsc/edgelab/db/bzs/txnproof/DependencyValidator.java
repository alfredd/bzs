package edu.ucsc.edgelab.db.bzs.txnproof;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.exceptions.ValidityException;

import java.util.LinkedList;
import java.util.List;

public class DependencyValidator {


    public int validate(List<Bzs.ReadResponse> readResponses) throws ValidityException {
        List<Integer> lceList = new LinkedList<>();
        int maxFaults = Configuration.getMaxAllowablefaults();
        List<Integer> validArray = new LinkedList<>();
        for (int i = 0; i < readResponses.size(); i++) {
            validArray.add(0);
        }
        int invalidCount =0;
        for (int i = 0; i < readResponses.size(); i++) {
            Bzs.ReadResponse readResponse = readResponses.get(i);
            lceList.add(readResponse.getLce());
            if (!validateProofs(readResponse.getProofList())) {
                validArray.set(i, -1);
                invalidCount++;
                continue;
            }
        }
        if (invalidCount>0) {
            throw new ValidityException("Failure during proof validation.", validArray);
        }

        return 0;
    }

    private boolean validateProofs(List<Bzs.ResponsePOW> proofList) {
        boolean isValid = false;
        boolean validCount = proofList.size() >= (Configuration.getMaxAllowablefaults() + 1);

        return isValid;
    }
}
