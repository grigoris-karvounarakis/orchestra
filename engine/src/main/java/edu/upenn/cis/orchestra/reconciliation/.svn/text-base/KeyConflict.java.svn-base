package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;



public class KeyConflict extends ConflictType {
	Tuple keyVal;
	
	public KeyConflict(Tuple keyVal) {
		this.keyVal = keyVal;
	}

	public ConflictTypeCode getTypeCode() {
		return ConflictTypeCode.KEY;
	}

	public boolean isApplicableFor(Update u) {
		return (keyVal.sameKey(u.getNewVal()));
	}

}
