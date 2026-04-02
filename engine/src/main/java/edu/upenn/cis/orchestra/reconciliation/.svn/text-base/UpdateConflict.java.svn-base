package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;



public class UpdateConflict extends ConflictType {
	Tuple oldVal;
	
	public UpdateConflict(Tuple oldVal) {
		this.oldVal = oldVal.duplicate();
	}

	public ConflictTypeCode getTypeCode() {
		return ConflictTypeCode.UPDATE;
	}

	public boolean isApplicableFor(Update u) {
		return (oldVal.equals(u.getOldVal()));
	}

}
