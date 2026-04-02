package edu.upenn.cis.orchestra.reconciliation;

public class ReconciliationEpoch {
	public final int recno;
	public final int epoch;
	
	public ReconciliationEpoch(int recno, int epoch) {
		this.recno = recno;
		this.epoch = epoch;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		ReconciliationEpoch re = (ReconciliationEpoch) o;
		return (recno == re.recno && epoch == re.epoch);
	}
	
	public int hashCode() {
		return recno + 37 * epoch;
	}
	
	public String toString() {
		return "(" + recno + "," + epoch + ")";
	}
}
