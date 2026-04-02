package edu.upenn.cis.orchestra.p2pqp;

public class UpdateProvenance { 
	
	public enum UpdateType {INS, DEL, REPLACE};
	
	public TupleProvenance tuplePv;
	
	public UpdateType updateType;
	
	public boolean propagate = true;
	
	//public QpTuple<UpdateProvenance> replaceTuple;
	public TupleProvenance replacePv;
	
	public UpdateProvenance() {
		this.tuplePv = null;
		this.updateType = UpdateType.INS;
		this.replacePv = null;
	}
	public UpdateProvenance(UpdateType updateType, TupleProvenance replacePv) {
		this.tuplePv = null;
		this.updateType = updateType;
		//this.replaceTuple = replaceTuple;
		this.replacePv = replacePv;
	}
	public UpdateProvenance(TupleProvenance tuplePv, UpdateType updateType, TupleProvenance replacePv) {
		this.tuplePv = tuplePv;
		this.updateType = updateType;
		//this.replaceTuple = replaceTuple;
		this.replacePv = replacePv;
	}
	
	public UpdateType getUpdateType() {
		return updateType;
	}
	
	public String toString() {
		return ((updateType == UpdateType.INS) ? "+" : "-") + tuplePv.toString();
	}
	
	public boolean equals(Object t) {
		UpdateProvenance that = (UpdateProvenance) t;
		
		if (this.updateType != that.updateType) {
			return false;
		}
		if (this.tuplePv == null && that.tuplePv != null) {
			return false;
		}
		if (this.tuplePv != null && !this.tuplePv.equalProvenance(that.tuplePv)) {
			return false;
		}
		if (this.replacePv == null && that.replacePv != null) {
			return false;
		}
		if (this.replacePv != null && !this.replacePv.equalProvenance(that.replacePv)) {
			return false;
		}
		return true;
	}
}