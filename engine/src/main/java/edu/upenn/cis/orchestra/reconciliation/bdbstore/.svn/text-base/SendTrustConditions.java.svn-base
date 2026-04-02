package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;

import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;

class SendTrustConditions implements Serializable {
	private static final long serialVersionUID = 1L;
	private final byte[] tcBytes;
	transient SchemaIDBinding sch;
	final Schema s;
	
	SendTrustConditions(TrustConditions tc, SchemaIDBinding sch, Schema s) {
		tcBytes = tc.getBytes(sch);
		this.sch = sch;
		this.s = s;
	}
	
	TrustConditions getTrustConditions(SchemaIDBinding sch) {
		this.sch = sch;
		return new TrustConditions(tcBytes,sch);
	}	
}
