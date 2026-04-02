package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;

class GetDecisions implements Serializable {
	private static final long serialVersionUID = 1L;
	public final int recno;
	
	GetDecisions(int recno) {
		this.recno = recno;
	}
}
