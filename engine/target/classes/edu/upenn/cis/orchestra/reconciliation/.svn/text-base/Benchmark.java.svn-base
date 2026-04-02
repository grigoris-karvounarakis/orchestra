package edu.upenn.cis.orchestra.reconciliation;

/**
 * Class to hold benchmark data
 * 
 * 
 * @author Nick
 *
 */
public class Benchmark {
	// All are in nanoseconds
	public long publish;
	public long publishNet;
	public long publishServer;
	public long recordTxnDecisions;
	public long recordTxnDecisionsNet;
	public long recordTxnDecisionsServer;
	public long recordReconcile;
	public long recordReconcileNet;
	public long recordReconcileServer;
	public long getReconciliationData;
	public long getReconciliationDataNet;
	public long getReconciliationDataServer;
	public long getCurrentRecno;
	public long getCurrentRecnoNet;
	public long getCurrentRecnoServer;
	public long resolveConflicts;
	public long reconcile;
	public long getTxnStatusNet;
	public long getTxnStatusServer;
	
	
	
	public Benchmark() {
		publish = 0;
		publishNet = 0;
		publishServer = 0;
		recordTxnDecisions = 0;
		recordTxnDecisionsNet = 0;
		recordTxnDecisionsServer = 0;
		recordReconcile = 0;
		recordReconcileNet = 0;
		recordReconcileServer = 0;
		getReconciliationData = 0;
		getReconciliationDataNet = 0;
		getReconciliationDataServer = 0;
		getCurrentRecno = 0;
		getCurrentRecnoNet = 0;
		getCurrentRecnoServer = 0;
		resolveConflicts = 0;
		reconcile = 0;
		getTxnStatusNet = 0;
		getTxnStatusServer = 0;
	}
	
	public static String getHeaders() {
		return "publish\tpublishNet\tpublishServer\trecordTxnDecisions\trecordTxnDecisionsNet" +
		"\trecordTxnDecisionsServer\trecordReconcile\trecordReconcileNet\trecordReconcileServer\t" +
		"getReconciliationData\tgetReconciliationDataNet\tgetReconciliationDataServer\t" +
		"getCurrentRecno\tgetCurrentRecnoNet\tgetCurrentRecnoServer\tresolveConflicts\t" +
		"reconcile\tgetTxnStatusNet\tgetTxnStatusServer";
	}
	
	public String toString() {
		return publish + "\t" + publishNet + "\t" + publishServer + "\t" +
		recordTxnDecisions + "\t" + recordTxnDecisionsNet + "\t" + recordTxnDecisionsServer + "\t" +
		recordReconcile + "\t" + recordReconcileNet + "\t" + recordReconcileServer + "\t" +
		getReconciliationData + "\t" + getReconciliationDataNet + "\t" + getReconciliationDataServer + "\t" +
		getCurrentRecno + "\t" + getCurrentRecnoNet + "\t" + getCurrentRecnoServer + "\t" +
		resolveConflicts + "\t" + reconcile + "\t" + getTxnStatusNet + "\t" + getTxnStatusServer;
	}
}