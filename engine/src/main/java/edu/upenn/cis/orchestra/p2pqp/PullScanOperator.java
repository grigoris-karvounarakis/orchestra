package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;

abstract class PullScanOperator<M> extends Operator<M> {
	PullScanOperator(Operator<M> dest, WhichInput outputDest, int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, boolean enableRecovery) {
		super(dest, outputDest, nodeId, operatorId, mdf, schemas, rt, enableRecovery);
	}

	PullScanOperator(Operator<M> componentOf) {
		super(componentOf);
	}
	
	public abstract int scan(int blockSize, int phaseNo);
	public abstract int scanAll(int phaseNo);	
	public abstract boolean isFinished(int phaseNo);
	protected final void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		throw new UnsupportedOperationException("Cannot deliver tuples to a scan operator (" + this.getClass().getName() + ")");
	}
	
	protected final void inputHasFinished(WhichInput whichChild, int phaseNo) {
		throw new UnsupportedOperationException("Cannot deliver tuples to a scan operator (" + this.getClass().getName() + ")");
	}
	
	public abstract void interrupt();
	
	public abstract boolean rescanDuringRecovery();
}