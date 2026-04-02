package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public abstract class Operator<M> {
	public static class OperatorCreationException extends Exception {
		private static final long serialVersionUID = 1L;

		public OperatorCreationException(int operatorId, String what, Throwable why) {
			super("Error creating operator #" + operatorId + ": " + what,why);
		}
		public OperatorCreationException(int operatorId, Throwable why) {
			super("Error creating operator #" + operatorId, why);
		}
	}

	public enum WhichInput {
		LEFT,
		RIGHT
	}


	final int operatorId;
	protected final boolean enableRecovery;
	private final RecordTuples rt;
	private final Operator<M> dest;
	private final WhichInput destInput;
	final InetSocketAddress nodeId;

	protected WhichInput getDestInput() {
		return destInput;
	}

	public Operator<M> getDest() {
		return dest;
	}

	protected final MetadataFactory<M> mdf;
	protected final QpSchema.Source schemas;

	protected Operator(InetSocketAddress nodeId, int operatorId, MetadataFactory<M> mdf, QpSchema.Source schemas, RecordTuples rt, boolean enableRecovery) {
		this(null, null, nodeId, operatorId, mdf, schemas, rt, enableRecovery);
	}

	protected Operator(Operator<M> dest, WhichInput destInput, InetSocketAddress nodeId,
			int operatorId, MetadataFactory<M> mdf, QpSchema.Source schemas, RecordTuples rt, boolean enableRecovery) {
		this.rt = rt;
		this.operatorId = operatorId;
		this.dest = dest;
		this.destInput = destInput;
		if (mdf == null || mdf instanceof NullMetadataFactory) {
			this.mdf = null;
		} else {
			this.mdf = mdf;
		}
		this.enableRecovery = enableRecovery;
		this.nodeId = nodeId;
		this.schemas = schemas;
	}

	protected Operator(Operator<M> componentOf) {
		this.dest = componentOf.dest;
		this.destInput = componentOf.destInput;
		this.rt = componentOf.rt;
		this.operatorId = componentOf.operatorId;
		this.mdf = componentOf.mdf;
		this.enableRecovery = componentOf.enableRecovery;
		this.nodeId = componentOf.nodeId;
		this.schemas = componentOf.schemas;
	}

	protected final void keysScanned(Iterable<IdRange> ranges, int phaseNo) {
		for (IdRange range : ranges) {
			rt.keysScanned(operatorId, range);
		}
	}
	
	protected final void keysScanned(IdRange range, int phaseNo) {
		rt.keysScanned(operatorId, range);
	}

	/**
	 * Deliver tuples to an operator.
	 * 
	 * @param dest				Which input to deliver the tuples to
	 * @param tuples			The tuples to deliver. The tuples list will not be modified by the caller
	 * 							until after this method returns, but it may be modified after that time.
	 * 							The callee may modify the array (i.e. to clear tuples that have been processed)
	 */
	protected abstract void receiveTuples(WhichInput dest, QpTupleBag<M> tuples);

	/**
	 * Inform an operator that it will receive no more tuples from the specified input
	 * during the specified phase or any earlier phases
	 * 
	 * @param whichChild		The child operator that is doing the notification. One-input
	 * 							operators may ignore this argument, which may be <code>null</code>
	 * @param phaseNo			The relevant phase no
	 */
	protected abstract void inputHasFinished(WhichInput whichChild, int phaseNo);

	protected void close() {

	}

	protected void sendTuples(QpTupleBag<M> tuples) {
		dest.receiveTuples(destInput, tuples);
	}

	final void finishedSending(int phaseNo) {
		if (phaseNo < 0) {
			throw new IllegalArgumentException("Phase number must be positive: " + phaseNo);
		}
		if (dest != null) {
			// Check to make sure that this works for ship operators and
			// spool operators, since they don't send directly
			// to another operator
			dest.inputHasFinished(destInput, phaseNo);
		}
		rt.operatorHasFinished(operatorId, phaseNo);
	}

	protected void reportException(Exception e) {
		if (rt == null) {
			throw new RuntimeException("Cannot report exception without RecordTuples");
		}
		rt.reportException(e);
	}

	protected void reportMissing(List<QpTupleKey> keys, int phaseNo) {
		rt.keysNotFound(operatorId, keys, phaseNo);
	}

	/**
	 * Inform this operator that a new query phase has begun
	 * 
	 * @param newPhaseNo			The number of the new phase. The previous phase was implicitly <code>newPhaseNo-1</code>
	 * @param newlyFailedNodes		The nodes that failed during the previous phase
	 * @param failedRanges			The ranges that failed during the previous phase
	 * @throws InterruptedException 
	 */
	protected void beginNewPhase(int newPhaseNo, InetSocketAddress[] newlyFailedNodes, IdRangeSet failedRanges) throws InterruptedException {
	}

	protected void purgeState(boolean destructive) {
	}

	protected void reportNodeFailure(InetSocketAddress node) {
		rt.nodesHaveFailed(Collections.singleton(node));
	}

	protected void activityHasOccurred() {
		rt.activityHasOccurred();
	}
}
