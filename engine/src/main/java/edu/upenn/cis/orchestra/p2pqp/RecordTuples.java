package edu.upenn.cis.orchestra.p2pqp;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collection;

public interface RecordTuples {
	static class TupleAndOp implements Serializable {
		private static final long serialVersionUID = 1L;
		final QpTuple<?> t;
		final int operatorId;
		final int phaseNo;
		
		public TupleAndOp(QpTuple<?> t, int operatorId, int phaseNo) {
			this.t = t;
			this.operatorId = operatorId;
			this.phaseNo = phaseNo;
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			
			TupleAndOp tao = (TupleAndOp) o;
			return operatorId == tao.operatorId && phaseNo == tao.phaseNo && t.equals(tao.t);
		}
		
		public int hashCode() {
			return operatorId + 37 * t.hashCode() + 97 * phaseNo;
		}
		
		public String toString() {
			return "<" + t + "," + operatorId + "," + phaseNo + ">";
		}
	}
	
	static class NodeAndOp implements Serializable {
		private static final long serialVersionUID = 1L;
		public final InetSocketAddress node;
		public final int operatorId;
		
		public NodeAndOp(InetSocketAddress node, int operatorId) {
			this.node = node;
			this.operatorId = operatorId;
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			
			NodeAndOp nao = (NodeAndOp) o;
			return (nao.operatorId == operatorId && (node == null ? nao.node == null : nao.node.equals(node)));
		}
		
		public int hashCode() {
			return (node == null ? 0 : node.hashCode()) + 37 * operatorId;
		}
		
		public String toString() {
			return "<" + node + "," + operatorId + ">";
		}
	}
	
	public static class OperatorAndPhase {
		public final int operator;
		public final int phase;
		
		public OperatorAndPhase(int operator, int phase) {
			this.operator = operator;
			this.phase = phase;
		}
		
		public int hashCode() {
			return operator + 31 * phase;
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != OperatorAndPhase.class) {
				return false;
			}
			
			OperatorAndPhase oap = (OperatorAndPhase) o;
			return (operator == oap.operator && phase == oap.phase);
		}
	}

	void operatorHasFinished(int operatorId, int phaseNo);
	void keysScanned(int operatorId, IdRange range);
	void keysNotFound(int operatorId, Collection<QpTupleKey> keys, int phaseNo);
	void reportException(Exception e);
	void nodesHaveFailed(Collection<InetSocketAddress> node);
	void activityHasOccurred();
	
	public void close();
}
