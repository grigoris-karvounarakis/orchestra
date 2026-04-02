package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class RecordTuplesTest implements RecordTuples {
	public final Set<QpTupleKey> notFound = new HashSet<QpTupleKey>();

	@Override
	public void close() {
	}

	@Override
	public void keysNotFound(int operatorId, Collection<QpTupleKey> keys, int phaseNo) {
		notFound.addAll(keys);
	}

	@Override
	public void keysScanned(int operatorId, IdRange range) {

	}

	@Override
	public void operatorHasFinished(int operatorId, int phase) {

	}

	@Override
	public void reportException(Exception e) {
		throw new RuntimeException("RecordTuplesTest received exception", e);
	}

	@Override
	public void nodesHaveFailed(Collection<InetSocketAddress> node) {
	}

	@Override
	public void activityHasOccurred() {
	}

}
