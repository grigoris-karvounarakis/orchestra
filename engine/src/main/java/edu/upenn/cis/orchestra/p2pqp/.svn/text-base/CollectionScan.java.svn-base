package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;

import edu.upenn.cis.orchestra.util.OutputBuffer;

public class CollectionScan<M> extends PullScanOperator<M> {
	private volatile boolean interrupted = false;

	private final QpSchema schema;
	private final Iterator<QpTuple<M>> i;
	private final byte[] contributingNodes;
	
	CollectionScan(QpSchema schema, QpSchema.Source findSchema, Collection<QpTuple<M>> tuples, Operator<M> dest, WhichInput outputDest, int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf, boolean enableRecovery) {
		super(dest, outputDest, queryId, nodeId, operatorId, rt, mdf, findSchema, enableRecovery);
		i = tuples.iterator();
		this.schema = schema;
		if (enableRecovery) {
			contributingNodes = OutputBuffer.getBytes(nodeId);
		} else {
			contributingNodes = null;
		}
	}

	public boolean isFinished(int phaseNo) {
		return (interrupted || (! i.hasNext()));
	}

	@Override
	public int scan(int blockSize, int phaseNo) {	
		return scan(blockSize,phaseNo,false);
	}

	@Override
	public int scanAll(int phaseNo) {
		return scan(Integer.MAX_VALUE,phaseNo,true);
	}

	private int scan(int blockSize, int phaseNo, boolean all) {
		QpTupleBag<M> block = new QpTupleBag<M>(schema, schemas, mdf);
		int currBlockSize = 0;
		int numScanned = 0;
		while ((! interrupted) && i.hasNext()) {
			QpTuple<M> t = i.next();
			byte[] metadataBytes;
			if (mdf == null) {
				metadataBytes = null;
			} else {
				M metadata = mdf.scan(this, t, t.getMetadata(schemas, mdf));
				metadataBytes = mdf.toBytes(metadata);
			}
			block.addWhileChanging(t, metadataBytes, contributingNodes, phaseNo);
			++currBlockSize;
			++numScanned;
			if (currBlockSize == blockSize) {
				if (all) {
					sendTuples(block);
					block.clear();
					currBlockSize = 0;
				} else {
					break;
				}
			}
		}
		if (! block.isEmpty()) {
			sendTuples(block);
		}
		if (! i.hasNext()) {
			this.finishedSending(phaseNo);
		}
		return numScanned;
	}

	@Override
	public void interrupt() {
		interrupted = true;
	}

	@Override
	public boolean rescanDuringRecovery() {
		return false;
	}

}
