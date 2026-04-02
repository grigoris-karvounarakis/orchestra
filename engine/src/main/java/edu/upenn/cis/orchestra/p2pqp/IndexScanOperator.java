package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation.FieldSource;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.p2pqp.Filter.FilterException;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class IndexScanOperator<M> extends PullScanOperator<M> {
	private final Predicate filter;
	private int numPagesRemaining = -1;
	private boolean done = false;
	private final QpApplication<M> app;
	private final int relationId;
	private final int epoch;
	private final byte[] contributingNodes;
	private final QpSchema scanSchema;
	private int numTuplesScanned = 0;
	private final QpSchema outputSchema;
	private final AbstractRelation.RelationMapping outputSchemaMapping;

	public IndexScanOperator(QpApplication<M> app, Predicate filter, int relationId, int epoch, Operator<M> dest, WhichInput destInput, int queryId,
			InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, boolean enableRecovery) throws ValueMismatchException {
		this(app,filter,relationId,epoch,null,null,dest,destInput,queryId,nodeId,operatorId,rt,mdf,schemas,enableRecovery);
	}

	public IndexScanOperator(QpApplication<M> app, Predicate filter, int relationId, int epoch, QpSchema outputSchema, int[] outputToRelation, Operator<M> dest, WhichInput destInput, int queryId,
			InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, boolean enableRecovery) throws ValueMismatchException {
		super(dest, destInput, queryId, nodeId, operatorId, rt, mdf, schemas, enableRecovery);

		this.filter = filter;
		this.app = app;
		this.relationId = relationId;
		this.epoch = epoch;
		if (enableRecovery) {
			contributingNodes = OutputBuffer.getBytes(this.nodeId);
		} else {
			contributingNodes = null;
		}
		scanSchema = schemas.getSchema(relationId);

		if (outputSchema == null || outputSchema.relId == relationId) {
			this.outputSchema = scanSchema;
			this.outputSchemaMapping = null;
		} else {
			this.outputSchema = outputSchema;
			FieldSource fss[] = new FieldSource[outputSchema.getNumCols()];
			if (outputToRelation == null || fss.length != outputToRelation.length) {
				throw new IllegalArgumentException("When changing schema, must provde outputToRelation mapping with same number of columns as output schema");
			}
			for (int i = 0; i < fss.length; ++i) {
				fss[i] = new FieldSource(outputToRelation[i], true);
			}
			outputSchemaMapping = new AbstractRelation.RelationMapping(schemas.getSchema(relationId),outputSchema,null,fss);
		}
	}


	@Override
	public synchronized boolean isFinished(int phaseNo) {
		return done;
	}

	@Override
	public int scan(int blockSize, int phaseNo) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int scanAll(final int phaseNo) {
		app.dht.getTuplesInRelation(relationId, epoch, new DHTService.KeyTupleSink() {
			public void deliverTuples(QpTupleKey[] contents) {
				boolean finished = false;
				synchronized (IndexScanOperator.this) {
					if (numPagesRemaining < 0) {
						reportException(new RuntimeException("NumPages < 0, totalNumTuplesIs is not being called"));
						return;
					}
					--numPagesRemaining;
					if (numPagesRemaining < 0) {
						reportException(new RuntimeException("NumPages < 0, pages are being delivered multiple times"));
						return;
					}
					if (numPagesRemaining == 0) {
						finished = true;
					}
				}

				QpTupleBag<M> toSend = new QpTupleBag<M>(outputSchema, schemas, mdf);


				int count = 0;
				int pos = 0;
				try {
					if (outputSchemaMapping == null) {
						while (pos < contents.length && contents[pos] != null) {
							QpTupleKey t = contents[pos++];
							if (filter == null || filter.eval(t)) {
								++count;
								QpTuple<M> newT = t.getNonKeyTuple(phaseNo, contributingNodes, null); 
								toSend.add(newT);
							}
						}
					} else {
						while (pos < contents.length && contents[pos] != null) {
							QpTupleKey t = contents[pos++];
							if (filter == null || filter.eval(t)) {
								++count;
								QpTuple<M> newT = t.getNonKeyTuple(outputSchema, outputSchemaMapping, phaseNo, contributingNodes, null); 
								toSend.add(newT);
							}
						}
					}
				} catch (FilterException fe) {
					reportException(fe);
					return;
				}
				sendTuples(toSend);

				synchronized (IndexScanOperator.this) {
					numTuplesScanned += count;
				}

				if (finished) {
					synchronized (IndexScanOperator.this) {
						done = finished;
						IndexScanOperator.this.notifyAll();
					}
					finishedSending(phaseNo);
				}
			}

			public void processException(Exception e) {
				IndexScanOperator.this.reportException(e);
			}

			public void totalNumTuplesIs(int numTuples, int numPages) {
				synchronized (IndexScanOperator.this) {
					IndexScanOperator.this.numPagesRemaining = numPages;
				}
			}
		});
		try {
			synchronized (this) {
				while (! done) {
					wait();
				}
			}
		} catch (InterruptedException ie) {
			reportException(ie);
		}
		return numTuplesScanned;
	}

	@Override
	public synchronized void interrupt() {
		done = true;
		notifyAll();
	}


	@Override
	public boolean rescanDuringRecovery() {
		return false;
	}

}
