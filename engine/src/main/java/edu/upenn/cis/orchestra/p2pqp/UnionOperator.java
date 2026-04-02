package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;

public class UnionOperator<M> extends Operator<M> {
	private final QpSchema leftSchema, rightSchema;
	private final QpSchema outputSchema;
	private final AbstractRelation.RelationMapping leftMapping, rightMapping;

	private final Set<Integer> leftFinishedPhases, rightFinishedPhases;


	public UnionOperator(QpApplication<M> app, QpSchema leftSchema, QpSchema rightSchema, QpSchema outputSchema, 
			Operator<M> dest, WhichInput outputDest, InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, boolean enableRecovery) throws ValueMismatchException {

		super(dest,outputDest,nodeId,operatorId,mdf,schemas,rt,enableRecovery);

		this.leftSchema = leftSchema;
		this.rightSchema = rightSchema;
		this.outputSchema = outputSchema;
		AbstractRelation.FieldSource[] fs = new AbstractRelation.FieldSource[outputSchema.getNumCols()];
		for (int i = 0; i < outputSchema.getNumCols(); ++i) {
			fs[i] = new AbstractRelation.FieldSource(i, true);
		}

		if (leftSchema.quickEquals(outputSchema)) {
			leftMapping = null;
		} else {
			leftMapping = new AbstractRelation.RelationMapping(leftSchema, outputSchema, null, fs);
		}
		if (rightSchema.quickEquals(outputSchema)) {
			rightMapping = null;
		} else {
			rightMapping = new AbstractRelation.RelationMapping(rightSchema, outputSchema, null, fs);
		}

		leftFinishedPhases = new HashSet<Integer>();
		rightFinishedPhases = new HashSet<Integer>();
	}


	@Override
	protected void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		if (tuples.isEmpty()) {
			return;
		}
		if (destInput == WhichInput.LEFT) {
			if (! tuples.schema.quickEquals(leftSchema)) {
				this.reportException(new RuntimeException("Received unexpected " + (destInput == null ? "unlabeled" : destInput.toString()) + tuples.schema.getName() + " tuples in union operator"));
				return;
			}
			if (leftMapping == null) {
				sendTuples(tuples);
			} else {
				QpTupleBag<M> mapped = tuples.applyMapping(leftMapping, outputSchema);
				sendTuples(mapped);
			}
		} else if (destInput == WhichInput.RIGHT) {
			if (! tuples.schema.quickEquals(rightSchema)) {
				this.reportException(new RuntimeException("Received unexpected " + (destInput == null ? "unlabeled" : destInput.toString()) + tuples.schema.getName() + " tuples in union operator"));				
			}
			if (rightMapping == null) {
				sendTuples(tuples);
			} else {
				QpTupleBag<M> mapped = tuples.applyMapping(rightMapping, outputSchema);
				sendTuples(mapped);
			}
		} else {
			this.reportException(new RuntimeException("Need WhichInput argument to union operator"));
		}
	}


	@Override
	protected synchronized void inputHasFinished(WhichInput whichChild, int phaseNo) {
		if (whichChild == WhichInput.LEFT) {
			if (leftFinishedPhases.add(phaseNo) && rightFinishedPhases.contains(phaseNo)) {
				this.finishedSending(phaseNo);
			}
		} else if (whichChild == WhichInput.RIGHT) {
			if (rightFinishedPhases.add(phaseNo) && leftFinishedPhases.contains(phaseNo)) {
				this.finishedSending(phaseNo);
			}
		} else {
			throw new IllegalArgumentException("whichChild must be LEFT or RIGHT");
		}
	}
}
