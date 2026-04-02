package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;

public class ProjectOperator<M> extends Operator<M> {
	private final QpSchema newSchema;
	private final AbstractRelation.RelationMapping rm;

	public ProjectOperator(QpSchema oldSchema, QpSchema newSchema, int[] newToOldCol, Operator<M> dest, WhichInput destInput, InetSocketAddress nodeId, int operatorId, MetadataFactory<M> mdf, QpSchema.Source schemas, RecordTuples rt, boolean enableRecovery) throws ValueMismatchException {
		super(dest,destInput,nodeId,operatorId,mdf,schemas,rt,enableRecovery);
		this.newSchema = newSchema;
		if (newSchema == null) {
			throw new IllegalArgumentException("Need new schema");
		}
		if (newToOldCol == null) {
			throw new IllegalArgumentException("Need new schema mapping");
		}
		if (newToOldCol.length != newSchema.getNumCols()) {
			throw new IllegalArgumentException("Mismatach between inverseSchemaMapping and newSchema");
		}
		AbstractRelation.FieldSource fss[] = new AbstractRelation.FieldSource[newSchema.getNumCols()];
		for (int i = 0; i < fss.length; ++i) {
			fss[i] = new AbstractRelation.FieldSource(newToOldCol[i], true);
		}
		rm = new AbstractRelation.RelationMapping(oldSchema, newSchema, null, fss);
	}


	@Override
	protected void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		if (tuples.isEmpty()) {
			return;
		}
		QpTupleBag<M> projected = tuples.applyMapping(rm, newSchema);
		tuples.clear();
		sendTuples(projected);
	}

	@Override
	protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
		this.finishedSending(phaseNo);
	}

}