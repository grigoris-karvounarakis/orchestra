package edu.upenn.cis.orchestra.p2pqp.plan;

import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.UnionOperator;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class UnionNode<M> extends TwoInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;

	public UnionNode(QpSchema outputSchema, Location loc, QueryPlan<M> left, QueryPlan<M> right, int operatorId) {
		super(outputSchema.getRelationName(), loc, left, right, operatorId); 
	}
	
	@Override
	Operator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode)
	throws Operator.OperatorCreationException {
	
		QpSchema outputSchema = exec.getSchema(this.outputSchema);
		QpSchema leftSchema = exec.getSchema(left.outputSchema);
		QpSchema rightSchema = exec.getSchema(right.outputSchema);
		
		try {
			return new UnionOperator<M>(exec.app, leftSchema, rightSchema, 
					  outputSchema, parent, dest, exec.getNodeAddress(), operatorId, exec.getRecordTuples(), exec.app.getMetadataFactory(), exec, exec.recoveryEnabled());
		} catch (ValueMismatchException e) {
			throw new Operator.OperatorCreationException(this.operatorId, e);
		}
		
	}
	@Override
	protected boolean subclassEquals(TwoInputQueryPlan<?> jqp) {
		if (jqp instanceof UnionNode) {
			return true;
		} else {
			return false;
		}
	}
	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc, el, schemas);
	}

	
	public static <M> UnionNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlanData qpd = deserializeHelper(el);
		QueryPlan<M> leftInput = getLeftInput(el, schemas, alreadyIds, metadataClass);
		QueryPlan<M> rightInput = getRightInput(el, schemas, alreadyIds, metadataClass);
		
		UnionNode<M> ret = new UnionNode<M>(schemas.get(qpd.outputSchema), qpd.loc,
				 leftInput, rightInput, qpd.operatorId);
		return ret;
	}

	@Override
	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols,
			int[] hashCols, Source schemas) {
		throw new UnsupportedOperationException();
	}

}
