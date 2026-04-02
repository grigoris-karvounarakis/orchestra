package edu.upenn.cis.orchestra.p2pqp.plan;

import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.SpoolOperator;
import edu.upenn.cis.orchestra.p2pqp.Operator.OperatorCreationException;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class SpoolNode<M> extends OneInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;

	public SpoolNode(QpSchema inputSchema, int operatorId, QueryPlan<M> input) {
		super(inputSchema.getRelationName(), null, CentralizedLoc.getInstance(), operatorId, input);
	}

	protected boolean subclassEquals(OneInputQueryPlan<?> qp) {
		return (qp instanceof SpoolNode);
	}

	SpoolOperator<M> createOperator(QueryExecution<M> exec, Operator<M> parent,
			WhichInput dest, QueryPlan<M> parentNode) throws OperatorCreationException {
		QpSchema schema = exec.getSchema(input.outputSchema);
		return SpoolOperator.create(schema, exec, exec.getNodeAddress(), operatorId, exec.getRecordTuples(), exec.app.getMetadataFactory(), exec.recoveryEnabled(), exec.discardResults());
	}

	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc,el,schemas);
	}
	
	public static <M> SpoolNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlanData qpd = deserializeHelper(el);
		QueryPlan<M> input = getInputQueryPlan(el, schemas, alreadyIds, metadataClass);
		return new SpoolNode<M>(schemas.get(input.outputSchema), qpd.operatorId, input);
	}

}
