package edu.upenn.cis.orchestra.p2pqp.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnInput;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnOrFunction;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class FunctionNode<M> extends OneInputQueryPlan<M> {
	public FunctionNode(QpSchema inputSchema, QpSchema outputSchema,
			List<? extends ColumnOrFunction> output,
			Location loc, int operatorId, QueryPlan<M> input) {
		super(inputSchema.getRelationName(), outputSchema.getRelationName(),
				loc, operatorId, input);
		this.output = new ArrayList<ColumnOrFunction>(output);
	}

	private static final long serialVersionUID = 1L;
	private final List<ColumnOrFunction> output;
	
	@Override
	FunctionEvaluator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode) throws Operator.OperatorCreationException { 
		for (ColumnOrFunction cof : output) {
			cof.loadSchemas(exec);
		}
		try {
			return new FunctionEvaluator<M>(exec.getSchema(inputSchema), exec.getSchema(outputSchema),
					output, parent, dest, exec.getNodeAddress(), operatorId, exec.app.getMetadataFactory(), exec, exec.getRecordTuples(), exec.recoveryEnabled());
		} catch (ValueMismatchException e) {
			throw new Operator.OperatorCreationException(operatorId, e);
		}
	}

	@Override
	protected boolean subclassEquals(OneInputQueryPlan<?> qp) {
		if (qp instanceof FunctionNode) {
			FunctionNode<?> fn = (FunctionNode<?>) qp;
			return output.equals(fn.output);
		} else {
			return false;
		}
	}

	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc,el,schemas);
		Element output = DomUtils.addChild(doc, el, "output");
		for (ColumnOrFunction cof : this.output) {
			output.appendChild(cof.serialize(doc, schemas));
		}
	}

	public static <M> FunctionNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlan<M> input = getInputQueryPlan(el, schemas, alreadyIds, metadataClass);
		QueryPlanData qpd = deserializeHelper(el);

		List<ColumnOrFunction> output = new ArrayList<ColumnOrFunction>();
		
		Element outputEl = DomUtils.getChildElementByName(el, "output");
		if (outputEl == null) {
			throw new XMLParseException("Missing output element", el);
		}
		for (Element colEl : DomUtils.getChildElements(outputEl)) {
			output.add(ColumnOrFunction.deserialize(colEl, schemas));
		}
		
		QpSchema inputSchema = schemas.get(input.outputSchema);
		if (inputSchema == null) {
			throw new IllegalArgumentException("Could not load schema " + input.outputSchema);
		}
		
		QpSchema outputSchema = schemas.get(qpd.outputSchema);
		if (outputSchema == null) {
			throw new IllegalArgumentException("Could not load schema " + qpd.outputSchema);
		}
		
		return new FunctionNode<M>(inputSchema, outputSchema,
				output, qpd.loc, qpd.operatorId, input);
	}
	
	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, int[] hashCols, Source schemas) {
		if (hashCols != null) {
			int[] newHashCols = new int[hashCols.length];
			int pos = 0;
			for (Integer hashCol : hashCols) {
				ColumnOrFunction cof = output.get(hashCol);
				if (cof instanceof ColumnInput) {
					newHashCols[pos++] = ((ColumnInput) cof).column;
				} else {
					throw new IllegalStateException("Can't recover when hashing on output of function");
				}
			}
			hashCols = newHashCols;
		}
		return input.getRecoveryResendOperators(opsHashCols, hashCols, schemas);
	}

}
