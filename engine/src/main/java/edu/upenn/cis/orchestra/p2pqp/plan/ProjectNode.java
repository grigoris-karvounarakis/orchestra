package edu.upenn.cis.orchestra.p2pqp.plan;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.ProjectOperator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class ProjectNode<M> extends OneInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;
	private final int[] outputToInputCol;

	public ProjectNode(QpSchema inputSchema, int[] outputToInputCol,
			QpSchema outputSchema, Location loc, int operatorId,
			QueryPlan<M> input) {
		super(inputSchema.getRelationName(), outputSchema.getRelationName(), loc, operatorId, input);
		this.outputToInputCol = new int[outputToInputCol.length];
		System.arraycopy(outputToInputCol, 0, this.outputToInputCol, 0, outputToInputCol.length);
		if (outputToInputCol.length != outputSchema.getNumCols()) {
			throw new IllegalArgumentException("Output schema has " + outputSchema.getNumCols() + " columns but mapping has " + outputToInputCol.length + " columns");
		}
	}

	@Override
	ProjectOperator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode) throws Operator.OperatorCreationException  {
		try {
			return new ProjectOperator<M>(exec.getSchema(inputSchema), exec.getSchema(outputSchema), outputToInputCol, parent, dest, exec.getNodeAddress(), operatorId, exec.app.getMetadataFactory(), exec, exec.getRecordTuples(), exec.recoveryEnabled());
		} catch (ValueMismatchException e) {
			throw new Operator.OperatorCreationException(this.operatorId, e);
		}
	}

	@Override
	protected boolean subclassEquals(OneInputQueryPlan<?> qp) {
		if (qp instanceof ProjectNode) {
			return true;
		}	else {
			return false;
		}
	}

	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc,el,schemas);
		Element mapping = DomUtils.addChild(doc, el, "mapping");
		for (int i = 0; i < outputToInputCol.length; ++i) {
			Element entry = DomUtils.addChild(doc, mapping, "entry");
			entry.setAttribute("outputPos", Integer.toString(i));
			entry.setAttribute("inputPos", Integer.toString(outputToInputCol[i]));
		}
	}

	public static <M> ProjectNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlanData qpd = deserializeHelper(el);
		QueryPlan<M> input = getInputQueryPlan(el, schemas, alreadyIds, metadataClass);
		QpSchema inputSchema = schemas.get(input.outputSchema);
		QpSchema outputSchema = schemas.get(qpd.outputSchema);
		Element mapping = DomUtils.getChildElementByName(el, "mapping");
		int[] outputToInputCol = new int[outputSchema.getNumCols()];
		boolean[] found = new boolean[outputSchema.getNumCols()];
		List<Element> entries = DomUtils.getChildElementsByName(mapping, "entry");
		for (Element entry : entries) {
			String inputPos = entry.getAttribute("inputPos");
			String outputPos = entry.getAttribute("outputPos");
			if (inputPos.length() == 0 || outputPos.length() == 0) {
				throw new XMLParseException("Each schema mapping entry must have an inputPos and an outputPos", entry);
			}
			try {
				int inputPosInt = Integer.parseInt(inputPos);
				int outputPosInt = Integer.parseInt(outputPos);
				if (found[outputPosInt]) {
					throw new XMLParseException("Found multiple mappings for output column " + outputPosInt, el);
				}
				found[outputPosInt] = true;
				outputToInputCol[outputPosInt] = inputPosInt;
			} catch (NumberFormatException nfe) {
				throw new XMLParseException(nfe, entry);
			}
		}
		for (int i = 0; i < found.length; ++i) {
			if (! found[i]) {
				throw new XMLParseException("Found no mapping for output column " + i, el);
			}
		}
		return new ProjectNode<M>(inputSchema, outputToInputCol, outputSchema, qpd.loc, qpd.operatorId, input);
	}

	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, int[] hashCols, Source schemas) {
		if (hashCols != null) {
			int[] newHashCols = new int[hashCols.length];
			int pos = 0;
			for (Integer hashCol : hashCols) {
				int inputCol = outputToInputCol[hashCol];
				newHashCols[pos++] = inputCol;
			}
			hashCols = newHashCols;
		}
		return input.getRecoveryResendOperators(opsHashCols, hashCols, schemas);
	}
}