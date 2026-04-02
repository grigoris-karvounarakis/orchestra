package edu.upenn.cis.orchestra.p2pqp.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.PipelinedHashJoin;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class PipelinedJoinNode<M> extends TwoInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;

	final List<Integer> leftJoinIndices, rightJoinIndices, leftOutputPos, rightOutputPos;
	final int numBuckets;
	
	public PipelinedJoinNode(QpSchema outputSchema, List<Integer> leftJoinIndices,
			List<Integer> rightJoinIndices, List<Integer> leftOutputPos,
			List<Integer> rightOutputPos, Location loc,
			QueryPlan<M> left, QueryPlan<M> right, int operatorId) {
		this(outputSchema, leftJoinIndices, rightJoinIndices, leftOutputPos, rightOutputPos, loc, -1, left, right, operatorId);
	}
	
	public PipelinedJoinNode(QpSchema outputSchema, List<Integer> leftJoinIndices,
			List<Integer> rightJoinIndices, List<Integer> leftOutputPos,
			List<Integer> rightOutputPos, Location loc, int numBuckets,
			QueryPlan<M> left, QueryPlan<M> right, int operatorId) {
		super(outputSchema.getRelationName(), loc, left, right, operatorId);

		this.leftJoinIndices = new ArrayList<Integer>(leftJoinIndices);
		this.rightJoinIndices = new ArrayList<Integer>(rightJoinIndices);
		this.leftOutputPos = new ArrayList<Integer>(leftOutputPos);
		this.rightOutputPos = new ArrayList<Integer>(rightOutputPos);

		this.numBuckets = numBuckets;
	}

	@Override
	Operator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode)
	throws Operator.OperatorCreationException {

		QpSchema outputSchema = exec.getSchema(this.outputSchema);

		try {
			if (numBuckets >= 0) {
				return new PipelinedHashJoin<M>(exec.getSchema(left.outputSchema), exec.getSchema(right.outputSchema),
						leftJoinIndices, rightJoinIndices, leftOutputPos, rightOutputPos,
						outputSchema, numBuckets, parent, dest, exec.getNodeAddress(), operatorId,
						exec.getRecordTuples(), exec.app.getMetadataFactory(), exec, exec.getRecoveryOperators());
			} else {
				return new PipelinedHashJoin<M>(exec.getSchema(left.outputSchema), exec.getSchema(right.outputSchema),
						leftJoinIndices, rightJoinIndices, leftOutputPos, rightOutputPos,
						outputSchema, parent, dest, exec.getNodeAddress(), operatorId,
						exec.getRecordTuples(), exec.app.getMetadataFactory(), exec, exec.getRecoveryOperators());
			}
		} catch (ValueMismatchException vme) {
			throw new Operator.OperatorCreationException(this.operatorId,vme);
		}
	}

	@Override
	protected boolean subclassEquals(TwoInputQueryPlan<?> jqp) {
		if (jqp instanceof PipelinedJoinNode) {
			PipelinedJoinNode<?> pjn = (PipelinedJoinNode<?>) jqp;
			return leftJoinIndices.equals(pjn.leftJoinIndices) &&
			rightJoinIndices.equals(pjn.rightJoinIndices) &&
			leftOutputPos.equals(pjn.leftOutputPos) &&
			rightOutputPos.equals(pjn.rightOutputPos) && numBuckets == pjn.numBuckets;
		} else {
			return false;
		}
	}
	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc, el, schemas);
		Element leftJoinEl = DomUtils.addChild(doc, el, "leftJoinIndices");
		writeList(doc, leftJoinEl, leftJoinIndices);
		Element rightJoinEl = DomUtils.addChild(doc, el, "rightJoinIndices");
		writeList(doc, rightJoinEl, rightJoinIndices);
		Element leftOutputEl = DomUtils.addChild(doc, el, "leftOutputPos");
		writeList(doc, leftOutputEl, leftOutputPos);
		Element rightOutputEl = DomUtils.addChild(doc, el, "rightOutputPos");
		writeList(doc, rightOutputEl, rightOutputPos);
		if (numBuckets >= 0) {
			el.setAttribute("numBuckets", Integer.toString(numBuckets));
		}

	}

	private static void writeList(Document doc, Element parent, List<Integer> list) {
		for (Integer col : list) {
			Element colEl = DomUtils.addChild(doc, parent, "col");
			if (col != null && col != -1) {
				colEl.setAttribute("pos", Integer.toString(col));
			}
		}
	}

	public static <M> PipelinedJoinNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlanData qpd = deserializeHelper(el);
		QueryPlan<M> leftInput = getLeftInput(el, schemas, alreadyIds, metadataClass);
		QueryPlan<M> rightInput = getRightInput(el, schemas, alreadyIds, metadataClass);

		Element leftJoinEl = DomUtils.getChildElementByName(el, "leftJoinIndices");
		if (leftJoinEl == null) {
			throw new XMLParseException("Missing leftJoinIndices child", el);
		}
		List<Integer> leftJoinIndices = readList(leftJoinEl);

		Element rightJoinEl = DomUtils.getChildElementByName(el, "rightJoinIndices");
		if (rightJoinEl == null) {
			throw new XMLParseException("Missing rightJoinIndices child", el);
		}
		List<Integer> rightJoinIndices = readList(rightJoinEl);

		Element leftOutputEl = DomUtils.getChildElementByName(el, "leftOutputPos");
		if (leftOutputEl == null) {
			throw new XMLParseException("Missing leftOutputPos child", el);
		}
		List<Integer> leftOutputPos = readList(leftOutputEl);

		Element rightOutputEl = DomUtils.getChildElementByName(el, "rightOutputPos");
		if (rightOutputEl == null) {
			throw new XMLParseException("Missing rightOutputPos child", el);
		}
		List<Integer> rightOutputPos = readList(rightOutputEl);

		String numBucketsString = el.getAttribute("numBuckets");
		int numBuckets;
		if (numBucketsString.length() == 0) {
			numBuckets = -1;
		} else {
			try {
				numBuckets = Integer.parseInt(numBucketsString);
			} catch (NumberFormatException nfe) {
				throw new XMLParseException("Invalid numBuckets attribute", nfe, el);
			}
		}
		
		return new PipelinedJoinNode<M>(schemas.get(qpd.outputSchema), leftJoinIndices,
				rightJoinIndices, leftOutputPos, rightOutputPos, qpd.loc,
				numBuckets, leftInput, rightInput, qpd.operatorId);
	}

	static List<Integer> readList(Element parent) throws XMLParseException {
		List<Element> children = DomUtils.getChildElementsByName(parent, "col");
		List<Integer> retval = new ArrayList<Integer>(children.size());
		for (Element el : children) {
			String pos = el.getAttribute("pos");
			if (pos.length() == 0) {
				retval.add(null);
			} else {
				retval.add(Integer.parseInt(pos));
			}
		}
		return retval;
	}

	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, int[] hashCols, Source schemas) {
		if (hashCols != null) {
			int[] val = new int[hashCols.length];
			int pos = 0;
			for (int col : hashCols) {
				val[pos++] = col;
			}
			opsHashCols.put(operatorId, val);
		}
		left.getRecoveryResendOperators(opsHashCols, null, schemas);
		right.getRecoveryResendOperators(opsHashCols, null, schemas);
		return true;
	}
}
