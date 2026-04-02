package edu.upenn.cis.orchestra.p2pqp.plan;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

abstract class TwoInputQueryPlan<M> extends QueryPlan<M> {
	public final QueryPlan<M> left, right;
	
	TwoInputQueryPlan(String outputSchema, Location loc,
			QueryPlan<M> left, QueryPlan<M> right,
			int operatorId) {
		super(outputSchema, loc, operatorId, getDescendents(left,right), left.isReplicated && right.isReplicated);
		this.left = left;
		this.right = right;
	}

	public Iterator<QueryPlanAndDest<M>> iterator() {
		return new Iterator<QueryPlanAndDest<M>>() {
			private boolean doneLeft = false, doneRight = false;
			
			public boolean hasNext() {
				return (! doneRight);
			}

			public QueryPlanAndDest<M> next() {
				if (doneRight) {
					throw new NoSuchElementException();
				} else if (doneLeft) {
					doneRight = true;
					return new QueryPlanAndDest<M>(right,WhichInput.RIGHT);
				} else {
					doneLeft = true;
					return new QueryPlanAndDest<M>(left,WhichInput.LEFT);
				}
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
		
	private static Set<Integer> getDescendents(QueryPlan<?> input1, QueryPlan<?> input2) {
		Set<Integer> retval = new HashSet<Integer>(input1.descendentOperators);
		retval.addAll(input2.descendentOperators);
		retval.add(input1.operatorId);
		retval.add(input2.operatorId);
		return retval;
	}

	protected abstract boolean subclassEquals(TwoInputQueryPlan<?> jqp);
	
	public boolean equals(Object o) {
		if (o == null || (! (o instanceof TwoInputQueryPlan))) {
			return false; 
		}
		
		TwoInputQueryPlan<?> jqp = (TwoInputQueryPlan<?>) o;
		if (! subclassEquals(jqp)) {
			return false;
		}
		
		return outputSchema.equals(jqp.outputSchema) && left.equals(jqp.left) && right.equals(jqp.right);
	}
	
	void serializeHelper(Document d, Element el, Map<String,QpSchema> schemas) {
		super.serializeHelper(d, el, schemas);
		
		Element leftInput = DomUtils.addChild(d, el, "leftInput");
		leftInput.appendChild(left.serialize(d, schemas));
		Element rightInput = DomUtils.addChild(d, el, "rightInput");
		rightInput.appendChild(right.serialize(d, schemas));
	}
	
	static <M> QueryPlan<M> getLeftInput(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		Element leftInput = DomUtils.getChildElementByName(el, "leftInput");
		if (leftInput == null) {
			throw new XMLParseException("Missing leftInput child", el);
		}
		List<Element> els = DomUtils.getChildElements(leftInput);
		if (els.isEmpty()) {
			throw new XMLParseException("Missing child of leftInput node", leftInput);
		}
		return QueryPlan.deserialize(els.get(0), schemas, alreadyIds, metadataClass);
	}

	static <M> QueryPlan<M> getRightInput(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		Element rightInput = DomUtils.getChildElementByName(el, "rightInput");
		if (rightInput == null) {
			throw new XMLParseException("Missing rightInput child", el);
		}
		List<Element> els = DomUtils.getChildElements(rightInput);
		if (els.isEmpty()) {
			throw new XMLParseException("Missing child of rightInput node", rightInput);
		}
		return QueryPlan.deserialize(els.get(0), schemas, alreadyIds, metadataClass);
	}
		
	static void checkJoinIndices(QpSchema leftSchema, QpSchema rightSchema, QpSchema outputSchema,
			List<Integer> leftJoinIndices, List<Integer> rightJoinIndices,
			List<Integer> leftOutputPos, List<Integer> rightOutputPos) {
		
		int leftSize = leftSchema.getNumCols(), rightSize = rightSchema.getNumCols();
		int outputSize = outputSchema.getNumCols();
		
		for (int pos : leftJoinIndices) {
			if (pos >= leftSize) {
				throw new IllegalArgumentException("Left join index " + pos + " is greater that left tuple size");
			}
		}
		for (int pos : rightJoinIndices) {
			if (pos >= rightSize) {
				throw new IllegalArgumentException("Right join index " + pos + " is greater that right tuple size");
			}
		}
		
		for (Integer pos : leftOutputPos) {
			if (pos != null && pos >= outputSize) {
				throw new IllegalArgumentException("Left output pos " + pos + " is greater that output tuple size");
			}
		}
		
		for (Integer pos : rightOutputPos) {
			if (pos != null && pos >= outputSize) {
				throw new IllegalArgumentException("Right output pos " + pos + " is greater that output tuple size");
			}
		}
	}
	
	static void checkUnionSchema(QpSchema leftSchema, QpSchema rightSchema, QpSchema outputSchema) {
		
		int leftSize = leftSchema.getNumCols(), rightSize = rightSchema.getNumCols();
		int outputSize = outputSchema.getNumCols();
		
		if (leftSize != rightSize) {
			throw new IllegalArgumentException("Left schema size is not equal to right schema size in the union");
		}
		
		if (leftSize != outputSize || rightSize != outputSize) {
			throw new IllegalArgumentException("Input schema size is not equal to output schema size in the union");
		}
		
		for (int pos = 0; pos < leftSize; pos++) {
			if (leftSchema.getColType(pos) != rightSchema.getColType(pos)) {
				throw new IllegalArgumentException("Left schema type at pos " + pos + 
						" is not equal to right schema type at pos " + pos +"  in the union");
			}
		}
	}

	public void getOperators(Map<Integer,QueryPlan<M>> operatorsMap) {
		super.getOperators(operatorsMap);
		left.getOperators(operatorsMap);
		right.getOperators(operatorsMap);
	}

	public void getShipInputLocations(Map<OperatorAndDest,Location> shipInputLocs) {
		left.getShipInputLocations(shipInputLocs);
		if (left instanceof ShipNode) {
			shipInputLocs.put(new OperatorAndDest(this.operatorId, WhichInput.LEFT), left.loc);
		}
		right.getShipInputLocations(shipInputLocs);
		if (right instanceof ShipNode) {
			shipInputLocs.put(new OperatorAndDest(this.operatorId, WhichInput.RIGHT), right.loc);
		}
	}
}
