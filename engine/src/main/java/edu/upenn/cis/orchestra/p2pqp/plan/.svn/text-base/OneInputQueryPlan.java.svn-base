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
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

abstract class OneInputQueryPlan<M> extends QueryPlan<M> {
	public final QueryPlan<M> input;
	public final String inputSchema;
	
	public OneInputQueryPlan(String inputSchema, String outputSchema,
			Location loc, int operatorId, QueryPlan<M> input) {
		super(outputSchema, loc, operatorId, getDescendents(input), input.isReplicated);

		this.input = input;
		this.inputSchema = inputSchema;
		if (! input.outputSchema.equals(inputSchema)) {
			throw new IllegalArgumentException("Input schema is " + inputSchema + " but output schema of input operator is " + input.outputSchema);
		}
	}

	final public Iterator<QueryPlanAndDest<M>> iterator() {
		return new Iterator<QueryPlanAndDest<M>>() {
			boolean done = false;

			public boolean hasNext() {
				return (! done);
			}

			public QueryPlanAndDest<M> next() {
				if (done) {
					throw new NoSuchElementException();
				} else {
					done = true;
					return new QueryPlanAndDest<M>(input, null);
				}
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
			
	private static Set<Integer> getDescendents(QueryPlan<?> input) {
		Set<Integer> retval = new HashSet<Integer>(input.descendentOperators);
		retval.add(input.operatorId);
		return retval;
	}
	
	protected abstract boolean subclassEquals(OneInputQueryPlan<?> qp);
	
	public boolean equals(Object o) {
		if (o == null || (!(o instanceof OneInputQueryPlan))) {
			return false;
		}
		OneInputQueryPlan<?> qp = (OneInputQueryPlan<?>) o;
		if (! subclassEquals(qp)) {
			return false;
		}
		return outputSchema.equals(qp.outputSchema) && input.equals(qp.input);
	}
	
	void serializeHelper(Document doc, Element el, Map<String,QpSchema> schemas) {
		super.serializeHelper(doc,el,schemas);
		Element input = DomUtils.addChild(doc, el, "input");
		input.appendChild(this.input.serialize(doc, schemas));
	}
	
	static <M> QueryPlan<M> getInputQueryPlan(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		Element inputHolder = DomUtils.getChildElementByName(el, "input");
		if (inputHolder == null) {
			throw new XMLParseException("Missing input element", el);
		}
		List<Element> inputHolderChildren = DomUtils.getChildElements(inputHolder);
		if (inputHolderChildren.size() != 1) {
			throw new XMLParseException("Missing input query plan");
		}
		
		QueryPlan<M> input = QueryPlan.deserialize(inputHolderChildren.get(0), schemas, alreadyIds, metadataClass);
		return input;
	}
		
	public void getOperators(Map<Integer,QueryPlan<M>> operatorsMap) {
		super.getOperators(operatorsMap);
		input.getOperators(operatorsMap);
	}
	
	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, int[] hashCols, Source schemas) {
		return input.getRecoveryResendOperators(opsHashCols, hashCols, schemas);
	}
	
	public void getShipInputLocations(Map<OperatorAndDest,Location> shipInputLocs) {
		input.getShipInputLocations(shipInputLocs);
		if (input instanceof ShipNode) {
			shipInputLocs.put(new OperatorAndDest(this.operatorId, null), input.loc);
		}
	}
}
