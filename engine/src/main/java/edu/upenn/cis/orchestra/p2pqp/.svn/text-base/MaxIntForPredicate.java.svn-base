package edu.upenn.cis.orchestra.p2pqp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.predicate.XMLification;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class MaxIntForPredicate extends Function {
	private static final long serialVersionUID = 1L;
	private transient QpSchema inputSchema = null;
	private final String schemaName;
	private final int[] predValues;
	private final Predicate[] preds;

	private static final IntType type = new IntType(false, false);
	
	public MaxIntForPredicate(QpSchema inputSchema, List<IntPredPair> preds) {
		this.inputSchema = inputSchema;
		schemaName = inputSchema.getRelationName();
		ArrayList<IntPredPair> ourPreds = new ArrayList<IntPredPair>(preds);
		Collections.sort(ourPreds);
		final int numPreds = ourPreds.size();
		predValues = new int[numPreds];
		this.preds = new Predicate[numPreds];
		
		for (int i = 0; i < numPreds; ++i) {
			IntPredPair ipp = ourPreds.get(i);
			predValues[i] = ipp.value;
			this.preds[i] = ipp.p;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		
		MaxIntForPredicate mifp = (MaxIntForPredicate) o;
		if (! Arrays.equals(predValues, mifp.predValues)) {
			return false;
		}
		
		if (! Arrays.equals(preds, mifp.preds)) {
			return false;
		}
		
		return (inputSchema.equals(mifp.inputSchema));
	}

	@Override
	public byte[] evaluateFunction(AbstractImmutableTuple<?> input, byte[][] inputFields, int[] inputCols) throws ValueMismatchException,
			EvaluateException {

		// TODO: reimplement MaxIntForPredicate
		throw new UnsupportedOperationException("Need to implement");
		/*
		
		if (input.length != inputSchema.getNumCols()) {
			throw new EvaluateException("Wrong number of inputs to MaxIntForPredicate");
		}
		
		QpMutableTuple<Null> t = new QpMutableTuple<Null>(inputSchema, input);
		
		final int numPreds = preds.length;
		
		for (int i = 0; i < numPreds; ++i) {
			try {
				if (preds[i].eval(t)) {
					return predValues[i];
				}
			} catch (CompareMismatch e) {
				throw new EvaluateException("Error evaluating predicate", e);
			}
		}
		
		return 0;
		*/
	}

	@Override
	public int getNumInputs() {
		return inputSchema.getNumCols();
	}

	@Override
	public Type getOutputType() {
		return type;
	}

	@Override
	public int hashCode() {
		// The other data members don't have hash codes
		return Arrays.hashCode(predValues);
	}

	@Override
	public boolean isValidForInput(int pos, Type inputType) {
		return inputSchema.getColType(pos).canReadFrom(inputType);
	}
	
	public static class IntPredPair implements Comparable<IntPredPair> {
		public final int value;
		public final Predicate p;
		
		public IntPredPair(int value, Predicate p) {
			this.value = value;
			this.p = p;
		}

		// Put large-valued predicates first
		public int compareTo(IntPredPair ipp) {
			return ipp.value - value;
		}
		
	}

	public void loadSchemas(Source schemas) {
		inputSchema = schemas.getSchema(schemaName);
		if (inputSchema == null) {
			throw new IllegalArgumentException("Schema " + schemaName + " not in map supplied to loadSchemas");
		}
	}

	@Override
	protected void subclassSerialize(Document d, Element e,
			Map<String, QpSchema> schemas) {
		e.setAttribute("relation", schemaName);
		final int numPreds = predValues.length;
		QpSchema ns = schemas.get(schemaName);
		for (int i = 0; i < numPreds; ++i) {
			Element pred = DomUtils.addChild(d, e, "predicate");
			pred.setAttribute("returnValue", Integer.toString(predValues[i]));
			XMLification.serialize(preds[i], d, pred, ns);
		}
	}
	
	public static MaxIntForPredicate deserialize(Element e, Map<String,QpSchema> schemas) throws XMLParseException {
		String schemaName = getAttribute(e,"relation");
		QpSchema ns = schemas.get(schemaName);
		List<IntPredPair> ipps = new ArrayList<IntPredPair>();
		List<Element> preds = DomUtils.getChildElements(e);
		for (Element predEl : preds) {
			int value;
			try {
				value = Integer.parseInt(getAttribute(predEl,"returnValue"));
			} catch (NumberFormatException nfe) {
				throw new XMLParseException("Invalid predicate value", nfe, predEl);
			}
			Predicate p = XMLification.deserialize(predEl, ns);
			ipps.add(new IntPredPair(value,p));
		}
		return new MaxIntForPredicate(ns, ipps);
	}
}
