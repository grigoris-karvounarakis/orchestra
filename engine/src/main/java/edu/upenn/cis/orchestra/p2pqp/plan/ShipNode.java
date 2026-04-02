package edu.upenn.cis.orchestra.p2pqp.plan;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.ShipOperator;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class ShipNode<M> extends OneInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;

	private final boolean doesRehash;
	private final String namedDest;
	private final int[] newToOld;

	public ShipNode(QpSchema outputSchema, Location loc, int operatorId, QueryPlan<M> input) {
		this(outputSchema,null,outputSchema,loc,operatorId,input);
	}
	
	// schemaMapping maps from input schema to output schema
	public ShipNode(QpSchema inputSchema, Map<Integer,Integer> schemaMapping, QpSchema outputSchema, Location loc, int operatorId, QueryPlan<M> input) {	
		super(inputSchema.getRelationName(), outputSchema.getRelationName(), loc, operatorId, input);
		edu.upenn.cis.orchestra.p2pqp.QpSchema.Location schemaLoc = outputSchema.getLocation();
		if (schemaLoc == edu.upenn.cis.orchestra.p2pqp.QpSchema.Location.STRIPED) {
			doesRehash = true;
			namedDest = null;
		} else if (schemaLoc == edu.upenn.cis.orchestra.p2pqp.QpSchema.Location.NAMED) {
			doesRehash = false;
			namedDest = outputSchema.getNamedLocation();
		} else {
			doesRehash = false;
			namedDest = null;
		}
		if (this.inputSchema.equals(this.outputSchema)) {
			this.newToOld = null;
		} else if (schemaMapping != null) {
			// newToOld[i] contains the index in the old schema from which to take field i in the new schema
			newToOld = new int[outputSchema.getNumCols()];
			Set<Integer> remaining = new HashSet<Integer>();
			if (schemaMapping.size() != outputSchema.getNumCols()) {
				throw new IllegalArgumentException("Schema mapping should contain same number of entries as the new schema");
			}
			for (int i = 0; i < newToOld.length; ++i) {
				remaining.add(i);
			}
			for (Map.Entry<Integer, Integer> me : schemaMapping.entrySet()) {
				if (me.getKey() == null || me.getValue() == null) {
					throw new NullPointerException("Schema mapping should not contain null mapping");
				}
				int key = me.getKey(), value = me.getValue();
				newToOld[value] = key;
				remaining.remove(value);
			}
			if (! remaining.isEmpty()) {
				throw new IllegalArgumentException("Schema mapping does not give source for fields " + remaining + " in new schema");
			}
		} else {
			throw new NullPointerException("Must specify schema mapping when ship node changes schema");
		}
	}

	@Override
	ShipOperator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode)
			throws Operator.OperatorCreationException {

		InetSocketAddress node;
		if (doesRehash) {
			node = null;
		} else if (namedDest != null) {
			node = exec.getNamedNode(namedDest);
		} else {
			node = exec.getOwnerAddress();
		}

		ShipOperator<M> ship = null;
		try {
		if (outputSchema.equals(inputSchema)) {
			ship = new ShipOperator<M>(exec.app, exec, exec.getSchema(outputSchema), node, namedDest, exec.queryId, exec.getNodeAddress(), operatorId, exec.getRecordTuples(), exec.getRecoveryOperators(), parentNode.operatorId, dest);
		} else {
			ship = new ShipOperator<M>(exec.app, exec, exec.getSchema(inputSchema), exec.getSchema(outputSchema), newToOld, node, namedDest, exec.queryId, exec.getNodeAddress(), operatorId, exec.getRecordTuples(), exec.getRecoveryOperators(), parentNode.operatorId, dest);
		}
		} catch (ValueMismatchException vme) {
			throw new Operator.OperatorCreationException(operatorId, vme);
		}
		
		return ship;
	}

	@Override
	protected boolean subclassEquals(OneInputQueryPlan<?> qp) {
		if (qp instanceof ShipNode) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc, el, schemas);
		if (newToOld != null) {
			Element mapping = DomUtils.addChild(doc, el, "mapping");
			for (int i = 0; i < newToOld.length; ++i) {
				Element entry = DomUtils.addChild(doc, mapping, "entry");
				entry.setAttribute("inputPos", Integer.toString(newToOld[i]));
				entry.setAttribute("outputPos", Integer.toString(i));
			}
		}
	}
	
	public static <M> ShipNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlanData qpd = deserializeHelper(el);
		QueryPlan<M> input = getInputQueryPlan(el, schemas, alreadyIds, metadataClass);
		QpSchema inputSchema = schemas.get(input.outputSchema);
		QpSchema outputSchema = schemas.get(qpd.outputSchema);
		Element mapping = DomUtils.getChildElementByName(el, "mapping");
		Map<Integer,Integer> schemaMapping = null;
		if (mapping != null) {
			List<Element> entries = DomUtils.getChildElementsByName(mapping, "entry");
			schemaMapping = new HashMap<Integer,Integer>(entries.size());
			for (Element entry : entries) {
				String inputPos = entry.getAttribute("inputPos");
				String outputPos = entry.getAttribute("outputPos");
				if (inputPos.length() == 0 || outputPos.length() == 0) {
					throw new XMLParseException("Each schema mapping entry must have an inputPos and an outputPos", entry);
				}
				try {
					int inputPosInt = Integer.parseInt(inputPos);
					int outputPosInt = Integer.parseInt(outputPos);
					schemaMapping.put(inputPosInt, outputPosInt);
				} catch (NumberFormatException nfe) {
					throw new XMLParseException(nfe, entry);
				}
			}
		}
		
		return new ShipNode<M>(inputSchema, schemaMapping, outputSchema, qpd.loc, qpd.operatorId, input);
	}
	
	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, int[] hashCols, Source schemas) {
		if (doesRehash) {
			QpSchema outputSchema = schemas.getSchema(this.outputSchema);
			int[] schemaHashCols = outputSchema.getHashCols();
			if (newToOld != null) {
				hashCols = new int[schemaHashCols.length];
				for (int i = 0; i < schemaHashCols.length; ++i) {
					hashCols[i] = newToOld[schemaHashCols[i]];
				}
			} else {
				hashCols = schemaHashCols;
			}
			if (! input.getRecoveryResendOperators(opsHashCols, hashCols, schemas)) {
				opsHashCols.put(this.operatorId, schemaHashCols);
			}
			return true;
		} else {
			return input.getRecoveryResendOperators(opsHashCols, null, schemas);
		}
	}
}
