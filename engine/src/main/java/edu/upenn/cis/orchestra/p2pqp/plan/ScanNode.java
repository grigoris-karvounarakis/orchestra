package edu.upenn.cis.orchestra.p2pqp.plan;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.DistributedScanOperator;
import edu.upenn.cis.orchestra.p2pqp.Filter;
import edu.upenn.cis.orchestra.p2pqp.FilterSerialization;
import edu.upenn.cis.orchestra.p2pqp.IndexScanOperator;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.ProbeScanOperator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.predicate.XMLification;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class ScanNode<M> extends NoInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;

	public enum Type {
		DistributedScan, DistributedProbe, LocalScan, IndexScan
	}


	final Type scanType;
	private final String keyColsPred, allColsFilter, scanRelation;
	private final int[] outputToRelation;

	public ScanNode(QpSchema scanRelation, Predicate keyColsPred,
			Filter<? super QpTuple<?>> allColsFilter, Location loc, int operatorId, Type scanType) {
		this(scanRelation, scanRelation, null, keyColsPred, allColsFilter, loc, operatorId, scanType);
	}

	public ScanNode(QpSchema scanRelation, QpSchema outputSchema, Map<Integer,Integer> outputMapping, Predicate keyColsPred,
			Filter<? super QpTuple<?>> allColsFilter, Location loc, int operatorId, Type scanType) {
		super(outputSchema.getRelationName(), loc, operatorId, false);

		if ((scanType == Type.DistributedProbe || scanType == Type.DistributedScan) && (! loc.isDistributed())) {
			throw new IllegalArgumentException("A distributed scan must have a distributed location");
		}

		if (scanType == Type.IndexScan && (! loc.isCentralized())) {
			throw new IllegalArgumentException("An index scan must have a centralized location");			
		}

		if (keyColsPred != null && (! scanRelation.getKeyColsSet().containsAll(keyColsPred.getColumns()))) {
			throw new IllegalArgumentException("Key columns filter uses non-key column");
		}

		if (scanRelation.relId != outputSchema.relId) {
			if (scanType != Type.IndexScan) {
				throw new IllegalArgumentException("Can only have a different output schema in an index scan");
			}
			if (outputMapping == null) {
				outputToRelation = null;
			} else {
				// outputToRelation[i] contains the index in the old schema from which to take field i in the new schema
				outputToRelation = new int[outputSchema.getNumCols()];
				Set<Integer> remaining = new HashSet<Integer>();
				if (outputMapping.size() != outputSchema.getNumCols()) {
					throw new IllegalArgumentException("Schema mapping should contain same number of entries as the new schema");
				}
				for (int i = 0; i < outputToRelation.length; ++i) {
					remaining.add(i);
				}
				for (Map.Entry<Integer, Integer> me : outputMapping.entrySet()) {
					if (me.getKey() == null || me.getValue() == null) {
						throw new NullPointerException("Schema mapping should not contain null mapping");
					}
					int key = me.getKey(), value = me.getValue();
					outputToRelation[key] = value;
					remaining.remove(key);
				}
				if (! remaining.isEmpty()) {
					throw new IllegalArgumentException("Schema mapping does not give source for fields " + remaining + " in new schema");
				}
			}
		} else {
			if (outputMapping != null) {
				throw new IllegalArgumentException("Should only have an output mapping when changing relation");
			}
			outputToRelation = null;
		}

		this.scanRelation = scanRelation.getName();

		if (keyColsPred == null) {
			this.keyColsPred = null;
		} else {
			Document d = db.get().newDocument();
			Element root = d.createElement("keyColsPred");
			d.appendChild(root);
			XMLification.serialize(keyColsPred, d, root, scanRelation);
			StringWriter sw = new StringWriter();
			DomUtils.write(d, sw);
			this.keyColsPred = sw.toString();
		}

		if (allColsFilter == null) {
			this.allColsFilter = null;
		} else {
			Document d = db.get().newDocument();
			Element root = d.createElement("allColsFilter");
			d.appendChild(root);
			FilterSerialization.serialize(d, root, allColsFilter, scanRelation);
			StringWriter sw = new StringWriter();
			DomUtils.write(d, sw);
			this.allColsFilter = sw.toString();

		}

		this.scanType = scanType;
	}

	private ScanNode(QpSchema scanRelation, QpSchema outputSchema, int[] outputToRelation, Element keyColsPred,
			Element allColsFilter, Location loc, int operatorId, Type scanType) {
		super(outputSchema.getRelationName(), loc, operatorId, false);

		this.scanRelation = scanRelation.getName();
		this.outputToRelation = outputToRelation;

		if ((scanType == Type.DistributedProbe || scanType == Type.DistributedScan) && (! loc.isDistributed())) {
			throw new IllegalArgumentException("A distributed scan must have a distributed location");
		}

		if (scanType == Type.IndexScan && (! loc.isCentralized())) {
			throw new IllegalArgumentException("An index scan must have a centralized location");			
		}

		if (scanRelation.relId != outputSchema.relId) {
			if (scanType != Type.IndexScan) {
				throw new IllegalArgumentException("Can only have a different output schema in an index scan");
			}
		} else {
			if (outputToRelation != null) {
				throw new IllegalArgumentException("Should only have an output mapping when changing relation");
			}
		}

		if (keyColsPred == null) {
			this.keyColsPred = null;
		} else {
			Document d = db.get().newDocument();
			Node n = d.importNode(keyColsPred, true);
			d.appendChild(n);
			StringWriter sw = new StringWriter();
			DomUtils.write(d, sw);
			this.keyColsPred = sw.toString();
		}

		if (allColsFilter == null) {
			this.allColsFilter = null;
		} else {
			Document d = db.get().newDocument();
			Node n = d.importNode(allColsFilter, true);
			d.appendChild(n);
			StringWriter sw = new StringWriter();
			DomUtils.write(d, sw);
			this.allColsFilter = sw.toString();

		}

		this.scanType = scanType;
	}

	@Override
	Operator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode)
	throws Operator.OperatorCreationException {
		QpSchema outputSchema = exec.getSchema(this.outputSchema);
		QpSchema scanSchema = exec.getSchema(this.scanRelation);
		Filter<? super QpTuple<M>> allColsFilter = null;
		Predicate keyColsPred = null;

		try {
			if (this.allColsFilter != null) {
				Document d = db.get().parse(new InputSource(new StringReader(this.allColsFilter)));
				Element root = d.getDocumentElement();
				allColsFilter = FilterSerialization.deserialize(root, scanSchema);
			}

			if (this.keyColsPred != null) {
				Document d = db.get().parse(new InputSource(new StringReader(this.keyColsPred)));
				Element root = d.getDocumentElement();
				keyColsPred = XMLification.deserialize(root, scanSchema);
			}
		} catch (Exception e) {
			throw new Operator.OperatorCreationException(this.operatorId, e);
		}

		switch (scanType) {
		case DistributedScan:
			return new DistributedScanOperator<M>(scanSchema.relId,
					exec.app, allColsFilter,
					parent, dest, exec.queryId, exec.getNodeAddress(), operatorId,
					exec.getRecordTuples(), exec.app.getMetadataFactory(), exec, exec.recoveryEnabled(), exec.getInitiallyOwnedRange());
		case DistributedProbe:
			return new ProbeScanOperator<M>(scanSchema, allColsFilter, exec.app.getStore(),
					parent, dest, exec.getNodeAddress(), operatorId, exec.getRecordTuples(),
					exec.app.getMetadataFactory(), exec.recoveryEnabled(), exec.getInitiallyOwnedRange());
		case LocalScan:
			return exec.app.getStore().beginScan(scanSchema.relId, parent, dest, keyColsPred, allColsFilter, exec.epoch, exec.queryId, exec.getNodeAddress(), operatorId, exec.getRecordTuples(), exec.recoveryEnabled(), 0);
		case IndexScan:
			try {
				if (outputToRelation == null) {
					return new IndexScanOperator<M>(exec.app, keyColsPred, scanSchema.relId, exec.epoch,
							parent, dest, exec.queryId, exec.getNodeAddress(), this.operatorId,
							exec.getRecordTuples(), exec.app.getMetadataFactory(), exec, exec.recoveryEnabled());
				} else {
					return new IndexScanOperator<M>(exec.app, keyColsPred, scanSchema.relId, exec.epoch,
							outputSchema, outputToRelation,
							parent, dest, exec.queryId, exec.getNodeAddress(), this.operatorId,
							exec.getRecordTuples(), exec.app.getMetadataFactory(), exec, exec.recoveryEnabled());
				}
			} catch (ValueMismatchException vme) {
				throw new Operator.OperatorCreationException(this.operatorId, vme);
			}
		default:
			throw new IllegalStateException();
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		ScanNode<?> sn = (ScanNode<?>) o;
		return outputSchema.equals(sn.outputSchema) &&
		(keyColsPred == null ? sn.keyColsPred == null : keyColsPred.equals(sn.keyColsPred)) &&
		(allColsFilter == null ? sn.allColsFilter == null : allColsFilter.equals(sn.allColsFilter));
	}

	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc,el,schemas);
		QpSchema ns = schemas.get(outputSchema);

		if (ns == null) {
			throw new IllegalArgumentException("Missing schema for relation " + outputSchema);
		}

		try {
			if (keyColsPred != null) {
				Document d = db.get().parse(new InputSource(new StringReader(keyColsPred)));
				Element keyColsPredNode = d.getDocumentElement();
				Node n = doc.importNode(keyColsPredNode, true);
				el.appendChild(n);
			}
			if (allColsFilter != null) {
				Document d = db.get().parse(new InputSource(new StringReader(allColsFilter)));
				Element allColsFilterNode = d.getDocumentElement();
				Node n = doc.importNode(allColsFilterNode, true);
				el.appendChild(n);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error serializing operator #" + this.operatorId, e);
		}

		el.setAttribute("scanType", scanType.name());
		if (! scanRelation.equals(outputSchema)) {
			el.setAttribute("relation", scanRelation);
		}
		if (outputToRelation != null) {
			Element mapping = DomUtils.addChild(doc, el, "mapping");
			for (int i = 0; i < outputToRelation.length; ++i) {
				Element entry = DomUtils.addChild(doc, mapping, "entry");
				entry.setAttribute("inputPos", Integer.toString(outputToRelation[i]));
				entry.setAttribute("outputPos", Integer.toString(i));
			}
		}
	}

	public static <M> ScanNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlanData qpd = deserializeHelper(el);
		String relationName = el.getAttribute("relation");
		QpSchema outputSchema = schemas.get(qpd.outputSchema);
		int[] outputMapping = null;
		if (relationName.length() == 0) {
			relationName = qpd.outputSchema;
		} else {
			Element mapping = DomUtils.getChildElementByName(el, "mapping");
			if (mapping == null) {
				throw new XMLParseException("Must have mapping when changing relation", el);
			} else {
				List<Element> entries = DomUtils.getChildElementsByName(mapping, "entry");
				outputMapping = new int[outputSchema.getNumCols()];
				for (Element entry : entries) {
					String inputPos = entry.getAttribute("inputPos");
					String outputPos = entry.getAttribute("outputPos");
					if (inputPos.length() == 0 || outputPos.length() == 0) {
						throw new XMLParseException("Each schema mapping entry must have an inputPos and an outputPos", entry);
					}
					try {
						int inputPosInt = Integer.parseInt(inputPos);
						int outputPosInt = Integer.parseInt(outputPos);
						outputMapping[outputPosInt] = inputPosInt;
					} catch (NumberFormatException nfe) {
						throw new XMLParseException(nfe, entry);
					}
				}
			}

		}
		QpSchema scanRelation = schemas.get(relationName);

		Element keyColsFilterEl = DomUtils.getChildElementByName(el, "keyColsPred");

		Element allColsFilterEl = DomUtils.getChildElementByName(el, "allColsFilter");

		String scanTypeString = el.getAttribute("scanType");
		if (scanTypeString.length() == 0) {
			throw new XMLParseException("Missing scanType attribute", el);
		}
		Type scanType;
		try {
			scanType = Type.valueOf(scanTypeString);
		} catch (IllegalArgumentException iae) {
			throw new XMLParseException("Invalid scan type: " + scanTypeString, iae, el);
		}

		return new ScanNode<M>(scanRelation, outputSchema, outputMapping, keyColsFilterEl, allColsFilterEl, qpd.loc, qpd.operatorId, scanType);
	}

	@Override
	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols,
			int[] hashCols, Source schemas) {
		return false;
	}

	Predicate getKeyColsPred(QpSchema.Source schemas) {
		if (keyColsPred == null) {
			return null;
		}
		Document d;
		try {
			d = db.get().parse(new InputSource(new StringReader(keyColsPred)));
		} catch (SAXException e) {
			throw new RuntimeException("Error parsing key cols pred for operator #" + this.operatorId, e);
		} catch (IOException e) {
			throw new RuntimeException("Should not happen", e);
		}
		Element root = d.getDocumentElement();
		try {
			return XMLification.deserialize(root, schemas.getSchema(this.outputSchema));
		} catch (Exception e) {
			throw new RuntimeException("Error parsing key cols pred for operator #" + this.operatorId, e);
		}
	}
}

