package edu.upenn.cis.orchestra.p2pqp.plan;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.p2pqp.Filter;
import edu.upenn.cis.orchestra.p2pqp.FilterOperator;
import edu.upenn.cis.orchestra.p2pqp.FilterSerialization;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.Operator.OperatorCreationException;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;


public class FilterNode<M> extends OneInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;

	private final String filters;

	public FilterNode(QpSchema ns,
			Collection<? extends Filter<? super QpTuple<?>>> filters, Location loc, int operatorId,
					QueryPlan<M> input) {
		super(ns.getRelationName(), ns.getRelationName(), loc, operatorId, input);

		Document d = db.get().newDocument();
		Element root = d.createElement("filters");
		d.appendChild(root);
		for (Filter<? super QpTuple<?>> f : filters) {
			Element el = d.createElement("filter"); 
			FilterSerialization.serialize(d, el, f, ns);
			root.appendChild(el);
		}
		StringWriter sw = new StringWriter();
		DomUtils.write(d, sw);
		this.filters = sw.toString();
	}

	private FilterNode(QpSchema ns, List<Element> filters, Location loc, int operatorId, QueryPlan<M> input) {
		super(ns.getRelationName(), ns.getRelationName(), loc, operatorId, input);
		Document d = db.get().newDocument();
		Element root = d.createElement("filters");
		d.appendChild(root);
		for (Element filter : filters) {
			Node n = d.importNode(filter, true);
			root.appendChild(n);
		}
		StringWriter sw = new StringWriter();
		DomUtils.write(d, sw);
		this.filters = sw.toString();

	}

	@Override
	FilterOperator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode) throws OperatorCreationException {
		QpSchema ns = exec.getSchema(inputSchema);
		List<Filter<? super QpTuple<?>>> filters = new ArrayList<Filter<? super QpTuple<?>>>();
		try {
			for (Element e : this.getFilters()) {
				filters.add(FilterSerialization.deserialize(e, ns));
			}
		} catch (Exception e) {
			throw new Operator.OperatorCreationException(this.operatorId, e);
		}
		return new FilterOperator<M>(filters, parent, dest, exec.getNodeAddress(), operatorId, exec.app.getMetadataFactory(), exec, exec.getRecordTuples(), exec.recoveryEnabled());
	}

	@Override
	protected boolean subclassEquals(OneInputQueryPlan<?> qp) {
		if (qp instanceof FilterNode) {
			FilterNode<?> fn = (FilterNode<?>) qp;
			return fn.filters.equals(filters);
		} else {
			return false;
		}
	}

	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc,el,schemas);
		QpSchema ns = schemas.get(outputSchema);
		if (ns == null) {
			throw new IllegalArgumentException("Schemas for " + outputSchema + " not found");
		}
		try {
			for (Element filter : this.getFilters()) {
				Node n = doc.importNode(filter, true);
				el.appendChild(n);
			}
		} catch (DOMException e) {
			throw new RuntimeException("Error serializing operator #" + this.operatorId, e);
		} catch (SAXException e) {
			throw new RuntimeException("Error serializing operator #" + this.operatorId, e);
		}
	}

	public static <M> FilterNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlanData qpd = deserializeHelper(el);
		QueryPlan<M> input = getInputQueryPlan(el,schemas,alreadyIds,metadataClass);
		QpSchema ns = schemas.get(qpd.outputSchema);

		List<Element> filterEls = DomUtils.getChildElementsByName(el, "filter");

		return new FilterNode<M>(ns, filterEls, qpd.loc, qpd.operatorId, input);
	}

	private List<Element> getFilters() throws SAXException {
		try {
			Document d = db.get().parse(new InputSource(new StringReader(filters)));
			Element root = d.getDocumentElement();
			return DomUtils.getChildElements(root);
		} catch (IOException e) {
			throw new RuntimeException("Shouldn't happen", e);
		}
	}
}
