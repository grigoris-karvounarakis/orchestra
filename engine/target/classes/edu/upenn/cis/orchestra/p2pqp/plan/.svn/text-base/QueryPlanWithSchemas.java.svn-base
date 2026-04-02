package edu.upenn.cis.orchestra.p2pqp.plan;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation.GetSchema;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class QueryPlanWithSchemas<M> {
	public final QueryPlan<M> qp;
	public final Class<M> metadataClass;
	public final double expectedCost;
	public final Map<String,QpSchema> querySchemas;

	public QueryPlanWithSchemas(QueryPlan<M> qp, Collection<? extends QpSchema> schemas, Class<M> metadataClass) {
		this(qp,schemas,Double.NaN,metadataClass);
	}
	
	public QueryPlanWithSchemas(QueryPlan<M> qp, Collection<? extends QpSchema> schemas, double expectedCost, Class<M> metadataClass) {
		this.qp = qp;
		Map<String,QpSchema> querySchemas = new HashMap<String,QpSchema>(schemas.size());
		for (QpSchema s : schemas) {
			querySchemas.put(s.getName(), s);
		}
		this.querySchemas = Collections.unmodifiableMap(querySchemas);
		this.expectedCost = expectedCost;
		this.metadataClass = metadataClass;
	}

	public QueryPlanWithSchemas(QueryPlan<M> qp, Map<String,QpSchema> schemas, Class<M> metadataClass) {
		this(qp,schemas,Double.NaN,metadataClass);
	}
	
	public QueryPlanWithSchemas(QueryPlan<M> qp, Map<String,QpSchema> schemas, double expectedCost, Class<M> metadataClass) {
		this.metadataClass = metadataClass;
		this.qp = qp;
		querySchemas = Collections.unmodifiableMap(new HashMap<String,QpSchema>(schemas));
		this.expectedCost = expectedCost;
	}
	
	public void serialize(Document doc, Element el, Map<String,QpSchema> otherSchemas) {
		NumberFormat nf = NumberFormat.getInstance();
		List<QpSchema> schemas = QpSchema.sortByFKs(querySchemas.values(), querySchemas.keySet());
		Map<String,QpSchema> combined = new UnionMap(otherSchemas,querySchemas);
		
		Element queryPlan = DomUtils.addChild(doc, el, "queryPlan");
		if (! Double.isNaN(expectedCost)) {
			queryPlan.setAttribute("expectedCost", nf.format(expectedCost));
		}
		queryPlan.appendChild(qp.serialize(doc, combined));
		for (QpSchema s : schemas) {
			el.appendChild(s.serialize(doc));
		}
		
		el.setAttribute("metadataClass", metadataClass.getName());
	}

	public static <M> QueryPlanWithSchemas<M> deserialize(Element el, final Map<String,QpSchema> otherSchemas, Class<M> metadataClass) throws XMLParseException {
		final Map<String,QpSchema> newSchemas = new HashMap<String,QpSchema>(); 
		GetSchema<QpSchema> gs = new GetSchema<QpSchema>() {
			public QpSchema getSchema(String name) {
				QpSchema qs = otherSchemas.get(name);
				if (qs != null) {
					return qs;
				}
				return newSchemas.get(name);
			}
		};
		Element queryPlan = DomUtils.getChildElementByName(el, "queryPlan");
		if (queryPlan == null) {
			throw new XMLParseException("Missing queryPlan node", el);
		}
		List<Element> serializedQps = DomUtils.getChildElements(queryPlan);
		if (serializedQps.isEmpty() || serializedQps.size() > 1) {
			throw new XMLParseException("Expected one child", queryPlan);
		}

		List<Element> schemaEls = DomUtils.getChildElementsByName(el, "QpSchema");
		for (Element schemaEl : schemaEls) {
			QpSchema s = QpSchema.deserialize(schemaEl, gs);
			newSchemas.put(s.getName(), s);
		}

		Map<String,QpSchema> schemas = new UnionMap(otherSchemas, newSchemas);

		QueryPlan<M> qp = QueryPlan.deserializePlan(serializedQps.get(0), schemas, metadataClass);
		String expectedCostStr = queryPlan.getAttribute("expectedCost");
		double expectedCost = Double.NaN;
		if (expectedCostStr.length() != 0) {
			NumberFormat nf = NumberFormat.getInstance();
			try {
				expectedCost = nf.parse(expectedCostStr).doubleValue();
			} catch (ParseException e) {
				throw new XMLParseException("Error parsing expected cost", e, queryPlan);
			}
		}
		
		String metadataClassName = el.getAttribute("metadataClass");
		if (metadataClassName.length() == 0) {
			throw new XMLParseException("Missing metadataClass attribute", el);
		}
		try {
			if (Class.forName(metadataClassName) != metadataClass) {
				throw new XMLParseException("Metadata class from plan '" + metadataClassName + "' does not match supplied metadata type " + metadataClass.getName());
			}
		} catch (ClassNotFoundException e) {
			throw new XMLParseException("Metadata class from plan '" + metadataClassName + "' not found");
		}

		return new QueryPlanWithSchemas<M>(qp,newSchemas, expectedCost, metadataClass);
	}

	private static class UnionMap implements Map<String,QpSchema> {
		private Map<String,QpSchema> otherSchemas;
		private Map<String,QpSchema> newSchemas;

		UnionMap(Map<String,QpSchema> otherSchemas, Map<String,QpSchema> newSchemas) {
			this.otherSchemas = otherSchemas;
			this.newSchemas = newSchemas;
		}


		public void clear() {
			throw new UnsupportedOperationException();
		}

		public boolean containsKey(Object key) {
			return otherSchemas.containsKey(key) || newSchemas.containsKey(key);
		}

		public boolean containsValue(Object value) {
			throw new UnsupportedOperationException();
		}

		public Set<java.util.Map.Entry<String, QpSchema>> entrySet() {
			throw new UnsupportedOperationException();
		}

		public QpSchema get(Object key) {
			QpSchema qs = otherSchemas.get(key);
			if (qs != null) {
				return qs;
			}
			return newSchemas.get(key);
		}

		public boolean isEmpty() {
			return otherSchemas.isEmpty() && newSchemas.isEmpty();
		}

		public Set<String> keySet() {
			throw new UnsupportedOperationException();
		}

		public QpSchema put(String key, QpSchema value) {
			throw new UnsupportedOperationException();
		}

		public void putAll(Map<? extends String, ? extends QpSchema> m) {
			throw new UnsupportedOperationException();
		}

		public QpSchema remove(Object key) {
			throw new UnsupportedOperationException();
		}

		public int size() {
			throw new UnsupportedOperationException();
		}

		public Collection<QpSchema> values() {
			throw new UnsupportedOperationException();
		}			
	};

	private QueryPlanWithSchemas(QueryPlan<M> qp, Class<M> metadataClass,
		double expectedCost, Map<String,QpSchema> querySchemas) {
		this.qp = qp;
		this.metadataClass = metadataClass;
		this.expectedCost = expectedCost;
		this.querySchemas = querySchemas;
	}
	
	@SuppressWarnings("unchecked")
	public <MM> QueryPlanWithSchemas<MM> convertNullPlan(Class<MM> metadataClass) {
		if (metadataClass == Null.class && this.metadataClass == Null.class) {
			return (QueryPlanWithSchemas<MM>) this;
		} else if (this.metadataClass == Null.class) {
			return new QueryPlanWithSchemas<MM>((QueryPlan<MM>) qp, metadataClass, expectedCost, querySchemas);
		} else {
			throw new ClassCastException("Can only convert Null plans to metadata-enabled plans");
		}
	}
}
