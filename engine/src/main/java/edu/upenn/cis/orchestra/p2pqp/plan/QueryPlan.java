package edu.upenn.cis.orchestra.p2pqp.plan;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.Pair;
import edu.upenn.cis.orchestra.util.XMLParseException;

public abstract class QueryPlan<M> implements Iterable<QueryPlan.QueryPlanAndDest<M>> {
	public final String outputSchema;
	public final Location loc;
	public final int operatorId;
	public final Set<Integer> descendentOperators;
	public final boolean isReplicated;
	
	private double expectedCost = Double.NEGATIVE_INFINITY;
	private double expectedCard = Double.NEGATIVE_INFINITY;
	
	public QueryPlan(String outputSchema, Location loc, int operatorId, Set<Integer> descendentOperators, boolean isReplicated) {
		if (loc == null) {
			throw new NullPointerException();
		}
		this.outputSchema = outputSchema;
		this.loc = loc;
		this.operatorId = operatorId;
		this.descendentOperators = Collections.unmodifiableSet(new HashSet<Integer>(descendentOperators));
		this.isReplicated = isReplicated;
	}
	
	/**
	 * Initialize a QueryExecution object for the distributed components of
	 * a query
	 * 
	 * @param exec			The QueryExecution object to add things too
	 * @throws Operator.OperatorCreationException
	 */
	public final void createDistributedExecution(QueryExecution<M> exec)
	throws Operator.OperatorCreationException {
		createDistributedExecution(exec, null, null, null);
	}

	/**
	 * Initialize a QueryExecution object for the distributed components of
	 * a query
	 * 
	 * @param exec			The QueryExecution object to add things too
	 * @param parent		The parent operator of this node (which must be
	 * 						created before creating this node)
	 * @param parentNode	The plan node for the parent of this node
	 * @param dest			The destination of the output tuples in the
	 * 						parent operator
	 * @throws Operator.OperatorCreationException
	 */
	final void createDistributedExecution(QueryExecution<M> exec, Operator<M> parent, QueryPlan<M> parentNode, WhichInput dest)
	throws Operator.OperatorCreationException {
		Operator<M> o = null;
		if (loc.isDistributed()) {
			o = createOperator(exec, parent, dest, parentNode);
			exec.addOperator(o);
		}
		for (QueryPlanAndDest<M> qpad : this) {
			qpad.qp.createDistributedExecution(exec, o, this, qpad.d);
		}
	}

	/**
	 * Initialize a QueryExecution object for the components of
	 * a query that execute at the query's owner
	 * 
	 * @param exec			The QueryExecution object to add things too
	 * @throws Operator.OperatorCreationException
	 */
	public final void createCentralExecution(QueryExecution<M> exec)
		throws Operator.OperatorCreationException {
		createCentralExecution(exec, null, null, null);
	}
	
	/**
	 * Initialize a QueryExecution object for the components of
	 * a query that execute at the query's owner
	 * 
	 * @param exec			The QueryExecution object to add things too
	 * @param parent		The parent operator of this node (which must be
	 * 						created before creating this node)
	 * @param parentNode	The plan node for the parent of this node
	 * @param dest			The destination of the output tuples in the
	 * 						parent operator
	 * @throws Operator.OperatorCreationException
	 */
	final void createCentralExecution(QueryExecution<M> exec, Operator<M> parent, QueryPlan<M> parentNode, WhichInput dest)
		throws Operator.OperatorCreationException {
		Operator<M> o = null;
		if (loc.isCentralized()) {
			o = createOperator(exec, parent, dest, parentNode);
			exec.addOperator(o);
		}
		for (QueryPlanAndDest<M> qpad : this) {
			qpad.qp.createCentralExecution(exec, o, this, qpad.d);
		}
	}
	
	/**
	 * Initialize a QueryExecution object for the components of
	 * a query that execute at a named node
	 * 
	 * @param name			Node name
	 * @param exec			The QueryExecution object to add things too
	 * @throws Operator.OperatorCreationException
	 */
	public final void createNamedExecution(String name, QueryExecution<M> exec)
	throws Operator.OperatorCreationException {
		createNamedExecution(name, exec, null, null, null);
		
	}
	
	/**
	 * Initialize a QueryExecution object for the components of
	 * a query that execute at a named node
	 * 
	 * @param name			Node name
	 * @param exec			The QueryExecution object to add things too
	 * @param parent		The parent operator of this node (which must be
	 * 						created before creating this node)
	 * @param parentNode	The plan node for the parent of this node
	 * @param dest			The destination of the output tuples in the
	 * 						parent operator
	 * @throws Operator.OperatorCreationException
	 */
	final void createNamedExecution(String name, QueryExecution<M> exec, Operator<M> parent, QueryPlan<M> parentNode, WhichInput dest)
		throws Operator.OperatorCreationException {
		Operator<M> o = null;
		if (loc.isNamed() && loc.getName().equals(name)) {
			o = createOperator(exec, parent, dest, parentNode);
			exec.addOperator(o);
		}
		for (QueryPlanAndDest<M> qpad : this) {
			qpad.qp.createNamedExecution(name, exec, o, this, qpad.d);
		}
	}
	
	abstract Operator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode) throws Operator.OperatorCreationException;

	protected static class QueryPlanAndDest<M> {
		final QueryPlan<M> qp;
		final WhichInput d;
		
		QueryPlanAndDest(QueryPlan<M> qp, WhichInput d) {
			this.qp = qp;
			this.d = d;
		}
	}
	
	public final void getDistributedScans(List<RelationAndFilter> scans, QpSchema.Source schemas) {
		List<QueryPlan<M>> nodes = new ArrayList<QueryPlan<M>>();
		this.getOperatorsOfType(nodes, ScanNode.class);
		
		for (QueryPlan<M> qp : nodes) {
			ScanNode<M> psn = (ScanNode<M>) qp;
			if (psn.scanType == ScanNode.Type.DistributedProbe || psn.scanType == ScanNode.Type.DistributedScan) {
				Predicate p = psn.getKeyColsPred(schemas);
				scans.add(new RelationAndFilter(psn.operatorId, psn.outputSchema, p));
			}
		}
	}

	public final void getBlockingOperatorDependents(Map<Integer,Set<Integer>> deps) {
		List<QueryPlan<M>> nodes = new ArrayList<QueryPlan<M>>();
		this.getOperatorsOfType(nodes, AggregateNode.class);
		for (QueryPlan<M> qp : nodes) {
			deps.put(qp.operatorId, Collections.unmodifiableSet(qp.descendentOperators));
		}
		nodes.clear();
		this.getOperatorsOfType(nodes, ScanNode.class);
		for (QueryPlan<M> qp : nodes) {
			ScanNode<M> sc = (ScanNode<M>) qp;
			if (sc.scanType == ScanNode.Type.LocalScan || sc.scanType == ScanNode.Type.IndexScan) {
				deps.put(qp.operatorId, Collections.unmodifiableSet(qp.descendentOperators));				
			}
		}
	}
	
	public final void getJoinOperatorDependents(Map<Integer,Pair<Set<Integer>,Set<Integer>>> deps) {
		List<QueryPlan<M>> nodes = new ArrayList<QueryPlan<M>>();
		this.getOperatorsOfType(nodes, PipelinedJoinNode.class);
		
		for (QueryPlan<M> qp : nodes) {
			PipelinedJoinNode<M> pjn = (PipelinedJoinNode<M>) qp;
			HashSet<Integer> left = new HashSet<Integer>(pjn.left.descendentOperators);
			left.add(pjn.left.operatorId);
			HashSet<Integer> right = new HashSet<Integer>(pjn.right.descendentOperators);
			right.add(pjn.right.operatorId);
			deps.put(qp.operatorId, new Pair<Set<Integer>,Set<Integer>>(left,right));
		}
	}
	
	public static class RelationAndFilter {
		public final int operatorId;
		public final String relation;
		public final Predicate keyFilter;
		
		RelationAndFilter(int operatorId, String relation, Predicate keyFilter) {
			this.relation = relation;
			this.keyFilter = keyFilter;
			this.operatorId = operatorId;
		}
	}
		
	public abstract boolean equals(Object o);
	
	public final Element serialize(Document doc, Map<String,QpSchema> schemas) {
		String tag = findTag.get(getClass());
		if (tag == null) {
			throw new IllegalStateException("Don't know how to serialize " + this.getClass().getName());
		}
		Element el = doc.createElement(tag);
		serialize(doc,el,schemas);
		
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);
		if (expectedCost >= 0.0) {
			el.setAttribute("expectedCost", df.format(expectedCost));
		}
		
		if (expectedCard >= 0.0) {
			el.setAttribute("expectedCard", df.format(expectedCard));
		}
		
		return el;
	}
	
	abstract void serialize(Document doc, Element el, Map<String,QpSchema> schemas);
	
	public static <M> QueryPlan<M> deserializePlan(Element field, Map<String,QpSchema> schemas, Class<M> metadataClass) throws XMLParseException {
		return deserialize(field, schemas, new HashSet<Integer>(), metadataClass);
	}
	
	@SuppressWarnings("unchecked")
	static <M> QueryPlan<M> deserialize(Element field, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		Class<? extends QueryPlan<?>> c = findClass.get(field.getTagName());
		if (c == null) {
			throw new XMLParseException("Cannot find class to deserialize query plan node", field);
		}
		try {
			Method m = c.getDeclaredMethod("deserialize", Element.class, Map.class, Set.class, Class.class);
			if (m == null) {
				throw new XMLParseException("Could not find deserialization method for class " + c.getName());
			}
			Object o = m.invoke(null, field, schemas,alreadyIds, metadataClass);
			QueryPlan<M> qp = (QueryPlan<M>) o;
			
			if (! alreadyIds.add(qp.operatorId)) {
				throw new XMLParseException("Query plan contains multiple operators with ID " + qp.operatorId);
			}
			DecimalFormat df = new DecimalFormat();
			
			String expectedCostStr = field.getAttribute("expectedCost");
			if (expectedCostStr.length() > 0) {
				try {
					Number expectedCost = df.parse(expectedCostStr);
					qp.setCost(expectedCost.doubleValue());
				} catch (ParseException e) {
					throw new XMLParseException("Error parsing expected cost", e, field);
				}
			}

			String expectedCardStr = field.getAttribute("expectedCard");
			if (expectedCardStr.length() > 0) {
				try {
					Number expectedCard = df.parse(expectedCostStr);
					qp.setCard(expectedCard.doubleValue());
				} catch (ParseException e) {
					throw new XMLParseException("Error parsing expected card", e, field);
				}
			}			
			return qp;
		} catch (NoSuchMethodException e) {
			throw new XMLParseException("Could not find deserialize method for class " + c.getName(), field);
		} catch (IllegalAccessException e) {
			throw new XMLParseException("Error invoking deserialize method on class " + c.getName(), field);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof XMLParseException) {
				throw (XMLParseException) e.getCause();
			} else {
				throw new XMLParseException("Error invoking deserialize method on class " + c.getName(), e.getCause());
			}
		}
	}
	
	private final static Map<Class<? extends QueryPlan<?>>,String> findTag = new HashMap<Class<? extends QueryPlan<?>>,String>();
	private final static Map<String,Class<? extends QueryPlan<?>>> findClass = new HashMap<String,Class<? extends QueryPlan<?>>>();

	@SuppressWarnings("unchecked")
	private static void addClass(String tag, Class<? extends QueryPlan> c) {
		findTag.put((Class<? extends QueryPlan<?>>) c, tag);
		findClass.put(tag, (Class<? extends QueryPlan<?>>) c);
	}
	
	static {
		addClass("aggregate", AggregateNode.class);
		addClass("scan", ScanNode.class);
		addClass("applyFilters", FilterNode.class);
		addClass("function", FunctionNode.class);
		addClass("pipelinedJoin", PipelinedJoinNode.class);
		addClass("union", UnionNode.class);
		addClass("project", ProjectNode.class);
		addClass("ship", ShipNode.class);
		addClass("spool", SpoolNode.class);
	}
	
	final public void setCost(double cost) {
		if (expectedCost >= 0.0) {
			throw new IllegalArgumentException("Cost is already set to " + expectedCost);
		}
		expectedCost = cost;
	}
	
	final public void setCard(double card) {
		if (expectedCard >= 0.0) {
			throw new IllegalArgumentException("Expected cardinality is already set to " + expectedCard);
		}
		expectedCard = card;
	}
	
	final public double getCost() {
		return expectedCost;
	}
	
	static class QueryPlanData {
		final int operatorId;
		final Location loc;
		final String outputSchema;
		
		QueryPlanData(int operatorId, Location loc, String outputSchema) {
			this.operatorId = operatorId;
			this.loc = loc;
			this.outputSchema = outputSchema;
		}
	}
	
	void serializeHelper(Document doc, Element el, Map<String,QpSchema> schemas) {
		el.setAttribute("operatorId", Integer.toString(operatorId));
		if (loc.isCentralized()) {
			el.setAttribute("centralized", Boolean.toString(true));
		} else if (loc.isReplicated()) {
			el.setAttribute("replicated", Boolean.toString(true));
			el.setAttribute("distributed", Boolean.toString(true));
		} else if (loc.isDistributed()) {
			el.setAttribute("distributed", Boolean.toString(true));
		} else {
			el.setAttribute("location", loc.getName());
		}
		if (outputSchema != null) {
			el.setAttribute("outputSchema", outputSchema);
		}
	}
	
	static QueryPlanData deserializeHelper(Element el) throws XMLParseException {
		Map<String,String> atts = DomUtils.getAttributes(el);
		String opIdString = atts.get("operatorId");
		if (opIdString == null) {
			throw new XMLParseException("Missing operatorId attribute", el);
		}
		int operatorId = Integer.parseInt(opIdString);
		
		Location loc = null;
		
		String centralizedString = atts.get("centralized");
		if (centralizedString != null) {
			boolean centralized = Boolean.parseBoolean(centralizedString);
			if (centralized) {
				loc = CentralizedLoc.getInstance();
			}
		}
		
		if (loc == null) {
			String distributedString = atts.get("distributed");
			if (distributedString != null) {
				boolean distributed = Boolean.parseBoolean(distributedString);
				if (distributed) {
					String replicatedString = atts.get("replicated");
					boolean replicated = false;
					if (replicatedString != null) {
						replicated = Boolean.parseBoolean(replicatedString);
					}
					if (replicated) {
						loc = DistributedLoc.getReplicatedInstance();
					} else {
						loc = DistributedLoc.getInstance();
					}
				}
			}
		}
		
		if (loc == null) {
			String namedLoc = atts.get("location");
			if (namedLoc == null) {
				throw new XMLParseException("Need either location attribute or centralized or distributed to be set to true", el);
			}
			loc = new NamedLoc(namedLoc);
		}
		String outputSchema = atts.get("outputSchema");
		// Output schema may sometimes be null (e.g. when storing tuples somewhere
		return new QueryPlanData(operatorId, loc, outputSchema);
	}
	
	static String getAttribute(Element el, String attName) throws XMLParseException {
		String attVal = el.getAttribute(attName);
		if (attVal.length() == 0) {
			throw new XMLParseException("Missing " + attName + " attribute", el);
		}
		return attVal;
	}
	
	public final void getOperatorsOfType(Collection<QueryPlan<M>> operators, Class<?> classToFind) {
		if (classToFind.isInstance(this)) {
			operators.add(this);
		}
		for (QueryPlanAndDest<M> qpad : this) {
			qpad.qp.getOperatorsOfType(operators, classToFind);
		}
	}
	
	public void getOperators(Map<Integer,QueryPlan<M>> operatorsMap) {
		operatorsMap.put(this.operatorId, this);
	}
	
	/**
	 * Get the recovery operators map
	 * 
	 * @param opsHashCols			Where to put the result
	 * @param schemas				Where to find the schemas
	 */
	public final void getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, Source schemas) {
		getRecoveryResendOperators(opsHashCols, null, schemas);
	}
	
	/**
	 * Get the recovery operators map
	 * 
	 * @param opsHashCols			Where to put the result
	 * @param hashCols				The positions in the current relation that will end up as hash columns, in the correct order,
	 * 								if this operator needs to resend
	 * @param schemas				Where to find the schemas
	 * @return						Whether or not the input operator can resend
	 */
	abstract boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, int[] hashCols, Source schemas);
	
	public abstract void getShipInputLocations(Map<OperatorAndDest,Location> shipInputLocs);
	
	public static class OperatorAndDest {
		public final int operator;
		public final WhichInput dest;
		
		public OperatorAndDest(int operator, WhichInput dest) {
			this.operator = operator;
			this.dest = dest;
		}
		
		public int hashCode() {
			return hashCode(operator, dest);
		}
		
		public boolean equals(Object o) {
			OperatorAndDest oad = (OperatorAndDest) o;
			return (this.operator == oad.operator && this.dest == oad.dest);
		}
		
		public String toString() {
			return "(" + operator + "," + dest + ")";
		}
		
		public static int hashCode(int operator, WhichInput dest) {
			return operator * 2 + (dest == null ? 0 : dest.ordinal());			
		}
	}
	
	public static final ThreadLocal<DocumentBuilder> db = new ThreadLocal<DocumentBuilder>() {
		private DocumentBuilderFactory dbf = null;
		@Override
		protected synchronized DocumentBuilder initialValue() {
			try {
				if (dbf == null) {
					dbf = DocumentBuilderFactory.newInstance(); 
				}
				return dbf.newDocumentBuilder();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
				return null;
			}
		}		
	};
}