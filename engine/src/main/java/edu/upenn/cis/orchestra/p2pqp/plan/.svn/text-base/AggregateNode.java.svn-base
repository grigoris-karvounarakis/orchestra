package edu.upenn.cis.orchestra.p2pqp.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.optimization.Aggregate.AggFunc;
import edu.upenn.cis.orchestra.p2pqp.HashAggregator;
import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QueryExecution;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class AggregateNode<M> extends OneInputQueryPlan<M> {
	private static final long serialVersionUID = 1L;

	private final List<Integer> groupingColumns;
	private final List<OutputColumn> outputColumns;
	
	public AggregateNode(QpSchema inputSchema, QpSchema outputSchema,
			List<Integer> groupingColumns, List<OutputColumn> outputColumns,
			Location loc, int operatorId, QueryPlan<M> input) {
		super(inputSchema.getRelationName(), outputSchema.getRelationName(), loc,
				operatorId, input);

		this.groupingColumns = new ArrayList<Integer>(groupingColumns);
		this.outputColumns = new ArrayList<OutputColumn>(outputColumns);
	}

	@Override
	Operator<M> createOperator(QueryExecution<M> exec, Operator<M> parent, WhichInput dest, QueryPlan<M> parentNode) {
		final QpSchema outputSchema = exec.getSchema(this.outputSchema);
		List<HashAggregator.OutputColumn> ocs = new ArrayList<HashAggregator.OutputColumn>(outputSchema.getNumCols());
		for (OutputColumn oc : outputColumns) {
			ocs.add(oc.getAggOutputColumn());
		}
		return new HashAggregator<M>(parent, dest, exec.getSchema(inputSchema),
				outputSchema, groupingColumns, ocs, exec.getNodeAddress(),
				operatorId, exec.getRecordTuples(), exec.app.getMetadataFactory(), exec, exec.getRecoveryOperators(), exec);
	}

	public static class OutputColumn implements Serializable {
		private static final long serialVersionUID = 1L;
		public final int inputCol;
		public final int countCol;
		public final AggFunc agg;
		
		
		/**
		 * Create an object to represent the output of a grouping attribute
		 * 
		 * @param inputCol			The index of the grouping column
		 * 							in the input schema
		 */
		public OutputColumn(int inputCol) {
			this(inputCol,null);
		}
		
		/**
		 * Create an object to represent the output of an aggregate
		 * 
		 * @param inputCol			The index of the input column in the
		 * 							input schema
		 * @param agg				The aggregate function to apply to the
		 * 							input column
		 */
		public OutputColumn(int inputCol, AggFunc agg) {
			if (inputCol < 0 && (! agg.canBeNullary)) {
				throw new IllegalArgumentException("Aggregate function " + agg + " cannot take no inputs");
			}
			this.inputCol = inputCol;
			this.agg = agg;
			this.countCol = -1;
		}
		
		/**
		 * Create an object to represent a nullary aggregate function (e.g. COUNT)
		 * 
		 * @param agg		The nullary aggregate function
		 */
		public OutputColumn(AggFunc agg) {
			this(-1,agg);
		}
		
		/**
		 * Create an object to represent a rewritten average column
		 * 
		 * @param sumCol		The index of the sum column in the input schema
		 * @param countCol		The index of the count column in the input schema
		 */
		public OutputColumn(int sumCol, int countCol) {
			this.inputCol = sumCol;
			this.countCol = countCol;
			this.agg = null;
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			OutputColumn oc = (OutputColumn) o;
			if  (inputCol != oc.inputCol) {
				return false;
			}
			if (agg == null) {
				return oc.agg == null;
			} else {
				return agg.equals(oc.agg);
			}
		}
		
		HashAggregator.OutputColumn getAggOutputColumn() {
			if (inputCol >= 0 && countCol >= 0) {
				return new HashAggregator.RewrittenAvgColumn(inputCol, countCol);		
			} else if (agg == null) {
				return new HashAggregator.OutputColumn(inputCol);
			} else if (inputCol >= 0) {
				return new HashAggregator.AggColumn(inputCol, agg);
			} else if (inputCol < 0 && countCol < 0) {
				return new HashAggregator.AggColumn(agg);
			} else {
				throw new IllegalStateException("Should not aggregator and only count column");
			}
		}		
	}

	@Override
	protected boolean subclassEquals(OneInputQueryPlan<?> qp) {
		if (qp instanceof AggregateNode) {
			AggregateNode<?> an = (AggregateNode<?>) qp;
			return groupingColumns.equals(an.groupingColumns) &&
				outputColumns.equals(an.outputColumns);
		} else {
			return false;
		}
	}

	@Override
	void serialize(Document doc, Element el, Map<String, QpSchema> schemas) {
		serializeHelper(doc,el,schemas);
		Element grouping = DomUtils.addChild(doc, el, "grouping");
		for (int col : groupingColumns) {
			Element colEl = DomUtils.addChild(doc, grouping, "column");
			colEl.setAttribute("pos", Integer.toString(col));
		}
		
		Element output = DomUtils.addChild(doc, el, "output");
		for (OutputColumn oc : outputColumns) {
			Element colEl = DomUtils.addChild(doc, output, "aggField");
			if (oc.inputCol >= 0) {
				colEl.setAttribute("pos", Integer.toString(oc.inputCol));
			}
			if (oc.countCol >= 0) {
				colEl.setAttribute("count", Integer.toString(oc.countCol));
			}
			if (oc.agg != null) {
				colEl.setAttribute("func", oc.agg.name());
			}
		}		
	}
	
	public static <M> AggregateNode<M> deserialize(Element el, Map<String,QpSchema> schemas, Set<Integer> alreadyIds, Class<M> metadataClass) throws XMLParseException {
		QueryPlan<M> input = getInputQueryPlan(el, schemas, alreadyIds, metadataClass);
		QueryPlanData qpd = deserializeHelper(el);
		
		Element grouping = DomUtils.getChildElementByName(el, "grouping");
		if (grouping == null) {
			throw new XMLParseException("Missing grouping elements tage", el);
		}
		List<Element> groupingColEls = DomUtils.getChildElementsByName(grouping, "column");
		List<Integer> groupingColumns = new ArrayList<Integer>();
		for (Element e : groupingColEls) {
			String pos = e.getAttribute("pos");
			if (pos.length() == 0) {
				throw new XMLParseException("Missing position attribute in grouping column element", e);
			}
			groupingColumns.add(Integer.parseInt(pos));
		}
		
		Element output = DomUtils.getChildElementByName(el, "output");
		List<Element> outputEls = DomUtils.getChildElementsByName(output, "aggField");
		List<OutputColumn> outputColumns = new ArrayList<OutputColumn>();
		for (Element e : outputEls) {
			String pos = e.getAttribute("pos");
			int posInt;
			if (pos.length() == 0) {
				posInt = -1;
			} else {
				try {
					posInt = Integer.parseInt(pos);
				} catch (NumberFormatException nfe) {
					throw new XMLParseException("Error parsing pos attribute", nfe,e);
				}
			}
			String count = e.getAttribute("count");
			int countInt;
			if (count.length() == 0) {
				countInt = -1;
			} else {
				try {
					countInt = Integer.parseInt(count);
				} catch (NumberFormatException nfe) {
					throw new XMLParseException("Error parsing count attribute", nfe, e);
				}
			}
			String func = e.getAttribute("func");
			AggFunc agg = null;
			if (func.length() > 0) {
				agg = AggFunc.valueOf(func);
				if (agg == null) {
					throw new XMLParseException("Invalid aggregate function " + func, e);
				}
			}
			if (posInt < 0) {
				outputColumns.add(new OutputColumn(agg));
			} else if (countInt < 0) {
				outputColumns.add(new OutputColumn(posInt, agg));
			} else {
				if (agg != null) {
					throw new XMLParseException("Should not supply aggregate function for rewritten average", e);
				}
				outputColumns.add(new OutputColumn(posInt, countInt));
			}
		}
		
		return new AggregateNode<M>(schemas.get(input.outputSchema), schemas.get(qpd.outputSchema),
				groupingColumns, outputColumns, qpd.loc, qpd.operatorId,
				input);
	}
	
	boolean getRecoveryResendOperators(Map<Integer, int[]> opsHashCols, int[] hashCols, Source schemas) {
		if (hashCols != null) {
			int[] saveHashCols = new int[hashCols.length];
			int pos = 0;
			for (int col : hashCols) {
				saveHashCols[pos++] = col;
			}
			opsHashCols.put(this.operatorId, saveHashCols);
		}
		input.getRecoveryResendOperators(opsHashCols, null, schemas);
		return true;
	}

}
