package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.p2pqp.MaxIntForPredicate;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.MaxIntForPredicate.IntPredPair;
import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.PredicateLitMismatch;
import edu.upenn.cis.orchestra.predicate.PredicateMismatch;

abstract class Function extends Variable {

	Function(Type t, int hashCode) {
		this.t = t;
		this.hashCode = hashCode;
	}

	final private Type t;
	final private int hashCode;

	public final Type getType() {
		return t;
	}

	abstract public boolean equals(Object o);

	abstract List<Variable> getInputVariables();

	final public int hashCode() {
		return hashCode;
	}

	abstract public String toString();

	/**
	 * Rename the atom variables in a new copy of this function
	 * 
	 * @param mapping			A mapping giving the new names of relation occurrences
	 * @param rt				Relation type information
	 * @return					The new function, or null if nothing has changed
	 */
	abstract Variable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping;

	@Override
	abstract Variable replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) throws VariableRemoved;

	@Override
	final boolean isAggregatedComputable(Set<Variable> groupingVariables) {
		for (Variable v : getInputVariables()) {
			if (! v.isAggregatedComputable(groupingVariables)) {
				return false;
			}
		}
		return true;
	}

	@Override
	final boolean isNonAggregatedComputable(Set<Variable> groupingVariables) {
		for (Variable v : getInputVariables()) {
			if (! v.isNonAggregatedComputable(groupingVariables)) {
				return false;
			}
		}
		return true;
	}

	@Override
	final void getEquivClassesUsed(Set<EquivClass> ecsUsed) {
		for (Variable v : getInputVariables()) {
			v.getEquivClassesUsed(ecsUsed);
		}
	}

	@Override
	final boolean usesVariable(Variable v) {
		for (Variable vv : getInputVariables()) {
			if (vv.equals(v) || vv.usesVariable(v)) {
				return true;
			}
		}
		return false;
	}

	abstract edu.upenn.cis.orchestra.p2pqp.Function getP2PQPFunction(SchemaFactory<? extends QpSchema> schemaFactory);

	final void getInputVariables(Collection<Variable> vs) {
		Collection<Variable> inputs = this.getInputVariables();
		vs.addAll(inputs);
		for (Variable v : inputs) {
			v.getInputVariables(vs);
		}
	}

	abstract Histogram<?> getOutputHistogram(List<Histogram<?>> inputHistograms);

	@SuppressWarnings("unchecked")
	static protected Histogram<? extends Number> getNumericHistogram(Variable v, Histogram<?> h) {
		if (v.getType() instanceof IntType || v.getType() instanceof DoubleType) {
			return (Histogram<? extends Number>) h;
		} else {
			throw new IllegalArgumentException("Can only get a numeric histogram from a numeric variable");
		}
	}


}

/**
 * A function designed to be used by the Orchestra trust conditions. It
 * evaluates each of the conjunctions of predicates and returns the value associated
 * with the highest one that evaluates to true, or zero if none of them do.
 * 
 * @author netaylor
 *
 */
class MaxIntValueForPredFunc extends Function {
	public MaxIntValueForPredFunc(Map<Integer,Set<Set<Predicate>>> valuesAndPredSets) {
		super(IntType.create(true,false), valuesAndPredSets.hashCode());

		// Duplicate the input map so we can be sure it never changes
		for (Map.Entry<Integer,Set<Set<Predicate>>> entry : valuesAndPredSets.entrySet()) {
			Set<Set<Predicate>> predSetSets = new HashSet<Set<Predicate>>(entry.getValue().size());
			for (Set<Predicate> predSet : entry.getValue()) {
				predSetSets.add(new HashSet<Predicate>(predSet));
			}
			this.valuesAndPredSets.put(entry.getKey(), predSetSets);
		}

		Set<Variable> vars = new HashSet<Variable>();
		for (Set<Set<Predicate>> predSets : valuesAndPredSets.values()) {
			for (Set<Predicate> preds : predSets) {
				for (Predicate p : preds) {
					if (! (p.var1 instanceof LiteralVariable)) {
						vars.add(p.var1);
					}
					if (! (p.var2 instanceof LiteralVariable)) {
						vars.add(p.var2);
					}
				}
			}
		}

		inputs = Collections.unmodifiableList(new ArrayList<Variable>(vars));
	}

	// Mapping from values to sets of conjunctions of predicates
	// Uses a reverse comparator so that higher values show up earlier
	private final SortedMap<Integer,Set<Set<Predicate>>> valuesAndPredSets = new TreeMap<Integer,Set<Set<Predicate>>>(Collections.reverseOrder());

	private final List<Variable> inputs;

	boolean typechecked = false;

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		MaxIntValueForPredFunc m = (MaxIntValueForPredFunc) o;
		// Use the hashcode as a prescreen for the potentially expensive
		// equality operator
		if (hashCode() != m.hashCode()) {
			return false;
		}
		return (valuesAndPredSets.equals(m.valuesAndPredSets));
	}

	@Override
	List<Variable> getInputVariables() {
		return inputs;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("{");

		boolean notFirst = false;
		for (Map.Entry<Integer, Set<Set<Predicate>>> entry : valuesAndPredSets.entrySet()) {
			for (Set<Predicate> preds : entry.getValue()) {
				if (notFirst) {
					sb.append(",");
				}
				sb.append("[");
				notFirst = true;
				for (Predicate p : preds) {
					sb.append(p);
				}
				sb.append("]=" + entry.getKey());
			}
		}

		sb.append("}");
		return sb.toString();
	}

	@Override
	MaxIntValueForPredFunc applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping {
		if (mapping == null) {
			return null;
		}
		Map<Integer,Set<Set<Predicate>>> newValuesAndPredSets = new HashMap<Integer,Set<Set<Predicate>>>();

		boolean foundChangedPredicate = false;

		for (Map.Entry<Integer, Set<Set<Predicate>>> entry : valuesAndPredSets.entrySet()) {
			Set<Set<Predicate>> predSets = new HashSet<Set<Predicate>>(entry.getValue().size());
			newValuesAndPredSets.put(entry.getKey(), predSets);
			for (Set<Predicate> predSet : entry.getValue()) {
				Set<Predicate> newPredSet = new HashSet<Predicate>(predSet.size());
				predSets.add(newPredSet);
				for (Predicate p : predSet) {
					Predicate newPred = p.renameVariables(mapping, rt);
					if (newPred == null) {
						newPredSet.add(p);
					} else {
						newPredSet.add(newPred);
						foundChangedPredicate = true;
					}
				}
			}
		}

		if (foundChangedPredicate) {
			return new MaxIntValueForPredFunc(newValuesAndPredSets);
		} else {
			return null;
		}
	}

	@Override
	MaxIntValueForPredFunc replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) throws VariableRemoved {
		Map<Integer,Set<Set<Predicate>>> newValuesAndPredSets = new HashMap<Integer,Set<Set<Predicate>>>();

		boolean foundChangedPredicate = false;

		for (Map.Entry<Integer, Set<Set<Predicate>>> entry : valuesAndPredSets.entrySet()) {
			Set<Set<Predicate>> predSets = new HashSet<Set<Predicate>>(entry.getValue().size());
			newValuesAndPredSets.put(entry.getKey(), predSets);
			for (Set<Predicate> predSet : entry.getValue()) {
				Set<Predicate> newPredSet = new HashSet<Predicate>(predSet.size());
				predSets.add(newPredSet);
				for (Predicate p : predSet) {
					Predicate newPred = p.replaceVariable(mapping, throughEC);
					if (newPred == null) {
						newPredSet.add(p);
					} else {
						newPredSet.add(newPred);
						foundChangedPredicate = true;
					}
				}
			}
		}

		if (foundChangedPredicate) {
			return new MaxIntValueForPredFunc(newValuesAndPredSets);
		} else {
			return null;
		}
	}

	/**
	 * Get the number of independent predicates for each value
	 * 
	 * @return		A mapping from values to the number of predicate sets for that value
	 */
	public Map<Integer,Integer> getValueCounts() {
		Map<Integer,Integer> retval = new HashMap<Integer,Integer>();
		for (Map.Entry<Integer, Set<Set<Predicate>>> entry : valuesAndPredSets.entrySet()) {
			retval.put(entry.getKey(), entry.getValue().size());
		}
		return retval;
	}

	@Override
	MaxIntForPredicate getP2PQPFunction(SchemaFactory<? extends QpSchema> schemaFactory) {
		QpSchema predSchema = schemaFactory.createNewSchema();
		Map<Variable,Integer> schemaPos = new HashMap<Variable,Integer>();
		int pos = 0;
		for (Variable v : getInputVariables()) {
			if (schemaPos.containsKey(v) || v.getType().valueKnown()) {
				continue;
			}
			try {
				predSchema.addCol("C" + Integer.toString(pos), v.getType().getExecutionType());
			} catch (BadColumnName e) {
				throw new RuntimeException(e);
			}
			schemaPos.put(v, pos);
			pos++;
		}
		predSchema.markFinished();
		List<IntPredPair> preds = new ArrayList<IntPredPair>();
		for (Map.Entry<Integer, Set<Set<Predicate>>> me : valuesAndPredSets.entrySet()) {
			int value = me.getKey();
			CONJUNCT: for (Set<Predicate> conjunct : me.getValue()) {
				edu.upenn.cis.orchestra.predicate.Predicate combined = null;
				for (Predicate p : conjunct) {
					edu.upenn.cis.orchestra.predicate.Predicate converted = null;
					edu.upenn.cis.orchestra.predicate.ComparePredicate.Op op;
					Variable v1, v2;
					if (p.var1.getType().valueKnown()) {
						v1 = p.var2;
						v2 = p.var1;
						op = p.op.getReverseComparePredicateOp();
					} else {
						v1 = p.var1;
						v2 = p.var2;
						op = p.op.getComparePredicateOp();
					}
					if (v1.getType().valueKnown()) {
						int compareResult;
						try {
							compareResult = v1.getType().getExecutionType().compareTwo(v1.getType().getConstantValue(), v2.getType().getConstantValue());
						} catch (CompareMismatch e) {
							throw new RuntimeException("Error comparing literals", e);
						}
						if (op.eval(compareResult)) {
							// Predicate is always true
							continue;
						} else {
							// Predicate is always false;
							continue CONJUNCT;
						}
					}
					int col1 = schemaPos.get(v1);
					if (v2.getType().valueKnown()) {
						try {
							converted = ComparePredicate.createColLit(predSchema, col1, op, v2.getType().getConstantValue());
						} catch (PredicateLitMismatch e) {
							throw new RuntimeException("Error converting predicates");
						}
					} else {
						int col2 = schemaPos.get(v2);
						try {
							converted = ComparePredicate.createTwoCols(predSchema, col1, op, col2);
						} catch (PredicateMismatch e) {
							throw new RuntimeException("Error converting predicates");
						}
					}
					if (combined == null) {
						combined = converted;
					} else {
						combined = new AndPred(converted,combined);
					}
				}
				preds.add(new IntPredPair(value,combined));
			}
		}

		return new MaxIntForPredicate(predSchema,preds);
	}

	Histogram<?> getOutputHistogram(List<Histogram<?>> inputHistograms) {
		if (inputHistograms.size() != inputs.size()) {
			throw new IllegalArgumentException("Wrong number of histograms");
		}
		Map<Integer,Integer> predsForVal = getValueCounts();
		if (predsForVal.containsKey(0)) {
			predsForVal.put(0, predsForVal.get(0) + 1);
		} else {
			predsForVal.put(0, 1);
		}
		int totalNumPreds = 0;
		for (Integer i : predsForVal.values()) {
			totalNumPreds += i;
		}
		double card = inputHistograms.get(0).getNumInRange(null, null).cardinality;
		List<Integer> vals = new ArrayList<Integer>(predsForVal.keySet());
		Collections.sort(vals);
		List<Integer> edges = new ArrayList<Integer>();
		List<Double> DVs = new ArrayList<Double>(), cards = new ArrayList<Double>();
		for (Integer i : vals) {
			if (edges.size() == 0) {
				edges.add(i);
			} else if (edges.get(edges.size() - 1) != i) {
				edges.add(i);
				DVs.add(0.0);
				cards.add(0.0);
			}
			edges.add(i + 1);
			DVs.add(1.0);
			cards.add(predsForVal.get(i) * card / totalNumPreds);
		}
		return Histogram.createIntegerHistogram(edges, Histogram.convertDoubleList(cards), Histogram.convertDoubleList(DVs));
	}
}



/**
 * A function that sums several numbers
 * 
 * @author netaylor
 *
 */
class Sum extends Function {
	// Mapping from input variables to multiplicities
	private final Map<Variable,Integer> inputs;
	// Ordering in which the input variables should be passed in
	private final List<Variable> inputList;
	private final double constantValue;

	static Variable create(List<Variable> inputs, List<Integer> multipliers) throws TypeError {
		if (inputs.size() != multipliers.size()) {
			throw new IllegalArgumentException("Arguments must be of the same size");
		}
		Map<Variable,Integer> inputsMap = new HashMap<Variable,Integer>();
		Iterator<Variable>  vIt = inputs.iterator();
		Iterator<Integer> iIt = multipliers.iterator();

		while (vIt.hasNext()) {
			Variable v = vIt.next();
			Integer i = iIt.next();
			Integer count = inputsMap.get(v);
			if (count == null) {
				inputsMap.put(v, i);
			} else {
				inputsMap.put(v, i + count);
			}
		}

		return create(inputsMap, 0.0);
	}

	static Variable create(Collection<Variable> inputs) throws TypeError {
		int size = inputs.size();
		List<Integer> multipliers = new ArrayList<Integer>(size);
		Integer one = new Integer(1);
		for (int i = 0; i < size; ++i) {
			multipliers.add(one);
		}
		if (inputs instanceof List) {
			return create((List<Variable>) inputs, multipliers);
		} else {
			return create(new ArrayList<Variable>(inputs), multipliers);
		}
	}

	static private Variable create(Map<Variable,Integer> inputs, double constantValue) throws TypeError {
		Map<Variable,Integer> multiset = new HashMap<Variable,Integer>();

		for (Map.Entry<Variable,Integer> inputsEntry : inputs.entrySet()) {
			Variable v = inputsEntry.getKey();
			if (v.getType().valueKnown() && (! (v instanceof LiteralVariable))) {
				v = new LiteralVariable(v.getType().getConstantValue());
			}
			int mult = inputsEntry.getValue();
			if (v instanceof Sum) {
				for (Map.Entry<Variable, Integer> me : ((Sum) v).inputs.entrySet()) {
					Integer count = multiset.get(me.getKey());
					if (count == null) {
						count = 0;
					}
					count += mult * me.getValue();
					multiset.put(me.getKey(), count);
				}
				constantValue += ((Sum) v).constantValue * mult;
			} else {
				Integer count = multiset.get(v);
				if (count == null) {
					count = 0;
				}
				count += mult;
				multiset.put(v, count);
			}
		}

		boolean isDouble = false;
		boolean isConstant = true;
		boolean nullable = false;

		Iterator<Map.Entry<Variable,Integer>> it = multiset.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Variable, Integer> me = it.next();
			Variable v = me.getKey();
			int count = me.getValue();
			if (count == 0) {
				it.remove();
				continue;
			}
			Type t = v.getType();
			nullable |= t.nullable;
			nullable |= t.labeledNullable;
			if (t instanceof IntType) {
				if (t.valueKnown()) {
					constantValue += count * (Integer) t.getConstantValue();
				} else {
					isConstant = false;
				}
			} else if (t instanceof DoubleType) {
				isDouble = true;
				if (t.valueKnown()) {
					constantValue += count * (Double) t.getConstantValue();
				} else {
					isConstant = false;
				}
			} else {
				throw new TypeError("Cannot add argument of type " + v.getType());
			}
			if (t.valueKnown()) {
				it.remove();
			}
		}

		if (isConstant) {
			if (isDouble) {
				return new LiteralVariable(constantValue);
			} else {
				return new LiteralVariable((int) constantValue);
			}
		}

		return new Sum(multiset, constantValue, isDouble ? DoubleType.create(nullable,false) : IntType.create(nullable,false));
	}

	private Sum(Map<Variable,Integer> inputs, double constantValue, Type t) {
		super(t, inputs.hashCode());
		this.inputs = Collections.unmodifiableMap(inputs);
		this.constantValue = constantValue;
		inputList = Collections.unmodifiableList(new ArrayList<Variable>(inputs.keySet()));
	}


	Variable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt)
	throws VariableNotInMapping {
		boolean changed = false;
		Map<Variable,Integer> multiset = new HashMap<Variable,Integer>(this.inputs.size());

		for (Map.Entry<Variable, Integer> me : inputs.entrySet()) {
			Variable v = me.getKey();
			Variable vv = v.applyMorphism(mapping, rt);
			if (vv == null) {
				vv = v;
			} else {
				changed = true;
			}
			Integer count = multiset.get(vv);
			if (count == null) {
				multiset.put(vv, me.getValue());
			} else {
				multiset.put(vv, count + me.getValue());
			}
		}

		if (changed) {
			try {
				return create(multiset, constantValue);
			} catch (TypeError e) {
				throw new RuntimeException(e);
			}
		} else {
			return null;
		}
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Sum s = (Sum) o;
		return s.constantValue == constantValue && inputs.equals(s.inputs);
	}

	List<Variable> getInputVariables() {
		return inputList;
	}

	@Override
	edu.upenn.cis.orchestra.p2pqp.Sum getP2PQPFunction(SchemaFactory<? extends QpSchema> schemaFactory) {		
		Number constantValue = null;
		boolean isDouble;
		if (getType() instanceof IntType) {
			isDouble = false;
			if (this.constantValue != 0.0) {
				constantValue = (int) this.constantValue; 
			}
		} else {
			isDouble = true;
			if (this.constantValue != 0.0) {
				constantValue = this.constantValue;
			}
		}
		boolean allowNulls = false;
		List<Integer> mults = new ArrayList<Integer>(inputList.size());
		for (Variable v : inputList) {
			mults.add(inputs.get(v));
			Type t = v.getType();
			allowNulls |= t.labeledNullable;
			allowNulls |= t.nullable;
		}
		return new edu.upenn.cis.orchestra.p2pqp.Sum(mults,allowNulls,isDouble,false,constantValue);
	}

	Variable replaceChildVariable(
			Map<? extends Variable, ? extends Variable> mapping,
			boolean throughEC) throws VariableRemoved {
		Map<Variable,Integer> multiset = new HashMap<Variable,Integer>();
		boolean changed = false;
		for (Map.Entry<Variable, Integer> me : inputs.entrySet()) {
			Variable v = me.getKey();
			Variable vv = v.replaceVariable(mapping, throughEC);
			if (vv == null) {
				vv = v;
			} else {
				changed = true;
			}
			Integer count = multiset.get(v);
			if (count == null) {
				multiset.put(vv, me.getValue());
			} else {
				multiset.put(vv, count + me.getValue());
			}
		}
		if (changed) {
			try {
				return create(multiset,constantValue);
			} catch (TypeError e) {
				throw new RuntimeException(e);
			}
		} else {
			return null;
		}
	}

	double getConstantValue() {
		return constantValue;
	}

	Map<Variable,Integer> getMultiplicites() {
		return inputs;
	}

	public String toString() {
		return "Sum(" + inputs.toString() + "," + constantValue + ")";
	}

	Histogram<?> getOutputHistogram(List<Histogram<?>> inputHistograms) {
		if (inputs.size() == 1) {
			int mult = 0;
			Variable v = null;
			for (Map.Entry<Variable, Integer> me : inputs.entrySet()) {
				v = me.getKey();
				mult = me.getValue();
			}
			Histogram<?> inputHist = inputHistograms.get(0);
			List<? extends Number> bucketEdges = getNumericHistogram(v, inputHist).getBucketEdges();
			double[] cards = inputHist.getCards();
			double[] DVs = inputHist.getDVs();
			// Need to make sure histogram is of correct type
			if (getType() instanceof IntType) {
				List<Integer> newEdges = new ArrayList<Integer>(bucketEdges.size());
				for (Number n : bucketEdges) {
					newEdges.add((int) Math.round(mult * n.doubleValue() + constantValue));
				}
				if (mult < 0) {
					Collections.reverse(newEdges);
					final int num = newEdges.size();
					for (int i = 0; i < num; ++i) {
						newEdges.set(i, newEdges.get(i) + 1);
					}
				}
				return Histogram.createIntegerHistogram(newEdges, cards, DVs);
			} else {
				List<Double> newEdges = new ArrayList<Double>(bucketEdges.size());
				for (Number n : bucketEdges) {
					newEdges.add(mult * n.doubleValue() + constantValue);
				}
				if (mult < 0) {
					Collections.reverse(newEdges);
				}
				return Histogram.createDoubleHistogram(newEdges, cards, DVs);
			}
		} else {
			double min = constantValue, max = constantValue;
			final int numInputs = inputList.size();
			double card = 0.0;
			for (int i = 0; i < numInputs; ++i) {
				Variable v = inputList.get(i);
				int mult = inputs.get(v);
				Histogram<?> h = inputHistograms.get(i);
				if (h.isEmpty()) {
					return h;
				}
				double minSum = mult * ((Number) h.getMinValue()).doubleValue();
				double maxSum = mult * ((Number) h.getMaxValue()).doubleValue();
				if (minSum < maxSum) {
					min += minSum;
					max += maxSum;
				} else {
					min += maxSum;
					max += minSum;
				}
				double thisCard = h.getNumInRange(null, null).cardinality;
				if (thisCard > card) {
					card = thisCard;
				}
			}
			if (getType() instanceof IntType) {
				int iMin = (int) Math.floor(min);
				int iMax = (int) Math.ceil(max);
				int buckSize = iMax - iMin;
				int intNum = (int) Math.floor(card);
				double DVs = 0.0;
				for (int j = 0; j < intNum; ++j) {
					DVs += (buckSize - DVs) / buckSize;
				}
				DVs += (card - intNum) * (buckSize - DVs) / buckSize; 
				List<Integer> bucketEdges = new ArrayList<Integer>(2);
				bucketEdges.add(iMin);
				bucketEdges.add(iMax);
				return Histogram.createIntegerHistogram(bucketEdges, new double[] {card}, new double[] {DVs});
			} else {
				List<Double> bucketEdges = new ArrayList<Double>(2);
				bucketEdges.add(min);
				bucketEdges.add(max);
				double[] cards = { card };
				return Histogram.createDoubleHistogram(bucketEdges, cards, cards);
			}

		}

	}
}

class Concatenate extends Function {
	// Input variables and constants
	private final List<Variable> vars;
	// Input variables without constants
	private final List<Variable> inputs;

	static Variable create(List<Variable> inputs) throws TypeError {
		if (inputs.size() < 2) {
			throw new IllegalArgumentException("Cannot concatenate fewer than two variables");
		}

		List<Variable> expandedVars = new ArrayList<Variable>();

		for (Variable v : inputs) {
			if (v instanceof Concatenate) {
				expandedVars.addAll(((Concatenate) v).vars);
			} else {
				expandedVars.add(v);
			}
		}

		boolean varying = false;
		boolean nullable = false;
		int expectedLength = 0;
		int maxLength = 0;
		String knownValue = "";

		List<Variable> compactedVars = new ArrayList<Variable>();
		for (Variable v : expandedVars) {
			Variable vv;
			if (v instanceof LiteralVariable) {
				vv = v;
			} else if (v.getType().valueKnown()) {
				vv = new LiteralVariable(v.getType().getConstantValue());
			} else {
				vv = v;
			}
			if (vv instanceof LiteralVariable) {
				if (compactedVars.isEmpty() || (! (compactedVars.get(compactedVars.size() - 1) instanceof LiteralVariable))) {
					compactedVars.add(v);
				} else {
					Variable vvv = compactedVars.get(compactedVars.size() - 1);
					String compacted = ((String) ((LiteralVariable) vvv).lit) + ((String) ((LiteralVariable) vv).lit);
					compactedVars.set(compactedVars.size() - 1, new LiteralVariable(compacted));
				}
			} else {
				compactedVars.add(v);
			}
		}

		for (Variable v : compactedVars) {
			Type t = v.getType();
			nullable |= t.nullable;
			nullable |= t.labeledNullable;
			if (t instanceof CharType) {
				CharType ct = (CharType) t;
				expectedLength += ct.length;
				maxLength += ct.length;
				if (t.valueKnown() && knownValue != null) {
					knownValue += (String) t.getConstantValue();
				} else {
					knownValue = null;
				}
			} else if (t instanceof VarCharType) {
				varying = true;
				VarCharType vct = (VarCharType) t;
				expectedLength += vct.expectedLength;
				maxLength += vct.maxLength;
				if (t.valueKnown() && knownValue != null) {
					knownValue += (String) t.getConstantValue();
				} else {
					knownValue = null;
				}
			} else {
				throw new TypeError("Cannot concatenate operand of type " + t);
			}
		}

		if (knownValue != null) {
			return new LiteralVariable(knownValue);
		}

		Type t;
		if (varying) {
			t = new VarCharType(nullable, false, maxLength, expectedLength);
		} else {
			t = new CharType(maxLength,nullable,false);
		}

		return new Concatenate(compactedVars,t);
	}

	private Concatenate(List<Variable> vars, Type t) {
		super(t, vars.hashCode());
		this.vars = Collections.unmodifiableList(vars);
		ArrayList<Variable> inputs = new ArrayList<Variable>();
		for (Variable v : vars) {
			if (! (v instanceof LiteralVariable)) {
				inputs.add(v);
			}
		}
		this.inputs = Collections.unmodifiableList(inputs);
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Concatenate c = (Concatenate) o;
		return (vars.equals(c.vars));
	}

	@Override
	List<Variable> getInputVariables() {
		return inputs;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		boolean notFirst = false;

		for (Variable v : vars) {
			if (notFirst) {
				sb.append(" || ");
			}
			sb.append(v);
			notFirst = true;
		}

		return sb.toString();
	}

	@Override
	Variable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping {
		if (mapping == null) {
			return null;
		}
		List<Variable> newVars = new ArrayList<Variable>(vars.size());
		boolean gotNotNull = false;
		for (Variable v : vars) {
			Variable newVar = v.applyMorphism(mapping, rt);
			if (newVar != null) {
				newVars.add(newVar);
				gotNotNull = true;
			} else {
				newVars.add(v);
			}
		}
		if (gotNotNull) {
			try {
				return Concatenate.create(newVars);
			} catch (TypeError te) {
				throw new RuntimeException("Shouldn't get type error when renaming variables");
			}
		} else {
			return null;
		}
	}


	@Override
	Variable replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) throws VariableRemoved {
		boolean changed = false;
		List<Variable> newVars = new ArrayList<Variable>(vars.size());
		for (Variable v : vars) {
			Variable vv = v.replaceVariable(mapping, throughEC);
			if (vv == null) {
				newVars.add(v);
			} else {
				changed = true;
				newVars.add(vv);
			}
		}
		if (! changed) {
			return null;
		}
		try {
			return Concatenate.create(newVars);
		} catch (TypeError te) {
			throw new RuntimeException("Shouldn't cause type error by expanding variables", te);
		}
	}


	@Override
	edu.upenn.cis.orchestra.p2pqp.Concatenate getP2PQPFunction(SchemaFactory<? extends QpSchema> schemaFactory) {
		boolean varying;
		Type t = getType();
		if (t instanceof VarCharType) {
			varying = true;
		} else {
			varying = false;
		}
		List<String> betweenInputs = new ArrayList<String>();
		List<Integer> inputLengths = new ArrayList<Integer>();

		boolean allowNulls = false;
		boolean seenConstant = false;
		for (Variable v : vars) {
			Type varT = v.getType();
			allowNulls |= varT.nullable;
			allowNulls |= varT.labeledNullable;
			if (varT.valueKnown()) {
				String constant = (String) varT.getConstantValue();
				if (seenConstant) {
					int last = betweenInputs.size() - 1;
					betweenInputs.set(last, betweenInputs.get(last) + constant);
				} else {
					betweenInputs.add(constant);
				}
			} else {
				if (! seenConstant) {
					betweenInputs.add("");
				}
				seenConstant = false;
				if (varT instanceof VarCharType) {
					inputLengths.add(((VarCharType) varT).maxLength);
				} else {
					inputLengths.add(((CharType) varT).length);
				}
			}
		}

		if (betweenInputs.size() <= inputLengths.size()) {
			betweenInputs.add("");
		}

		if (betweenInputs.size() != (inputLengths.size() + 1)) {
			throw new RuntimeException("Error instantiating concatenate function");
		}

		return new edu.upenn.cis.orchestra.p2pqp.Concatenate(varying, allowNulls, betweenInputs, inputLengths);
	}

	Histogram<?> getOutputHistogram(List<Histogram<?>> inputHistograms) {
		if (inputHistograms.size() != inputs.size()) {
			throw new IllegalArgumentException("Wrong number of histograms supplied");
		}
		// For now, let's just use the histogram
		// for the first string being concatenated
		return inputHistograms.get(0);
	}	
}

class Product extends Function {
	private final Map<Variable,Integer> inputs;
	private final List<Variable> inputList;
	private final double constantValue;

	static Variable create(List<Variable> inputs, List<Integer> powers) throws TypeError {
		if (inputs.size() != powers.size()) {
			throw new IllegalArgumentException("Arguments must be of the same size");
		}
		Map<Variable,Integer> inputsMap = new HashMap<Variable,Integer>();
		Iterator<Variable>  vIt = inputs.iterator();
		Iterator<Integer> iIt = powers.iterator();

		while (vIt.hasNext()) {
			Variable v = vIt.next();
			Integer i = iIt.next();
			Integer count = inputsMap.get(v);
			if (count == null) {
				inputsMap.put(v, i);
			} else {
				inputsMap.put(v, i + count);
			}
		}

		return create(inputsMap, 1.0);
	}

	static Variable create(Collection<Variable> inputs) throws TypeError {
		int size = inputs.size();
		List<Integer> multipliers = new ArrayList<Integer>(size);
		Integer one = new Integer(1);
		for (int i = 0; i < size; ++i) {
			multipliers.add(one);
		}
		if (inputs instanceof List) {
			return create((List<Variable>) inputs, multipliers);
		} else {
			return create(new ArrayList<Variable>(inputs), multipliers);
		}
	}


	static private Variable create(Map<Variable,Integer> inputs, double constantValue) throws TypeError {
		Map<Variable,Integer> multiset = new HashMap<Variable,Integer>();

		for (Map.Entry<Variable,Integer> inputsEntry : inputs.entrySet()) {
			Variable v = inputsEntry.getKey();
			if (v.getType().valueKnown() && (! (v instanceof LiteralVariable))) {
				v = new LiteralVariable(v.getType().getConstantValue());
			}
			int power = inputsEntry.getValue();
			if (v instanceof Product) {
				for (Map.Entry<Variable, Integer> me : ((Product) v).inputs.entrySet()) {
					Integer count = multiset.get(me.getKey());
					if (count == null) {
						count = 0;
					}
					count += power * me.getValue();
					multiset.put(me.getKey(), count);
				}
				constantValue *= Math.pow(((Product) v).constantValue, power);
			} else {
				Integer count = multiset.get(v);
				if (count == null) {
					count = 0;
				}
				count += power;
				multiset.put(v, count);
			}
		}

		boolean isDouble = false;
		boolean isConstant = true;
		boolean nullable = false;

		Iterator<Map.Entry<Variable,Integer>> it = multiset.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Variable, Integer> me = it.next();
			Variable v = me.getKey();
			int count = me.getValue();
			if (count == 0) {
				it.remove();
				continue;
			}
			Type t = v.getType();
			nullable |= t.nullable;
			nullable |= t.labeledNullable;
			if (t instanceof IntType) {
				if (t.valueKnown()) {
					constantValue *= Math.pow((Integer) t.getConstantValue(), count);
				} else {
					isConstant = false;
				}
			} else if (t instanceof DoubleType) {
				isDouble = true;
				if (t.valueKnown()) {
					constantValue *= Math.pow((Double) t.getConstantValue(), count);
				} else {
					isConstant = false;
				}
			} else {
				throw new TypeError("Cannot multipy argument of type " + v.getType());
			}
			if (t.valueKnown()) {
				it.remove();
			}
		}

		if (isConstant) {
			if (isDouble) {
				return new LiteralVariable(constantValue);
			} else {
				return new LiteralVariable((int) constantValue);
			}
		}

		return new Product(multiset, constantValue, isDouble ? DoubleType.create(nullable,false) : IntType.create(nullable,false));
	}

	private Product(Map<Variable,Integer> inputs, double constantValue, Type t) {
		super(t, inputs.hashCode());
		this.inputs = Collections.unmodifiableMap(inputs);
		this.constantValue = constantValue;
		inputList = Collections.unmodifiableList(new ArrayList<Variable>(inputs.keySet()));
	}

	Variable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt)
	throws VariableNotInMapping {
		boolean changed = false;
		Map<Variable,Integer> multiset = new HashMap<Variable,Integer>(this.inputs.size());

		for (Map.Entry<Variable, Integer> me : inputs.entrySet()) {
			Variable v = me.getKey();
			Variable vv = v.applyMorphism(mapping, rt);
			if (vv == null) {
				vv = v;
			} else {
				changed = true;
			}
			Integer count = multiset.get(vv);
			if (count == null) {
				multiset.put(vv, me.getValue());
			} else {
				multiset.put(vv, count + me.getValue());
			}
		}

		if (changed) {
			try {
				return create(multiset, constantValue);
			} catch (TypeError e) {
				throw new RuntimeException(e);
			}
		} else {
			return null;
		}
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Product p = (Product) o;
		return p.constantValue == constantValue && inputs.equals(p.inputs);
	}

	List<Variable> getInputVariables() {
		return inputList;
	}

	edu.upenn.cis.orchestra.p2pqp.Function getP2PQPFunction(
			SchemaFactory<? extends QpSchema> schemaFactory) {
		Number constantValue = null;
		boolean isDouble;
		boolean allowNulls = false;
		if (getType() instanceof IntType) {
			isDouble = false;
			if (this.constantValue != 0.0) {
				constantValue = (int) this.constantValue; 
			}
		} else {
			isDouble = true;
			if (this.constantValue != 0.0) {
				constantValue = this.constantValue;
			}
		}
		List<Integer> powers = new ArrayList<Integer>(inputList.size());
		for (Variable v : inputList) {
			powers.add(inputs.get(v));
			Type t = v.getType();
			allowNulls |= t.nullable;
			allowNulls |= t.labeledNullable;
		}
		return new edu.upenn.cis.orchestra.p2pqp.Product(powers,allowNulls,isDouble,false,constantValue);
	}

	Variable replaceChildVariable(
			Map<? extends Variable, ? extends Variable> mapping,
			boolean throughEC) throws VariableRemoved {
		Map<Variable,Integer> multiset = new HashMap<Variable,Integer>();
		boolean changed = false;
		for (Map.Entry<Variable, Integer> me : inputs.entrySet()) {
			Variable v = me.getKey();
			Variable vv = v.replaceVariable(mapping, throughEC);
			if (vv == null) {
				vv = v;
			} else {
				changed = true;
			}
			Integer count = multiset.get(v);
			if (count == null) {
				multiset.put(vv, me.getValue());
			} else {
				multiset.put(vv, count + me.getValue());
			}
		}
		if (changed) {
			try {
				return create(multiset,constantValue);
			} catch (TypeError e) {
				throw new RuntimeException(e);
			}
		} else {
			return null;
		}
	}

	double getConstantValue() {
		return constantValue;
	}

	Map<Variable,Integer> getPowers() {
		return inputs;
	}

	Histogram<?> getOutputHistogram(List<Histogram<?>> inputHistograms) {
		double min = constantValue, max = constantValue;
		final int numInputs = inputList.size();
		double card = 0.0;
		for (int i = 0; i < numInputs; ++i) {
			Variable v = inputList.get(i);
			int power = inputs.get(v);
			Histogram<?> h = inputHistograms.get(i);
			if (h.isEmpty()) {
				return h;
			}
			double minMult = Math.pow(((Number) h.getMinValue()).doubleValue(), power);
			double maxMult = Math.pow(((Number) h.getMaxValue()).doubleValue(), power);
			if (minMult < maxMult) {
				min *= minMult;
				max *= maxMult;
			} else {
				max *= minMult;
				min *= maxMult;
			}
			double thisCard = h.getNumInRange(null, null).cardinality;
			if (thisCard > card) {
				card = thisCard;
			}
		}
		if (getType() instanceof IntType) {
			int iMin = (int) Math.floor(min);
			int iMax = (int) Math.ceil(max);
			int buckSize = iMax - iMin;
			int intNum = (int) Math.floor(card);
			double DVs = 0.0;
			for (int j = 0; j < intNum; ++j) {
				DVs += (buckSize - DVs) / buckSize;
			}
			DVs += (card - intNum) * (buckSize - DVs) / buckSize; 
			List<Integer> bucketEdges = new ArrayList<Integer>(2);
			bucketEdges.add(iMin);
			bucketEdges.add(iMax);
			return Histogram.createIntegerHistogram(bucketEdges, new double[] {card}, new double[] {DVs});
		} else {
			List<Double> bucketEdges = new ArrayList<Double>(2);
			bucketEdges.add(min);
			bucketEdges.add(max);
			double[] cards = { card };
			return Histogram.createDoubleHistogram(bucketEdges, cards, cards);
		}
	}


	public String toString() {
		return "Product(" + inputs.toString() + "," + constantValue + ")";
	}
}

class Year extends Function {
	private final Variable input;
	
	private Year(Variable input) {
		super(IntType.create(input.getType().nullable || input.getType().labeledNullable,false), input.hashCode());
		this.input = input;
	}

	static Variable create(Variable input) throws TypeError {
		if (! (input.getType() instanceof DateType)) {
			throw new TypeError("Cannot use variable of type " + input.getType() + " as a Date");
		}
		if (input.getType().valueKnown()) {
			int year = ((Date) input.getType().getConstantValue()).getYear();
			return new LiteralVariable(year);
		} else {
			return new Year(input);
		}
	}

	Variable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt)
			throws VariableNotInMapping {
		Variable v = input.applyMorphism(mapping, rt);
		if (v == null) {
			return null;
		} else {
			return new Year(v);
		}
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != Year.class) {
			return false;
		}
		return input.equals(((Year) o).input);
	}

	List<Variable> getInputVariables() {
		return Collections.singletonList(input);
	}

	@SuppressWarnings("unchecked")
	Histogram<?> getOutputHistogram(List<Histogram<?>> inputHistograms) {
		if (inputHistograms.size() != 1) {
			throw new IllegalArgumentException("Year takes one input");
		}

		Histogram<Date> hist = (Histogram<Date>) ((Histogram) inputHistograms.get(0));
		
		List<Integer> years = new ArrayList<Integer>();
		List<Double> cards = new ArrayList<Double>(), DVs = new ArrayList<Double>();

		List<Date> dates = hist.getBucketEdges();
		double[] inputCards = hist.getCards(), inputDVs = hist.getDVs();
		
		for (int i = 0; i < inputCards.length; ++i) {
			Date bot = dates.get(i), top = dates.get(i + 1);
			double card = inputCards[i];
			double DV = inputDVs[i];
			int yearBot = bot.getYear();
			int yearTop = top.getYear();
			if (yearTop == yearBot) {
				++yearTop;
			}
			if (years.isEmpty()) {
				years.add(yearBot);
				cards.add(card);
				DVs.add(DV);
				years.add(yearTop);				
			} else if (years.get(years.size() - 1) == yearTop) {
				cards.set(cards.size() - 1, cards.get(cards.size() - 1) + card);
				DVs.set(DVs.size() - 1, DVs.get(DVs.size() - 1) + DV);
			} else if ((years.get(years.size() - 1)) == yearBot) {
				cards.add(card);
				DVs.add(DV);
				years.add(yearTop);
			} else {
				cards.add(0.0);
				DVs.add(0.0);
				years.add(yearBot);
				cards.add(card);
				DVs.add(DV);
				years.add(yearTop);
			}
		}
		
		final int size = DVs.size();
		for (int i = 0; i < size; ++i) {
			int bot = years.get(i);
			int top = years.get(i + 1);
			double maxSize = top - bot + 1;
			if (maxSize > DVs.get(i)) {
				DVs.set(i, maxSize);
			}
		}
		
		return Histogram.createIntegerHistogram(years, Histogram.convertDoubleList(cards),
				Histogram.convertDoubleList(DVs));
	}

	edu.upenn.cis.orchestra.p2pqp.Function getP2PQPFunction(
			SchemaFactory<? extends QpSchema> schemaFactory) {
		Type inputType = input.getType();
		return edu.upenn.cis.orchestra.p2pqp.Year.getInstance(inputType.labeledNullable || inputType.nullable);
	}

	Variable replaceChildVariable(
			Map<? extends Variable, ? extends Variable> mapping,
			boolean throughEC) throws VariableRemoved {
		Variable vv = input.replaceVariable(mapping, throughEC);
		if (vv == null) {
			return null;
		} else {
			try {
				return create(vv);
			} catch (TypeError e) {
				throw new RuntimeException(e);
			}
		}
	}

	public String toString() {
		return "year(" + input + ")";
	}
	
}