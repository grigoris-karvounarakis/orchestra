package edu.upenn.cis.orchestra.optimization;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.util.ReadOnlyCollection;
import edu.upenn.cis.orchestra.util.ReadOnlyList;
import edu.upenn.cis.orchestra.util.ReadOnlyMap;
import edu.upenn.cis.orchestra.util.ReadOnlySet;
import edu.upenn.cis.orchestra.util.SubsetIterator;

class RelationMetadata {
	final ReadOnlySet<Variable> primaryKey;
	final ReadOnlyCollection<ForeignKeyDef> foreignKeys;
	final ReadOnlyMap<Variable,Histogram<?>> histograms;
	FunctionalDependencies FDs;
	final double cardinality;
	// If all of the joins in this expression are foreign key joins,
	// this gives the combined selectivity of all predicates evaluated over
	// the component relations
	final double pkSelectivity;

	static class FunctionalDependencies {
		private final Map<ReadOnlySet<Variable>, Set<Variable>> map;
		private boolean finished = false;

		public static final FunctionalDependencies EMPTY = new FunctionalDependencies(0);

		static {
			EMPTY.setFinished();
		}

		FunctionalDependencies(FunctionalDependencies input) {
			this(input.map.size());
			this.addDependenciesFrom(input);
		}

		FunctionalDependencies() {
			map = new HashMap<ReadOnlySet<Variable>,Set<Variable>>();
		}

		FunctionalDependencies(int expectedSize) {
			map = new HashMap<ReadOnlySet<Variable>,Set<Variable>>();
		}

		void setFinished() {
			finished = true;
		}

		void addDependency(Collection<Variable> head, Collection<? extends Variable> body) {
			if (finished) {
				throw new IllegalArgumentException("FunctionalDependencies object is read only");
			}
			if (head.isEmpty() || body.isEmpty()) {
				throw new IllegalArgumentException("head and body cannot be empty");
			}
			for (Variable v : head) {
				if (body.contains(v)) {
					throw new IllegalArgumentException("head and body should be disjoint");
				}
			}
			for (Variable v : body) {
				if (head.contains(v)) {
					throw new IllegalArgumentException("head and body should be disjoint");
				}
			}

			Set<Variable> alreadyBody = map.get(head);
			if (alreadyBody == null) {
				alreadyBody = new HashSet<Variable>(body.size());
				map.put(ReadOnlySet.create(head), alreadyBody);
			}
			alreadyBody.addAll(body);
		}

		FunctionalDependencies applyMorphism(Morphism m, RelationTypes<?,?> rt) {
			FunctionalDependencies newFD = new FunctionalDependencies(map.size());

			for (Map.Entry<ReadOnlySet<Variable>, Set<Variable>> me : map.entrySet()) {
				Set<Variable> head = me.getKey();
				Set<Variable> body = me.getValue();
				Set<Variable> newHead = new HashSet<Variable>(head.size());
				Set<Variable> newBody = new HashSet<Variable>(body.size());

				boolean headChanged = false;

				for (Variable v : head) {
					Variable vv = v.applyMorphism(m, rt);
					if (vv == null) {
						newHead.add(v);
					} else {
						headChanged = true;
						newHead.add(vv);
					}
				}

				for (Variable v : body) {
					Variable vv = v.applyMorphism(m, rt);
					if (vv == null) {
						newBody.add(v);
					} else {
						newBody.add(vv);
					}
				}
				if (headChanged) {
					newFD.addDependency(newHead, newBody);
				} else {
					newFD.addDependency(head, newBody);
				}
			}
			return newFD;
		}

		FunctionalDependencies updateFDsWithECs(Map<? extends Variable,? extends EquivClass> findEc) {
			FunctionalDependencies newFD = new FunctionalDependencies(map.size());

			for (Map.Entry<ReadOnlySet<Variable>, Set<Variable>> me : map.entrySet()) {
				Set<Variable> head = me.getKey();
				Set<Variable> body = me.getValue();
				Set<Variable> newHead = new HashSet<Variable>(head.size());
				Set<Variable> newBody = new HashSet<Variable>(body.size());

				boolean headChanged = false;


				for (Variable v : head) {
					Variable vv = replaceVariables(v, findEc);
					newHead.add(vv);
					if (vv != v) {
						headChanged = true;
					}
				}

				for (Variable v : body) {
					Variable vv = replaceVariables(v, findEc);
					newBody.add(vv);
				}

				if (headChanged) {
					newFD.addDependency(newHead, newBody);
				} else {
					newFD.addDependency(head, newBody);
				}
			}
			return newFD;
		}

		FunctionalDependencies restrictToVariables(Set<? extends Variable> variables) {
			FunctionalDependencies newFD = new FunctionalDependencies(map.size());

			for (Map.Entry<ReadOnlySet<Variable>, Set<Variable>> me : map.entrySet()) {
				Set<Variable> head = me.getKey();
				Set<Variable> body = me.getValue();
				Set<Variable> newBody = new HashSet<Variable>(body);

				if (! variables.containsAll(head)) {
					continue;
				}
				newBody.retainAll(variables);
				
				if (! newBody.isEmpty()) {
					newFD.addDependency(head, newBody);
				}
			}
			return newFD;
		}

		void addDependenciesFrom(FunctionalDependencies fd) {
			for (Map.Entry<ReadOnlySet<Variable>, Set<Variable>> me : fd.map.entrySet()) {
				addDependency(me.getKey(), me.getValue());
			}
		}

		Set<Variable> findDeterminingVariables(Set<? extends Variable> determined) {
			// TODO: do something more efficient
			if (map.isEmpty()) {
				return new HashSet<Variable>(determined);
			}
			Set<Variable> minimal = null;
			Iterator<Set<Variable>> subsetIt = new SubsetIterator<Variable>(determined, true);
			while (subsetIt.hasNext()) {
				Set<Variable> determining = subsetIt.next();
				Set<Variable> created = new HashSet<Variable>(determining);
				boolean changed = true;
				while (changed) {
					changed = false;
					for (Map.Entry<? extends Set<Variable>, Set<Variable>> me : map.entrySet()) {
						if (created.containsAll(me.getKey())) {
							if (created.addAll(me.getValue())) {
								changed = true;
							}
						}
					}
				}
				if (created.containsAll(determined)) {
					if (minimal == null || determining.size() < minimal.size()) {
						minimal = determining;
					}
				}
			}

			return minimal;
		}
	}

	static class ForeignKeyDef {
		// Name of other relation
		final String relation;
		// Columns in other relation
		final ReadOnlyList<Integer> columns;
		// Columns in this relation
		final ReadOnlyList<Variable> localVars;

		ForeignKeyDef(String relation, List<Integer> cols, List<Variable> localVars) {
			this.relation = relation;
			this.columns = ReadOnlyList.create(cols);
			this.localVars = ReadOnlyList.create(localVars);
		}

		public boolean equals(Object o) {
			if (o == null || (! (o instanceof ForeignKeyDef))) {
				return false;
			}
			ForeignKeyDef fk = (ForeignKeyDef) o;
			return (relation.equals(fk.relation) && columns.equals(fk.columns) && localVars.equals(fk.localVars));
		}

		public int hashCode() {
			return relation.hashCode() + 37 * columns.hashCode();
		}
	}

	RelationMetadata(Set<Variable> primaryKey, Collection<ForeignKeyDef> foreignKeys, Map<Variable,Histogram<?>> histograms, FunctionalDependencies fds, double pkSelectivity) {
		this(primaryKey, foreignKeys, histograms, fds, computeExpectedCardinality(histograms.values()), pkSelectivity);
	}

	private RelationMetadata(Set<Variable> primaryKey, Collection<ForeignKeyDef> foreignKeys, Map<Variable,Histogram<?>> histograms, FunctionalDependencies fds, double cardinality, double pkSelectivity) {
		this.primaryKey = ReadOnlySet.create(primaryKey);
		this.foreignKeys = ReadOnlyCollection.create(foreignKeys);
		this.histograms = ReadOnlyMap.create(histograms);
		this.FDs = fds;
		if (this.primaryKey == null) {
			this.pkSelectivity = Double.NaN;
		} else {
			this.pkSelectivity = pkSelectivity;
		}
		this.cardinality = cardinality;
	}
	
	RelationMetadata(Set<Variable> primaryKey, Collection<ForeignKeyDef> foreignKeys, Map<Variable,Histogram<?>> histograms, FunctionalDependencies fds) {
		this(primaryKey, foreignKeys, histograms, fds, computeExpectedCardinality(histograms.values()), primaryKey == null ? Double.NaN : 1.0);
	}


	RelationMetadata(Set<Variable> primaryKey, Collection<ForeignKeyDef> foreignKeys, double cardinality) {
		this.primaryKey = ReadOnlySet.create(primaryKey);
		this.foreignKeys = ReadOnlyCollection.create(foreignKeys);
		this.histograms = null;
		this.cardinality = cardinality;
		this.FDs = FunctionalDependencies.EMPTY;
		if (this.primaryKey == null) {
			pkSelectivity = Double.NaN;
		} else {
			pkSelectivity = 1.0;
		}
	}

	RelationMetadata applyMorphim(Morphism m, RelationTypes<?,?> rt) {
		if (m == null) {
			return this;
		}

		Set<Variable> pk;
		if (primaryKey == null) {
			pk = null;
		} else {
			pk = new HashSet<Variable>(primaryKey.size());
			for (Variable v : primaryKey) {
				Variable vv = v.applyMorphism(m, rt);
				if (vv == null) {
					pk.add(v);
				} else {
					pk.add(vv);
				}
			}
		}

		Map<Variable,Histogram<?>> h = new HashMap<Variable,Histogram<?>>(histograms.size());
		for (Map.Entry<Variable, Histogram<?>> me : histograms.entrySet()) {
			Variable vv = me.getKey().applyMorphism(m, rt);
			if (vv == null) {
				h.put(me.getKey(), me.getValue());
			} else {
				h.put(vv, me.getValue());
			}
		}

		List<ForeignKeyDef> fks = new ArrayList<ForeignKeyDef>(foreignKeys.size());
		for (ForeignKeyDef fk : foreignKeys) {
			List<Variable> newLocalVars = new ArrayList<Variable>(fk.localVars.size());
			for (Variable v : fk.localVars) {
				Variable vv = v.applyMorphism(m, rt);
				if (vv == null) {
					newLocalVars.add(v);
				} else {
					newLocalVars.add(vv);
				}
			}
			fks.add(new ForeignKeyDef(fk.relation, fk.columns, newLocalVars));
		}

		FunctionalDependencies fds = FDs.applyMorphism(m, rt);

		return new RelationMetadata(pk, fks, h, fds, this.cardinality, this.pkSelectivity);
	}

	RelationMetadata updateWithECs(Map<? extends Variable, ? extends EquivClass> findEc) {
		FunctionalDependencies fds = FDs.updateFDsWithECs(findEc);
		fds.setFinished();

		return new RelationMetadata(updateWithEcs(primaryKey, findEc), 
				updateWithEcs(foreignKeys, findEc), updateWithEcs(histograms, findEc), fds, this.cardinality, this.pkSelectivity);
	}

	double getExpectedCardinality() {
		return cardinality;
	}

	private static double computeExpectedCardinality(Collection<Histogram<?>> histograms) {
		double card = 0.0;
		for (Histogram<?> h : histograms) {
			if (h == null) {
				continue;
			}
			double thisCard = h.getNumInRange(null, null).cardinality;
			if (thisCard > card) {
				card = thisCard;
			}
		}
		return card;
	}

	RelationMetadata applyPredicates(Set<Predicate> preds) {
		if (preds.isEmpty()) {
			return this;
		}

		
		Map<Variable,Histogram<?>> newHists = applySelectionsToHistograms(preds, histograms);
		double newCard = computeExpectedCardinality(newHists.values());
		double pkSelectivity = this.pkSelectivity * newCard / this.cardinality;
		return new RelationMetadata(primaryKey, foreignKeys, newHists, FDs, newCard, pkSelectivity);
	}
	
	RelationMetadata restrictForExpression(Expression e) {
		Map<Variable,Histogram<?>> hists = new HashMap<Variable,Histogram<?>>(histograms);
		Set<Variable> pk = primaryKey;
		Collection<ForeignKeyDef> fks = new ArrayList<ForeignKeyDef>(foreignKeys);
		FunctionalDependencies fds = FDs;

		if (pk != null && (! e.getExposedVariables().containsAll(pk))) {
			pk = null;
		}
		Iterator<ForeignKeyDef> fkIt = fks.iterator();
		while (fkIt.hasNext()) {
			ForeignKeyDef fk = fkIt.next();
			if (! e.getExposedVariables().containsAll(fk.localVars)) {
				fkIt.remove();
			}
		}

		fds = fds.restrictToVariables(e.getExposedVariables());
		fds.setFinished();

		RelationMetadata result = new RelationMetadata(pk, fks, hists, fds, this.cardinality, this.pkSelectivity);
		return result;
	}

	static private Map<Variable,Histogram<?>> applySelectionsToHistograms(Set<Predicate> preds, Map<Variable,Histogram<?>> histograms) {
		// We assume that all predicates are independent, since we don't have any multidimensional histograms

		Map<Variable,Histogram<?>> result = histograms;

		for (Predicate p : preds) {
			Map<Variable,Histogram<?>> filtered = new HashMap<Variable,Histogram<?>>(result.size());

			Type t1 = p.var1.getType(), t2 = p.var2.getType();
			if (t1.valueKnown() || t2.valueKnown()) {
				final Object val;
				final Variable col;
				final ComparePredicate.Op op;

				if (t1.valueKnown()) {
					val = t1.getConstantValue();
					col = p.var2;
					op = p.op.getReverseComparePredicateOp();
				} else {
					val = t2.getConstantValue();
					col = p.var1;
					op = p.op.getComparePredicateOp();
				}

				Histogram<?> colHist = result.get(col);
				if (colHist == null) {
					throw new IllegalArgumentException("Missing histogram for variable " + col);
				}

				final Histogram<?> newColHist;

				double numSelected;
				double currCard = colHist.getNumInRange(null, null).cardinality;
				if (op == ComparePredicate.Op.NE) {
					numSelected = currCard - getNumInRange(col, colHist, val, val);
					newColHist = removeRange(col, colHist, val, val);
				} else if (op == ComparePredicate.Op.EQ) {
					numSelected = getNumInRange(col, colHist, val, val);
					newColHist = retainRange(col, colHist, val, val);
				} else if (op == ComparePredicate.Op.LT) {
					numSelected = getNumInRange(col, colHist, null, val) - getNumInRange(col, colHist, val, val);
					newColHist = removeRange(col, colHist, val, null);
				} else if (op == ComparePredicate.Op.LE) {
					numSelected = getNumInRange(col, colHist, null, val);
					newColHist = retainRange(col, colHist, null, val);
				} else if (op == ComparePredicate.Op.GT) {
					numSelected = getNumInRange(col, colHist, val, null) - getNumInRange(col, colHist, val, val);
					newColHist = removeRange(col, colHist, null, val);
				} else if (op == ComparePredicate.Op.GE) {
					numSelected = getNumInRange(col, colHist, val, null);
					newColHist = retainRange(col, colHist, val, null);
				} else {
					throw new IllegalStateException("Don't know what to do with Op" + op);
				}

				double fracRemaining = ((double) numSelected) / currCard;
				filtered.put(col, newColHist);

				for (Map.Entry<Variable, Histogram<?>> me : result.entrySet()) {
					Variable v = me.getKey();
					if (! v.equals(col)) {
						filtered.put(v, me.getValue().scaleHistogram(fracRemaining, fracRemaining));
					}
				}
			} else {
				// TODO: Figure out a better way to estimate the
				// histograms derived from comparing two columns
				double scaleFactor;
				ComparePredicate.Op op = p.op.getComparePredicateOp();
				if (op == ComparePredicate.Op.EQ) {
					scaleFactor = 0.1;
				} else if (op == ComparePredicate.Op.NE) {
					scaleFactor = 0.9;
				} else {
					scaleFactor = 0.3;
				}
				for (Map.Entry<Variable, Histogram<?>> me : result.entrySet()) {
					filtered.put(me.getKey(), me.getValue().scaleHistogram(scaleFactor, scaleFactor));
				}
			}
			result = filtered;
		}

		return result;
	}


	static private double getNumInRange(Variable v, Histogram<?> hist, Object lower, Object upper) {
		Type t = v.getType();
		if (t instanceof CharType || t instanceof VarCharType) {
			return getStringHistogram(v, hist).getNumInRange((String) lower, (String) upper).cardinality;
		} else if (t instanceof DateType) {
			return getDateHistogram(v, hist).getNumInRange((Date) lower, (Date) upper).cardinality;
		} else if (t instanceof DoubleType) {
			return getDoubleHistogram(v, hist).getNumInRange((Double) lower, (Double) upper).cardinality;			
		} else if (t instanceof IntType) {
			return getIntegerHistogram(v, hist).getNumInRange((Integer) lower, (Integer) upper).cardinality;
		} else {
			throw new IllegalArgumentException("Don't know how to process histogram of type " + t);
		}
	}

	static private Histogram<?> removeRange(Variable v, Histogram<?> hist, Object lower, Object upper) {
		Type t = v.getType();
		if (t instanceof CharType || t instanceof VarCharType) {
			return getStringHistogram(v, hist).removeRange((String) lower, (String) upper);
		} else if (t instanceof DateType) {
			return getDateHistogram(v, hist).removeRange((Date) lower, (Date) upper);
		} else if (t instanceof DoubleType) {
			return getDoubleHistogram(v, hist).removeRange((Double) lower, (Double) upper);
		} else if (t instanceof IntType) {
			return getIntegerHistogram(v, hist).removeRange((Integer) lower, (Integer) upper);
		} else {
			throw new IllegalArgumentException("Don't know how to process histogram of type " + t);
		}
	}

	static private Histogram<?> retainRange(Variable v, Histogram<?> hist, Object lower, Object upper) {
		Type t = v.getType();
		if (t instanceof CharType || t instanceof VarCharType) {
			return getStringHistogram(v, hist).retainRange((String) lower, (String) upper);
		} else if (t instanceof DateType) {
			return getDateHistogram(v, hist).retainRange((Date) lower, (Date) upper);
		} else if (t instanceof DoubleType) {
			return getDoubleHistogram(v, hist).retainRange((Double) lower, (Double) upper);
		} else if (t instanceof IntType) {
			return getIntegerHistogram(v, hist).retainRange((Integer) lower, (Integer) upper);
		} else {
			throw new IllegalArgumentException("Don't know how to process histogram of type " + t);
		}
	}

	@SuppressWarnings("unchecked")
	static private Histogram<Date> getDateHistogram(Variable v, Histogram h) {
		if (v.getType() instanceof DateType) {
			return (Histogram<Date>) h;
		} else {
			throw new IllegalArgumentException("Can only get a date histgram from a date variable");
		}
	}

	@SuppressWarnings("unchecked")
	static private Histogram<String> getStringHistogram(Variable v, Histogram h) {
		if (v.getType() instanceof CharType || v.getType() instanceof VarCharType) {
			return (Histogram<String>) h;
		} else {
			throw new IllegalArgumentException("Can only get a string histogram from a string variable");
		}
	}

	@SuppressWarnings("unchecked")
	static private Histogram<Integer> getIntegerHistogram(Variable v, Histogram h) {
		if (v.getType() instanceof IntType) {
			return (Histogram<Integer>) h;
		} else {
			throw new IllegalArgumentException("Can only get an integer histogram from an integer variable");
		}
	}

	@SuppressWarnings("unchecked")
	static private Histogram<Double> getDoubleHistogram(Variable v, Histogram h) {
		if (v.getType() instanceof DoubleType) {
			return (Histogram<Double>) h;
		} else {
			throw new IllegalArgumentException("Can only get a double histogram from a double variable");
		}
	}

	private static Variable replaceVariables(Variable v, Map<? extends Variable,? extends EquivClass> findEc) {
		EquivClass ec = null;
		if (v instanceof EquivClass) {
			for (Variable vv : ((EquivClass) v)) {
				ec = findEc.get(vv);
				if (ec != null) {
					break;
				}
			}
		} else {
			ec = findEc.get(v);
		}
		if (ec == null) {
			return v;
		} else {
			return ec;
		}

	}

	private static Set<Variable> updateWithEcs(Collection<Variable> input, Map<? extends Variable,? extends EquivClass> findEc) {
		if (input == null) {
			return null;
		}
		Set<Variable> retval = new HashSet<Variable>(input.size());
		for (Variable v : input) {
			retval.add(replaceVariables(v, findEc));
		}
		return retval;
	}

	private static List<ForeignKeyDef> updateWithEcs(Collection<ForeignKeyDef> fks, Map<? extends Variable,? extends EquivClass> findEc) {
		List<ForeignKeyDef> newFks = new ArrayList<ForeignKeyDef>(fks.size());
		for (ForeignKeyDef fk : fks) {
			List<Variable> localVars = new ArrayList<Variable>(fk.localVars.size());
			for (Variable v : fk.localVars) {
				localVars.add(replaceVariables(v,findEc));
			}
			newFks.add(new ForeignKeyDef(fk.relation, fk.columns,localVars));
		}
		return newFks;
	}

	private static Map<Variable,Histogram<?>> updateWithEcs(Map<Variable,Histogram<?>> hists, Map<? extends Variable,? extends EquivClass> findEc) {
		Map<Variable,Histogram<?>> newHists = new HashMap<Variable,Histogram<?>>(hists.size());
		for (Map.Entry<Variable, Histogram<?>> me : hists.entrySet()) {
			Variable v = me.getKey();
			newHists.put(replaceVariables(v,findEc), me.getValue());
		}
		return newHists;
	}
	
	double getPerNodeCardinality(int numNodes, Collection<? extends Variable> hashCols) {
		double numHashKeys = 1.0;
		for (Variable v : hashCols) {
			Histogram<?> h = histograms.get(v);
			if (h == null) {
				if (! v.getType().valueKnown()) {
					// Just guess
					numHashKeys = 10 * numNodes;
				}
			} else {
				numHashKeys *= h.getNumInRange(null, null).distinctValues;
			}
		}
		if (numHashKeys < 1.0) {
			numHashKeys = 1.0;
		}
		if (numHashKeys < numNodes) {
			return this.cardinality / numHashKeys;
		} else {
			return this.cardinality / numNodes;
		}
	}

}
