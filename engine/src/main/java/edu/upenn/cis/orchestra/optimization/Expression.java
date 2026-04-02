package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.util.ReadOnlyMap;
import edu.upenn.cis.orchestra.util.ReadOnlySet;

public class Expression {
	// Relational atoms to be joined together
	// Mapping from relation name to number of occurrences (>= 1)
	public final Map<String,Integer> relAtoms;


	// Sets of variables that are required to be equal
	public final Set<EquivClass> equivClasses;
	public final Map<Variable,EquivClass> findEquivClass;

	// Grouping variables, if applicable
	// null for not grouped, empty for one group for the entire expression
	public final Set<Variable> groupBy;

	// Predicates over query variables.  Does not contain any equality predicates,
	// those are stored using equivalence classes (which has the advantage of being
	// a canonical representation).
	public final Set<Predicate> predicates;

	// Set of all functions computed by this expression
	public final Set<Function> functions;
	// Set of all aggregates computed by this expression
	public final Set<Aggregate> aggregates;

	// Set of all variables not projected away 
	public final Set<Variable> head;

	// Since everything is immutable, we can precompute the hash code
	private final int hashcode;

	Expression(Set<Variable> head, Map<String,Integer> relAtoms,
			Set<EquivClass> equivClasses, Set<Variable> groupBy, Set<Predicate> predicates,
			Set<Function> functions, Set<Aggregate> aggregates) {
		for (EquivClass ec : equivClasses) {
			ec.setFinished();
		}
		this.head = ReadOnlySet.create(head);
		this.relAtoms = ReadOnlyMap.create(relAtoms);
		this.equivClasses = ReadOnlySet.create(equivClasses);
		if (groupBy == null) {
			this.groupBy = null;
		} else {
			this.groupBy = ReadOnlySet.create(groupBy);
		}
		this.predicates = ReadOnlySet.create(predicates);
		this.functions = ReadOnlySet.create(functions);
		this.aggregates = ReadOnlySet.create(aggregates);
		hashcode = relAtoms.hashCode() + 19 * equivClasses.hashCode() + 37 * (groupBy == null ? 0 : groupBy.hashCode())
		+ 61 * predicates.hashCode() + 127 * functions.hashCode() + 257 * aggregates.hashCode()
		+ 521 * head.hashCode();

		Map<Variable,EquivClass> findEquivClass = new HashMap<Variable,EquivClass>();
		for (EquivClass ec : equivClasses) {
			for (Variable v : ec) {
				findEquivClass.put(v, ec);
			}
		}

		for (Predicate p : predicates) {
			if (p.op == Predicate.Op.EQ) {
				throw new IllegalArgumentException("Equality in an expression should go into equiv class");
			}
		}

		for (Variable v : head) {
			if (v == null) {
				throw new NullPointerException("Should not have null variable in head");
			}
			if ((v instanceof EquivClass) && (! equivClasses.contains(v))) {
				throw new IllegalArgumentException("Equiv class " + v + " from head is not in supplied set of equiv classes");
			} else if ((! (v instanceof EquivClass) && findEquivClass.get(v) != null)) {
				throw new IllegalArgumentException("Variable " + v + " is in an equiv class but appears by itself in the head");
			} else if ((v instanceof Function) && (! functions.contains(v))) {
				throw new IllegalArgumentException("Function " + v + " is in the head but not in the supplied set of functions");
			} else if ((v instanceof Aggregate) && (! aggregates.contains(v))) {
				throw new IllegalArgumentException("Aggregate " + v + " in the head is not in the supplied set of aggregates");
			} else if (v instanceof LiteralVariable) {
				throw new IllegalArgumentException("Literal " + v + " found in head");
			} else if (groupBy != null && (! v.isAggregatedComputable(groupBy))) {
				throw new IllegalArgumentException("Variable " + v + " cannot be computed after aggregation but is in the head");
			}
		}

		if (equivClasses.contains(null)) {
			throw new NullPointerException("Should not have null equiv class");
		}

		if (groupBy != null && groupBy.contains(null)) {
			throw new NullPointerException("Should not have null grouping variable");
		}

		if (predicates.contains(null)) {
			throw new NullPointerException("Should not have null predicate");
		}

		if (functions.contains(null)) {
			throw new NullPointerException("Should not have null function");
		}

		if (aggregates.contains(null)) {
			throw new NullPointerException("Should not have null aggregate");
		}

		if ((! aggregates.isEmpty()) && groupBy == null) {
			throw new IllegalArgumentException("If aggregates is non-empty, groupBy must be non-null");
		}

		for (Function f : functions) {
			for (Variable v : f.getInputVariables()) {
				if ((! (v instanceof EquivClass)) && findEquivClass.containsKey(v)) {
					throw new IllegalArgumentException("Variable " + v + " is in equiv class but appears by itself in function " + f);
				}
			}
		}

		for (Aggregate a : aggregates) {
			if (a.hasInputVariable()) {
				Variable v = a.getInputVariable();
				if ((! (v instanceof EquivClass)) && findEquivClass.containsKey(v)) {
					throw new IllegalArgumentException("Variable " + v + " is in equiv class but appears by itself in aggregate " + a);
				}
			}
		}

		this.findEquivClass = ReadOnlyMap.create(findEquivClass);

	}



	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}

		Expression e = (Expression) o;

		if (hashcode != e.hashcode) {
			return false;
		}

		if (groupBy == null) {
			if (e.groupBy != null) {
				return false;
			}
		} else {
			if (e.groupBy == null || (! groupBy.equals(e.groupBy))) {
				return false;
			}
		}

		return (relAtoms.equals(e.relAtoms) && equivClasses.equals(e.equivClasses) &&
				predicates.equals(e.predicates) &&
				functions.equals(e.functions) &&
				aggregates.equals(e.aggregates) && head.equals(e.head));
	}

	public int hashCode() {
		return hashcode;
	}

	/**
	 * Apply the supplied mapping to this expression and return the result as a new expression.
	 * This expression is not modified. If a query head is supplied, the same mapping is applied
	 * to the head.
	 * 
	 * @param mapping				The mapping to apply
	 * @param rt					The types of the relations in this expression
	 * @return						The new expression that results from applying the mapping
	 * @throws VariableNotInMapping
	 */
	Expression applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping {
		if (mapping == null) {
			return this;
		}
		Map<String,Integer> newRelAtoms = new HashMap<String,Integer>(relAtoms);
		Set<Predicate> newPredicates = new HashSet<Predicate>(predicates.size());
		Set<EquivClass> newEquivClasses = new HashSet<EquivClass>(equivClasses.size());
		Set<Function> newFunctions = new HashSet<Function>(functions.size());
		Set<Aggregate> newAggregates = new HashSet<Aggregate>(aggregates.size());
		Set<Variable> newHead = new HashSet<Variable>(head.size());

		for (Variable v : head) {
			Variable newV = v.applyMorphism(mapping, rt);
			if (newV != null) {
				newHead.add(newV);
			} else {
				newHead.add(v);
			}
		}

		Set<Variable> newGroupBy = null;
		if (groupBy != null) {
			newGroupBy = new HashSet<Variable>(groupBy.size());
			for (Variable v : groupBy) {
				Variable newV = v.applyMorphism(mapping, rt);
				if (newV != null) {
					newGroupBy.add(newV);
				} else {
					newGroupBy.add(v);
				}
			}
		}

		for (Function f : functions) {
			Variable newF = f.applyMorphism(mapping, rt);
			if (newF != null) {
				if (newF instanceof Function) {
					newFunctions.add((Function) newF);
				} else {
					throw new RuntimeException("Function became constant");
				}
			} else {
				newFunctions.add(f);
			}
		}

		for (Aggregate a : aggregates) {
			Aggregate newA = a.applyMorphism(mapping, rt);
			if (newA != null) {
				newAggregates.add(newA);
			} else {
				newAggregates.add(a);
			}
		}

		for (Predicate p : predicates) {
			Predicate newP = p.renameVariables(mapping, rt);
			if (newP != null) {
				newPredicates.add(newP);
			} else {
				newPredicates.add(p);
			}
		}

		for (EquivClass ec : equivClasses) {
			EquivClass newEc = new EquivClass();
			boolean changed = false;
			for (Variable v : ec) {
				Variable newV = v.applyMorphism(mapping,rt);
				try {
					if (newV != null) {
						changed = true;
						newEc.add(newV);
					} else {
						newEc.add(v);
					}
				} catch (TypeError te) {
					throw new RuntimeException("Shouldn't get type error when renaming variables");
				}
			}
			if (changed) {
				newEc.setFinished();
				newEquivClasses.add(newEc);
			} else {
				newEquivClasses.add(ec);
			}
		}

		return new Expression(newHead, newRelAtoms, newEquivClasses, newGroupBy, newPredicates,
				newFunctions, newAggregates);

	}

	List<Morphism> computePermutationMappings() {
		// If there are no repeated atoms, we can just return a single null,
		// which implicitly represents an identity mapping
		boolean canReturnIdentity = true;
		for (int val : relAtoms.values()) {
			if (val != 1) {
				canReturnIdentity = false;
				break;
			}
		}

		if (canReturnIdentity) {
			return Collections.singletonList(null);
		}

		List<List<Morphism>> permutationsForRelations = new ArrayList<List<Morphism>>();

		// Total number if occurrences of possibly non-identity mappings,
		// used to estimate the size of each morphism
		int totalOcc = 0;

		// Create all permutations for each relation
		for (Map.Entry<String, Integer> entry : relAtoms.entrySet()) {
			final String relName = entry.getKey();
			final int relOcc = entry.getValue();

			List<Morphism> mappings = new ArrayList<Morphism>();
			permutationsForRelations.add(mappings);
			totalOcc += relOcc;
			PermutationGenerator pg = new PermutationGenerator(relOcc);


			while (pg.hasMore()) {
				int[] permutation = pg.getNext();
				Morphism mapping = new Morphism(relOcc);
				mappings.add(mapping);
				for (int i = 0; i < permutation.length; ++i) {
					mapping.mapOccurrence(relName, permutation[i]+1, i+1);
				}
			}
		}

		// Combine the permutations for the relations into permutations for
		// all relations

		// Current position in the permutations for each relation. We treat this
		// as a number (where each digit is at most the number of permutations for
		// that relation) and repeatedly increment the number until we overflow
		int relPos[] = new int[permutationsForRelations.size()];

		List<Morphism> retval = new ArrayList<Morphism>();


		MAIN: for ( ; ; ) {
			Morphism mapping = new Morphism(totalOcc);
			for (int i = 0; i < relPos.length; ++i) {
				mapping.addMappingsFromMorphism(permutationsForRelations.get(i).get(relPos[i]));
			}
			retval.add(mapping);

			// The position in relPos that we're about to increment
			int finger = relPos.length - 1;

			while (relPos[finger] == (permutationsForRelations.get(finger).size() - 1)) {
				--finger;
				if (finger < 0) {
					// We're done
					break MAIN;
				}
			}

			++relPos[finger];
			for (int i = finger + 1; i < relPos.length; ++i) {
				relPos[i] = 0;
			}
		}


		return retval;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("Head: " + head);
		sb.append(" Rel. atoms: " + relAtoms);
		if (! equivClasses.isEmpty()) {
			sb.append(" Equiv. classes: " + equivClasses);
		}
		if (groupBy != null) {
			sb.append(" Group by: " + groupBy);
		}
		if (! predicates.isEmpty()) {
			sb.append(" Predicates: " + predicates);
		}
		if (! functions.isEmpty()) {
			sb.append(" Functions: " + functions);
		}
		if (! aggregates.isEmpty()) {
			sb.append(" Aggregates: " + aggregates);
		}
		return sb.toString();
	}

	boolean isScan() {
		if (relAtoms.size() > 1) {
			return false;
		}

		if (groupBy != null) {
			return false;
		}

		for (Function f : functions) {
			if (! f.getType().valueKnown()) {
				return false;
			}
		}

		// Easiest to iterate even though we know there's only going to be one relation (since since size is at most 1)
		for (int count : relAtoms.values()) {
			if (count > 1) {
				return false;
			}
		}

		return true;
	}

	public Set<Variable> getExposedVariables() {
		return head;
	}
}
