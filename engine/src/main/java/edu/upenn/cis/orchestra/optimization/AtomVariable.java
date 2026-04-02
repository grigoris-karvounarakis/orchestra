package edu.upenn.cis.orchestra.optimization;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class AtomVariable extends Variable {
	final String relation;
	final int occurrence;
	final int position;
	final Type t;
	final int hashCode;

	/**
	 * Create a new variable to hold a value from a relational atom
	 * 
	 * @param relation			The name of the relation
	 * @param occurrence		The number of the occurrence of that relation,
	 * 							starting at 1
	 * @param position			The position of the value within that relation,
	 * 							starting at 0
	 * @param rt				A way to determine the types of relation columns
	 * @throws RTException		if an unknown relation or column is requested
	 */
	AtomVariable(String relation, int occurrence, int position, RelationTypes<?,?> rt) {
		this(relation,occurrence,position,rt.getColumnType(relation, position));
	}

	public AtomVariable(String relation, int occurrence, int position, Type t) {
		this.relation = relation;
		this.occurrence = occurrence;
		this.position = position;
		this.t = t;
		this.hashCode = computeHashCode(relation, occurrence, position);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}

		AtomVariable av = (AtomVariable) o;
		return (av.relation.equals(relation) && av.occurrence == occurrence &&
				av.position == position);

	}

	static private int computeHashCode(String relation, int occurrence, int position) {
		return relation.hashCode() + 37 * (occurrence + 37 * position);
	}

	@Override
	public String toString() {
		return relation + "[" + occurrence + "," + position + "]";
	}

	@Override
	AtomVariable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping {
		if (mapping == null) {
			return null;
		}
		AtomVariable av = mapping.mapAtomVariable(this, rt);
		if (! av.equals(this)) {
			return av;
		} else {
			return null;
		}
	}

	@Override
	AtomVariable replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) {
		// Atom variables have no children
		return null;
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}
	
	@Override
	public Type getType() {
		return t;
	}

	@Override
	boolean isAggregatedComputable(Set<Variable> groupingVariables) {
		return groupingVariables.contains(this);
	}

	@Override
	boolean isNonAggregatedComputable(Set<Variable> groupingVariables) {
			return true;
	}

	@Override
	void getEquivClassesUsed(Set<EquivClass> ecsUsed) {
	}

	@Override
	boolean usesVariable(Variable v) {
		return v.equals(this);
	}

	void getInputVariables(Collection<Variable> vs) {
	}
	
}
