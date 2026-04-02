package edu.upenn.cis.orchestra.optimization;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author netaylor
 *
 */
public abstract class Variable {
	abstract public boolean equals(Object o);
	abstract public String toString();

	/**
	 * Shuffle the names of the atom variables referred to by this variable
	 * and return the newly created variable
	 * 
	 * @param mapping			The mapping of relation occurrences
	 * @param rt				Relation type information
	 * @return					A variable with the same form as the current variable
	 * 							but with the atom variables swapped as specified, or
	 * 							null if the variable has not changed
	 */
	abstract Variable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping;

	/**
	 * Replace all instances of variables with instances of an equiv class,
	 * if it is in one. Also tries to replace itself with an instance of an equiv class.
	 * 
	 * @param mapping
	 * @param throughEC	If <code>true</code>, continue replacing variables inside an equivalence class.
	 * 					If <code>false</code>, do not continue through an equivalence class.
	 * @return			The new variable, if it has changed, or <code>null</code> if
	 * 					it has not
	 */
	final Variable replaceVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) throws VariableRemoved {
		Variable v = replaceChildVariable(mapping, throughEC);
		if (v == null) {
			Variable vv = mapping.get(this);
			if (vv == null && mapping.containsKey(this)) {
				throw new VariableRemoved(this);
			}
			if (v == vv) {
				// Refer to same object in memory
				return null;
			} else {
				return vv;
			}
		} else {
			// No need to check for pointer equality, since child
			// variable has definitely changed
			Variable vv = mapping.get(v);
			if (vv == null) {
				return v;
			} else {
				return vv;
			}
		}
	}
	
	/**
	 * Replace all instances of variables with instances of an equiv class,
	 * if it is in one. Does not try to replace itself with an instance of an equiv class.
	 * 
	 * @param mapping
	 * @param throughEC	If <code>true</code>, continue replacing variables inside an equivalence class.
	 * 					If <code>false</code>, do not continue through an equivalence class.
	 * @return			The new variable, if it has changed, or <code>null</code> if
	 * 					it has not
	 */
	abstract Variable replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) throws VariableRemoved;

	public abstract Type getType();

	/**
	 * Determine if a variable can be computed after aggregation is performed
	 * 
	 * @return <code>true</code> if a variable can be computed after aggregation, <code>false</code> if it cannot
	 */
	abstract boolean isAggregatedComputable(Set<Variable> groupingVariables);
	
	/**
	 * Determine if a variable can be computed before aggregation is performed
	 * 
	 * @return <code>true</code> if a variable can be computed before aggregation, <code>false</code> if it cannot
	 */
	abstract boolean isNonAggregatedComputable(Set<Variable> groupingVariables);
	
	/**
	 * Get the equivalence classes referred to by this variable.
	 * 
	 * @param ecsUsed			The set to add the equivalence classes to
	 */
	abstract void getEquivClassesUsed(Set<EquivClass> ecsUsed);
	
	abstract public int hashCode();
	
	/**
	 * Determine if this variable refers to the specified variable
	 * 
	 * @param v			The variable to check for
	 * @return			<code>true</code> if this variable refers directly or
	 * 					indirectly to <code>v</code>, <code>false</code> otherwise
	 */
	abstract boolean usesVariable(Variable v);
	
	abstract void getInputVariables(Collection<Variable> vs);
}

class VariableRemoved extends Exception {
	private static final long serialVersionUID = 1L;
	Variable v;
	
	VariableRemoved(Variable v) {
		super("Variable " + v + " explicitly removed by mapping");
		this.v = v;
	}
}

class VariableNotInMapping extends RuntimeException {
	private static final long serialVersionUID = 1L;

	VariableNotInMapping(Variable v, Morphism m) {
		super("Mapping not found for " + v + " in " + m);
	}
	
	VariableNotInMapping(AtomVariable av, Morphism m) {
		this(av.relation, av.occurrence, m);
	}
	
	VariableNotInMapping(String relation, int occurrence, Morphism m) {
		super("Mapping not found for " + relation + "[" + occurrence + "] in mapping " + m);
	}
}