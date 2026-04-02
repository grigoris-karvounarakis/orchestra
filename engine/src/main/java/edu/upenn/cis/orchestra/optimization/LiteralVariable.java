package edu.upenn.cis.orchestra.optimization;

import java.util.Collection;
import edu.upenn.cis.orchestra.datamodel.Date;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.util.Bijection;

class LiteralVariable extends Variable {
	final Object lit;

	final Type t;
	final int hashCode;
	
	LiteralVariable(Object lit) {
		this.lit = lit;
		t = getLiteralType(lit);
		hashCode = lit.hashCode();
		if (getType() == null) {
			throw new IllegalArgumentException("Cannot determine optimizer type of literal of type " + lit.getClass().getName());
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		LiteralVariable lv = (LiteralVariable) o;

		return (lit.getClass() == lv.lit.getClass() && lit.equals(lv.lit));
	}

	/**
	 * Determine the optimizer type (i.e. the subclass of <code>Type</code>)
	 * that corresponds to the supplied Java object
	 * 
	 * @param lit		The object of which to determine the type
	 * @return			It's type, or <code>null</code> if it cannot be determined
	 */
	static Type getLiteralType(Object lit) {
		if (lit instanceof Integer) {
			return new IntType((Integer) lit);
		} else if (lit instanceof Double) {
			return new DoubleType((Double) lit);
		} else if (lit instanceof Date) {
			return new DateType((Date) lit);
		} else if (lit instanceof String) {
			return new CharType((String) lit);
		} else {
			return null;
		}
	}

	public String toString() {
		if (lit instanceof String) {
			String s = (String) lit;
			return "'" + s.replace("'", "''") + "'";
		} else {
			return lit.toString();
		}
	}

	@Override
	LiteralVariable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) {
		return null;
	}

	@Override
	LiteralVariable replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) {
		// Literal variable has no children
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
		return true;
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
		return false;
	}
	
	Integer getPosForVariable(Bijection<? extends Variable,Integer> findVarPos) {
		throw new IllegalStateException("Cannot find position for literal variable");
	}

	void getInputVariables(Collection<Variable> vs) {
	}

}
