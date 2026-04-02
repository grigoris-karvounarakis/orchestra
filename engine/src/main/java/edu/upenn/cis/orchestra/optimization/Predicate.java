package edu.upenn.cis.orchestra.optimization;

import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;

class Predicate {
	Predicate(Variable var1, Op op, Variable var2) throws TypeError {
		this.var1 = var1;
		this.var2 = var2;
		this.op = op;

		// Typecheck the predicates

		Type t1 = var1.getType();
		if (t1 == null) {
			throw new TypeError("Couldn't determine type for variable " + var1 + " when typechecking " + this);
		}
		Type t2 = var2.getType();
		if (t2 == null) {
			throw new TypeError("Couldn't determine type for variable " + var2 + " when typechecking " + this);
		}

		switch (op) {
		case EQ:
		case NE:
		case LT:
		case LE:
			if (t1 instanceof DateType) {
				if (!(t2 instanceof DateType)) {
					throw new TypeError("Can only compare a date to another date");
				}
			} else if (t1 instanceof IntType) {
				if (! (t2 instanceof IntType)) {
					throw new TypeError("Can only compare an integer to an integer");
				}
			} else if (t1 instanceof DoubleType) {
				if (! (t2 instanceof DoubleType)) {
					throw new TypeError("Can only compare a double to a double");
				}
			} else if (t1 instanceof CharType) {
				if (! (t2 instanceof CharType || t2 instanceof VarCharType)) {
					throw new TypeError("Can only compare a char to a char or a varchar");
				}
			} else if (t1 instanceof VarCharType) {
				if (! (t2 instanceof CharType || t2 instanceof VarCharType)) {
					throw new TypeError("Can only compare a varchar to a char or a varchar");
				}
			}
		}

		// Need to get same hash value if order reversed for = or !=
		hashCode = op.hashCode() + 37 * (var1.hashCode() + var2.hashCode());

	}
	// Don't want to support GT or GE because then there are multiple
	// ways of expressing a given predicate
	enum Op {
		LT(ComparePredicate.Op.LT, ComparePredicate.Op.GT),
		LE(ComparePredicate.Op.LE, ComparePredicate.Op.GE),
		NE(ComparePredicate.Op.NE, ComparePredicate.Op.NE),
		EQ(ComparePredicate.Op.EQ, ComparePredicate.Op.EQ);

		private final ComparePredicate.Op op, reverseOp;

		Op(ComparePredicate.Op op, ComparePredicate.Op reverseOp) {
			this.op = op;
			this.reverseOp = reverseOp;
		}

		ComparePredicate.Op getComparePredicateOp() {
			return op;
		}

		ComparePredicate.Op getReverseComparePredicateOp() {
			return reverseOp;
		}
	}
	// The first variable in the predicate
	final Variable var1;
	// The second variable in the predicate
	final Variable var2;
	// The comparison function that must hold, i.e. var1 op var2 is true
	// or var1 op lit is true
	final Op op;

	// Precompute the hash value for quick hashing
	final private int hashCode;

	public String toString() {
		return var1 + " " + op + " " + var2;
	}

	public int hashCode() {
		return hashCode;
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Predicate p = (Predicate) o;
		if (op != p.op) {
			return false;
		}
		if (op == Op.EQ || op == Op.NE) {
			// Allow for commutativity
			return ((var1.equals(p.var1) && var2.equals(p.var2)) ||
					(var1.equals(p.var2) && var2.equals(p.var1)));

		} else {
			return (var1.equals(p.var1) && var2.equals(p.var2));
		}
	}

	Predicate renameVariables(Morphism mapping, RelationTypes<?,?> rt) throws VariableNotInMapping {
		Variable v1 = var1.applyMorphism(mapping, rt);
		Variable v2 = var2.applyMorphism(mapping, rt);
		if (v1 == null && v2 == null) {
			return null;
		} else {
			try {
				return new Predicate(v1 == null ? var1 : v1, op, v2 == null ? var2 : v2);
			} catch (TypeError e) {
				throw new RuntimeException("Shouldn't get type error when renaming variables");
			}
		}
	}

	Predicate replaceVariable(Map<? extends Variable,? extends Variable> mapping, boolean throughEC) throws VariableRemoved {
		Variable v1 = var1.replaceVariable(mapping, throughEC);
		Variable v2 = var2.replaceVariable(mapping, throughEC);
		if (v1 == null && v2 == null) {
			return null;
		} else {
			try {
				return new Predicate(v1 == null ? var1 : v1, op, v2 == null ? var2 : v2);
			} catch (Type.TypeError te) {
				throw new RuntimeException("Shouldn't cause type error by rewriting predicate", te);
			}
		}
	}

	void getEquivClassesUsed(Set<EquivClass> ecsUsed) {
		var1.getEquivClassesUsed(ecsUsed);
		var2.getEquivClassesUsed(ecsUsed);
	}

	boolean isAggregatedComputable(Set<Variable> groupingVariables) {
		return var1.isAggregatedComputable(groupingVariables) && var2.isAggregatedComputable(groupingVariables);
	}

	boolean isNonAggregatedComputable(Set<Variable> groupingVariables) {
		return var1.isNonAggregatedComputable(groupingVariables) && var2.isNonAggregatedComputable(groupingVariables);
	}

	boolean implies(Predicate p) {
		if (op == Op.EQ || op == Op.NE) {
			return false;
		}

		try {
			if (var1.equals(p.var1)) {
				if (!(var2.getType().valueKnown() && p.var2.getType().valueKnown())) {
					return false;
				}
				edu.upenn.cis.orchestra.datamodel.Type t = var2.getType().getExecutionType();
				int comp = t.compareTwo(var2.getType().getConstantValue(), p.var2.getType().getConstantValue());
				if (p.op == Op.LE) {
					// op is LT or LE
					return comp <= 0;
				} else if (op == Op.LT) {
					// p.op is LT
					return comp <= 0;
				} else {
					// p.op is LT, op is LE
					return comp < 0;
				}
			} else if (var2.equals(p.var2)) {
				if (!(var1.getType().valueKnown() && p.var1.getType().valueKnown())) {
					return false;
				}
				edu.upenn.cis.orchestra.datamodel.Type t = var1.getType().getExecutionType();
				int comp = t.compareTwo(var2.getType().getConstantValue(), p.var2.getType().getConstantValue());
				if (p.op == Op.LE) {
					return comp >= 0;
				} else if (op == Op.LT) {
					// p.op is LT
					return comp >= 0;
				} else {
					// p.op is LT, op is LE
					return comp < 0;
				}
			} else {
				return false;
			}
		} catch (CompareMismatch cm) {
			throw new RuntimeException(cm);
		}
	}
}

