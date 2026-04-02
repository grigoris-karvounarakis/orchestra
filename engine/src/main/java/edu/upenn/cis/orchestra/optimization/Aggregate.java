package edu.upenn.cis.orchestra.optimization;

import edu.upenn.cis.orchestra.optimization.Type.TypeError;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class Aggregate extends Variable {

	/**
	 * Create an aggregate function
	 * 
	 * @param aggFunc			The name of the aggregate function
	 * @param inputVariable		The variable to aggregate over
	 * @return					The aggregate function
	 * @throws IllegalArgumentException
	 * 							If the input variable is itself an aggregate
	 * 							variable, or if the aggregate function is unknown
	 */
	static Aggregate makeAggregate(String aggFunc, Variable inputVariable) throws TypeError {
		AggFunc af;
		try {
			af = AggFunc.valueOf(aggFunc.toUpperCase());
		} catch (IllegalArgumentException iae) {
			throw new IllegalArgumentException("Aggregate function " + aggFunc + " is not known");
		}
		return new Aggregate(af, inputVariable);
	}

	/**
	 * Create an aggregate function that takes no inputs
	 * 
	 * @param aggFunc			The name of the aggregate function
	 * @return					The aggregate function, or <code>null</code>
	 * 							if the aggregate function is not known
	 * @throws IllegalArgumentException
	 * 							If the aggregate function is unknown
	 */
	static Aggregate makeAggregate(String aggFunc) throws TypeError {
		AggFunc af;
		try {
			af = AggFunc.valueOf(aggFunc.toUpperCase());
		} catch (IllegalArgumentException iae) {
			return null;
		}
		return new Aggregate(af, null);
	}

	Aggregate(AggFunc aggFunc, Variable inputVariable) throws TypeError {
		this(aggFunc, inputVariable, false);
	}

	Aggregate(AggFunc aggFunc, Variable inputVariable, boolean allowInputAggregates) throws TypeError {
		if ((! allowInputAggregates) && (inputVariable instanceof Aggregate)) {
			throw new IllegalArgumentException("Cannot use an aggregate variable as input to an aggregate function");
		}

		this.aggFunc = aggFunc;
		this.inputVariable = inputVariable;

		if (inputVariable == null && (! aggFunc.canBeNullary)) {
			throw new IllegalArgumentException("Aggregate function " + aggFunc + " cannot take no inputs");
		}

		Type inputType = inputVariable == null ? null : inputVariable.getType();

		switch (aggFunc) {
		case COUNT:
			t = IntType.create(false, false);
			break;
		case SUM:
			if (!((inputType instanceof IntType) || (inputType instanceof DoubleType))) {
				throw new TypeError("Can only SUM integers or doubles");
			}
		default:
		case MAX:
		case MIN:
			t = inputType.getWithLabelledNullable(false);
		}

		if (inputVariable == null) {
			hashCode = aggFunc.hashCode();
		} else {
			hashCode = aggFunc.hashCode() + 37 * inputVariable.hashCode();
		}
	}

	// Precompute the hash value and type
	final private int hashCode;
	final private Type t;

	public Type getType() {
		return t;
	}

	/**
	 * @author netaylor
	 *
	 */
	public enum AggFunc { 
		MAX (true, false), 
		MIN (true, false), 
		COUNT (false, true), 
		SUM (true, false),
		AVG(false, false);

		/*
		 * Is it true that f(f(A) U f(B)) = f(A U B)?
		 */
		public final boolean composable;
		/*
		 * Can this function operate over no input columns? 
		 */
		public final boolean canBeNullary;

		AggFunc(boolean composable, boolean canBeNullary) {
			this.composable = composable;
			this.canBeNullary = canBeNullary;
		}



	}
	// Aggregate function
	final AggFunc aggFunc;
	// Variable to evaluate aggregate over
	private final Variable inputVariable;

	public String toString() {
		if (inputVariable == null) {
			return aggFunc + "(*)";
		} else {
			return aggFunc + "(" + inputVariable + ")";
		}
	}

	public Variable getInputVariable() {
		if (inputVariable == null) {
			throw new IllegalStateException("Aggregate has no input variable");
		}

		return inputVariable;
	}

	public boolean hasInputVariable() {
		return inputVariable != null;
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}

		Aggregate a = (Aggregate) o;
		if (aggFunc != a.aggFunc) {
			return false;
		}
		if (this.inputVariable == null) {
			return a.inputVariable == null;
		} else {
			return this.inputVariable.equals(a.inputVariable);
		}
	}

	public int hashCode() {
		return hashCode;
	}

	Aggregate applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping {
		if (mapping == null || inputVariable == null) {
			return null;
		}
		Variable v = inputVariable.applyMorphism(mapping, rt);
		if (v == null) {
			return null;
		} else {
			try {
				return new Aggregate(aggFunc, v);
			} catch (TypeError te) {
				throw new RuntimeException("Shouldn't get type error when renaming variables");
			}
		}
	}

	Aggregate replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) throws VariableRemoved {
		if (inputVariable == null) {
			return null;
		}
		Variable v = inputVariable.replaceVariable(mapping, throughEC);
		if (v == null) {
			return null;
		}
		try {
			return new Aggregate(aggFunc, v);
		} catch (Type.TypeError te) {
			throw new RuntimeException("Shouldn't cause type error by expanding aggregate", te);
		}
	}

	@Override
	boolean isAggregatedComputable(Set<Variable> groupingVariables) {
		return true;
	}

	@Override
	boolean isNonAggregatedComputable(Set<Variable> groupingVariables) {
		return false;
	}

	@Override
	void getEquivClassesUsed(Set<EquivClass> ecsUsed) {
		if (inputVariable != null) {
			inputVariable.getEquivClassesUsed(ecsUsed);
		}
	}

	@Override
	boolean usesVariable(Variable v) {
		if (inputVariable != null) {
			return inputVariable.usesVariable(v);
		} else {
			return false;
		}
	}

	void getInputVariables(Collection<Variable> vs) {
		if (inputVariable != null) {
			vs.add(inputVariable);
			inputVariable.getInputVariables(vs);
		}
	}
}

