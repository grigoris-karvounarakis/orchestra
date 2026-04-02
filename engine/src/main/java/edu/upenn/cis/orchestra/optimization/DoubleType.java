package edu.upenn.cis.orchestra.optimization;


public class DoubleType extends Type {
	public static DoubleType create(boolean nullable, boolean labeledNullable) {
		if (nullable) {
			if (labeledNullable) {
				return FULLY_NULLABLE;
			} else {
				return ONLY_REGULAR_NULL;
			}
		} else if (labeledNullable) {
			return ONLY_LABELED_NULL;
		} else {
			return NOT_NULLABLE;
		}
	}
	
	private static final DoubleType NOT_NULLABLE = new DoubleType(false, false);
	private static final DoubleType ONLY_REGULAR_NULL = new DoubleType(true, false);
	private static final DoubleType ONLY_LABELED_NULL = new DoubleType(false, true);
	private static final DoubleType FULLY_NULLABLE = new DoubleType(true,true);
	
	private DoubleType(boolean nullable, boolean labeledNullable) {
		super(nullable,labeledNullable);
	}
	
	public DoubleType(double constantValue) {
		super(constantValue);
	}

	@Override
	int getExpectedSize() {
		return Double.SIZE / Byte.SIZE;
	}

	@Override
	int getMaximumSize() {
		return Double.SIZE / Byte.SIZE;
	}
	
	public String toString() {
		return "DOUBLE";
	}

	public int compareTo(Type t) {
		return getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
	}

	@Override
	Object convertLit(Object lit, Type to) throws TypeError {
		if (!(lit instanceof Double)) {
			throw new IllegalArgumentException("Expected double literal");
		}
		if (to.equals(this)) {
			return lit;
		} else if (to instanceof IntType && (! to.valueKnown())) {
			double value = (Double) lit;
			return new Integer((int) value);
		} else {
			throw new TypeError("Cannot convert from " + this + " to " + to);			
		}
	}
	
	public edu.upenn.cis.orchestra.datamodel.DoubleType getExecutionType() {
		return new edu.upenn.cis.orchestra.datamodel.DoubleType(nullable, labeledNullable);
	}

	DoubleType setConstantValue(Type t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new DoubleType((Double) newVal);
	}

	@Override
	Type getWithLabelledNullable(boolean newLabeledNullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(this.nullable, newLabeledNullable);
	}

	@Override
	Type getWithNullable(boolean nullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(nullable, this.labeledNullable);
	}
}

