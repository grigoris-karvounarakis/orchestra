package edu.upenn.cis.orchestra.optimization;


public class IntType extends Type {
	public static IntType create(boolean nullable, boolean labeledNullable) {
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
	
	private static final IntType NOT_NULLABLE = new IntType(false, false);
	private static final IntType ONLY_REGULAR_NULL = new IntType(true, false);
	private static final IntType ONLY_LABELED_NULL = new IntType(false, true);
	private static final IntType FULLY_NULLABLE = new IntType(true,true);
	
	private IntType(boolean nullable, boolean labeledNullable) {
		super(nullable,labeledNullable);
	}
	
	IntType(int constantValue) {
		super(constantValue);
	}
	
	@Override
	int getExpectedSize() {
		return Integer.SIZE / Byte.SIZE;
	}

	@Override
	int getMaximumSize() {
		return Integer.SIZE / Byte.SIZE;
	}

	public String toString() {
		return "INT";
	}

	public int compareTo(Type t) {
		return getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
	}

	@Override
	Object convertLit(Object lit, Type to) throws TypeError {
		if (!(lit instanceof Integer)) {
			throw new IllegalArgumentException("Expected integer literal");
		}
		if (to.equals(this)) {
			return lit;
		} else if (to instanceof DoubleType && (! to.valueKnown())) {
			int value = (Integer) lit;
			return new Double(value);
		} else {
			throw new TypeError("Cannot convert from " + this + " to " + to);
		}
	}

	@Override
	public edu.upenn.cis.orchestra.datamodel.IntType getExecutionType() {
		return new edu.upenn.cis.orchestra.datamodel.IntType(nullable, labeledNullable);
	}
	
	IntType setConstantValue(Type t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new IntType((Integer) newVal);
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

