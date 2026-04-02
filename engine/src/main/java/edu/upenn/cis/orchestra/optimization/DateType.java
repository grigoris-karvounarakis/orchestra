package edu.upenn.cis.orchestra.optimization;


import edu.upenn.cis.orchestra.datamodel.Date;

public class DateType extends Type {
	public static DateType create(boolean nullable, boolean labeledNullable) {
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
	
	private static final DateType NOT_NULLABLE = new DateType(false, false);
	private static final DateType ONLY_REGULAR_NULL = new DateType(true, false);
	private static final DateType ONLY_LABELED_NULL = new DateType(false, true);
	private static final DateType FULLY_NULLABLE = new DateType(true,true);

	private DateType(boolean nullable, boolean labeledNullable) {
		super(nullable,labeledNullable);
	}
	
	public DateType(Date constantValue) {
		super(constantValue);
	}

	@Override
	int getExpectedSize() {
		return Date.bytesPerDate;
	}

	@Override
	int getMaximumSize() {
		return Date.bytesPerDate;
	}
	
	public String toString() {
		return "DATE";
	}

	public int compareTo(Type t) {
		return getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
	}

	@Override
	Object convertLit(Object lit, Type to) throws TypeError {
		if (!(lit instanceof Date)) {
			throw new IllegalArgumentException("Expected date literal");
		}
		if (to.equals(this)) {
			return lit;
		}
		throw new TypeError("Cannot convert from " + this + " to " + to);			
	}
	
	public edu.upenn.cis.orchestra.datamodel.DateType getExecutionType() {
		return new edu.upenn.cis.orchestra.datamodel.DateType(nullable, labeledNullable);
	}

	DateType setConstantValue(Type t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new DateType((Date) newVal);
	}

	@Override
	Type getWithLabelledNullable(boolean newLabelledNullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(this.nullable, newLabelledNullable);
	}

	@Override
	Type getWithNullable(boolean nullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(nullable, this.labeledNullable);
	}
}
