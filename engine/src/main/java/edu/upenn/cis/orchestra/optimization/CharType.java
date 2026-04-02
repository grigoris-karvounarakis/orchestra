package edu.upenn.cis.orchestra.optimization;


import edu.upenn.cis.orchestra.datamodel.StringType;

public class CharType extends Type {
	public CharType(String constantValue) {
		super(constantValue);
		this.length = constantValue.length();
	}
	
	int length;
	public CharType(int length, boolean nullable, boolean labeledNullable) {
		super(nullable, labeledNullable);
		this.length = length;
	}

	@Override
	int getExpectedSize() {
		return length * Character.SIZE / Byte.SIZE;
	}
	@Override
	int getMaximumSize() {
		return getExpectedSize();
	}
	
	@Override
	public boolean equals(Object o) {
		if (! super.equals(o)) {
			return false;
		}
		CharType ct = (CharType) o;
		return (length == ct.length);
	}

	public String toString() {
		return "CHAR(" + length + ")";
	}

	public int compareTo(Type t) {
		int compare = getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
		if (compare != 0) {
			return compare;
		}
		CharType ct = (CharType) t;
		return length - ct.length;
	}

	@Override
	Object convertLit(Object lit, Type to) throws TypeError {
		if (!(lit instanceof String)) {
			throw new IllegalArgumentException("Expected string literal");
		}
		String s = (String) lit;
		if (to instanceof VarCharType) {
			VarCharType vct = (VarCharType) to;
			if (vct.maxLength >= s.length()) {
				return s;
			} else {
				throw new TypeError("Cannot convert from " + this + " to " + to);			
			}
		} else if (to instanceof CharType) {
			CharType ct = (CharType) to;
			if (ct.length == s.length()) {
				return s;
			} else {
				throw new TypeError("Cannot convert from " + this + " to " + to);			
			}
		} else {
			throw new TypeError("Cannot convert from " + this + " to " + to);			
		}
	}
	
	public StringType getExecutionType() {
		return new StringType(nullable, labeledNullable, false, length);
	}
	
	CharType setConstantValue(Type t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new CharType((String) newVal);
	}

	@Override
	Type getWithLabelledNullable(boolean newLabeledNullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		if (newLabeledNullable == this.labeledNullable) {
			return this;
		}
		return new CharType(length, this.nullable, newLabeledNullable);
	}

	@Override
	Type getWithNullable(boolean nullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		if (nullable == this.nullable) {
			return this;
		}
		return new CharType(length, nullable, this.labeledNullable);
	}

}

