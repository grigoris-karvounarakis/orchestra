package edu.upenn.cis.orchestra.optimization;


public abstract class Type implements Comparable<Type> {

	public static class TypeError extends Exception {
		private static final long serialVersionUID = 1L;
		TypeError(String error) {
			super(error);
		}
	}	

	abstract int getExpectedSize();
	abstract int getMaximumSize();
	
	/**
	 * Convert the constant value of this type to a literal of
	 * another type
	 * 
	 * @param to			The desired type of the literal
	 * @return				A literal of the desired type
	 * @throws Expression.TypeError
	 * 						if the conversion cannot be made
	 */
	abstract Object convertLit(Object lit, Type to) throws TypeError;

	public boolean equals(Object o) {
		return (o != null && o.getClass() == this.getClass());
	}
	
	abstract public String toString();
	// If variable being typed is known to have a constant value,
	// put it here.
	private final Object constantValue;
	
	final boolean nullable;
	final boolean labeledNullable;
	
	Type(boolean nullable, boolean labeledNullable) {
		constantValue = null;
		this.nullable = nullable;
		this.labeledNullable = labeledNullable;
	}
	
	public Type(Object constantValue) {
		if (constantValue == null) {
			throw new NullPointerException();
		}
		this.constantValue = constantValue;
		nullable = false;
		labeledNullable = false;
	}
	
	boolean valueKnown() {
		return (constantValue != null);
	}
	
	Object getConstantValue() {
		return constantValue;
	}
	
	/**
	 * Create a new Type object of the same kind as this one
	 * but with the constant value from another
	 * 
	 * @param t			The type with the constant value to convert
	 * @throws Expression.TypeError
	 * 					If the conversion could not take place
	 */
	abstract Type setConstantValue(Type t) throws TypeError;
	
	public abstract edu.upenn.cis.orchestra.datamodel.Type getExecutionType();
	
	abstract Type getWithLabelledNullable(boolean newLabeledNullable);
	
	abstract Type getWithNullable(boolean nullable);
}