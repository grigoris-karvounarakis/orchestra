/**
 * 
 */
package edu.upenn.cis.orchestra.datamodel.exceptions;



public class IncompatibleTypesException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public IncompatibleTypesException(String type1, String type2) {
		super("Type mismatch -- same var used in columns of different types: " + type1 + ", " + type2);
	}
}