/**
 * 
 */
package edu.upenn.cis.orchestra.datamodel.exceptions;



public class UnsupportedTypeException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public UnsupportedTypeException(String type) {
		super("Unsupported type " + type);
	}
}