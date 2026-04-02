/**
 * 
 */
package edu.upenn.cis.orchestra.datamodel.exceptions;



public class IncompatibleKeysException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public IncompatibleKeysException(String msg) {
		super(msg);
	}
}