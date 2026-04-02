/*
 * OWLDBParseErrorListener.java
 */

package edu.upenn.cis.orchestra.owldb;

import org.openrdf.rio.ParseErrorListener;

/**
 * This class represents an implementation of a ParseErrorListener.
 * 
 * @author Soeren Auer
 * @version 0.1, 2007-10-25
 */
public class OWLDBParseErrorListener implements ParseErrorListener {

	/**
	 * @see ParseErrorListener
	 */
	public void error(String msg, int line, int col) {
		System.out.println("ERROR (" + line + "," + col + "): " + msg);
	}
	
	/**
	 * @see ParseErrorListener
	 */
	public void fatalError(String msg, int line, int col) {
		System.out.println("FATAL ERROR (" + line + "," + col + "): " + msg);
	}
	
	/**
	 * @see ParseErrorListener
	 */
	public void warning(String msg, int line, int col) {
		System.out.println("WARNING (" + line + "," + col + "): " + msg);
	}
}
