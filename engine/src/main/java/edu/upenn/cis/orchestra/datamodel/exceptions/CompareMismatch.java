/**
 * 
 */
package edu.upenn.cis.orchestra.datamodel.exceptions;

import edu.upenn.cis.orchestra.p2pqp.Filter;

public class CompareMismatch extends Filter.FilterException {
	private static final long serialVersionUID = 1L;
	
	public CompareMismatch(Object o1, Object o2, Class<?> expected) {
		super("Tried to compare value " + o1 + " of type " + o1.getClass().getName() + " with value " + o2 + " of type " + o2.getClass().getName() + " in comparison function for type " + expected.getName());
	}

}