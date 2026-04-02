package edu.upenn.cis.orchestra.predicate;

import java.io.Serializable;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.p2pqp.Filter;

/**
 * Define how we can interact with a tuple-level predicate.
 * 
 * @author Nick Taylor
 */
public interface Predicate extends Serializable, Filter<AbstractTuple<?>> {
	/**
	 * Evaluate the predicate over the supplied tuple.
	 * 
	 * @param t				The tuple to evaluate the predicate over
	 * @return				<code>true</code> if the predicate is satisfied,
	 * 						<code>false</code> if it is not
	 * @throws CompareMismatch
	 * 						If a type error occurs
	 */
	public abstract boolean eval(AbstractTuple<?> t) throws CompareMismatch;
	
	/**
	 * Returns a string encoding this predicate in SQL. The specified prefix
	 * and suffix are inserted into the predicate before and after any column
	 * names.
	 * 
	 * @param prefix		Prefix for column names
	 * @param suffix		Suffix for column names
	 * @return				The predicate in SQL
	 */
	public abstract String getSqlCondition(AbstractRelation ts, String prefix, String suffix);
	
	/**
	 * Serialize a predicate to an element in an XML document.
	 * 
	 * @param d			The document
	 * @param el		The element in the document
	 * @param ts		The schema of the relation to which the predicate
	 * 					refers
	 */
	public abstract void serialize(Document d, Element el, AbstractRelation ts);	
}