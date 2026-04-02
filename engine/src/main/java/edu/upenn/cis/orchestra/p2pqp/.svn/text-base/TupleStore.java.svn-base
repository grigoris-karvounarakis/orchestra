package edu.upenn.cis.orchestra.p2pqp;

import java.io.PrintStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.util.ByteArraySet;

public interface TupleStore<M> extends QpSchema.Source {
	/**
	 * Returns the value of a tuple for a particular epoch
	 * 
	 * @param key				The key of the tuple
	 * @param epoch				The epoch
	 * @return					The most recent value for the key before
	 * 							the supplied epoch, or null if the tuple was
	 * 							deleted or does not exist
	 * @throws TupleStoreException
	 */
	QpTuple<M> getTuple(AbstractTuple<QpSchema> key, int epoch) throws TupleStoreException;
	
	/**
	 * Return the value of a tuple stored for a particular key at a
	 * particular epoch. It does not search previous epochs, use getTuple to do
	 * that.
	 * 
	 * @param key			The key (at the correct epoch)
	 * @return				The associated value, or <code>null</code> if there is none
	 * @throws TupleStoreException
	 */
	QpTuple<M> getTupleByKey(QpTupleKey key) throws TupleStoreException;
	
	/**
	 * Return the value of tuples stored for a particular keys at
	 * particular epochs. It does not search previous epochs, use getTuples to do
	 * that.
	 * 
	 * @param key			The keys (at the correct epoch)
	 * @return				The associated values, or <code>null</code> if there is none
	 * @throws TupleStoreException
	 */
	List<QpTuple<M>> getTuplesByKey(List<QpTupleKey> key) throws TupleStoreException;
		
	/**
	 * Return the value of tuples stored for a particular keys at
	 * particular epochs. It does not search previous epochs, use getTuples to do
	 * that.
	 * 
	 * @param keys			The keys (at the correct epoch)
	 * @param dest			Where to put the tuples
	 * @throws TupleStoreException
	 */
	void getTuplesByKey(Iterator<QpTupleKey> key, TupleSink<M> dest) throws TupleStoreException;
	
	void deleteTuple(AbstractTuple<QpSchema> t, int epoch) throws TupleStoreException;
	void deleteTuples(Collection<QpTupleKey> keys) throws TupleStoreException;
	
	/**
	 * Add a tuple to the tuple store
	 * 
	 * @param t			The tuple to add
	 * @return			<code>null</code> if the tuple was added successfully, or a
	 * 					<code>ConstraintViolation</code> if one occurred
	 * @throws TupleStoreException
	 * 					In the event of a database error
	 */
	ConstraintViolation addTuple(QpTuple<M> t, int epoch) throws TupleStoreException;

	/**
	 * @param ts		A list of tuples to add
	 * @return 			A map from tuples to constrain violations
	 * @throws TupleStoreException
	 * 					In the event of a database error
	 */
	Map<QpTuple<M>,ConstraintViolation> addTuples(Iterator<QpTuple<M>> ts, int epoch) throws TupleStoreException;

	public static class TupleStoreException extends Exception {
		private static final long serialVersionUID = 1L;

		TupleStoreException(String what) {
			super(what);
		}
		
		TupleStoreException(String what, Throwable why) {
			super(what, why);
		}
	}
	
	public static class ConstraintViolation implements Serializable {
		private static final long serialVersionUID = 1L;
		public final String err;
		ConstraintViolation(String why, QpTuple<?> qt) {
			err = "Error inserting " + qt + " into " + qt.getRelationName() + ": " + why;
		}
		
		public String toString() {
			return "ContraintViolation(" + err + ")";
		}
	}


	
	/**
	 * Start a scan of a table in the TupleStore.
	 * 
	 * @param relation		The name of the table to scan
	 * @param dest			Where to send the results
	 * @param outputDest	Which input to send the results to
	 * @param keyFilter		A filter over the keys columns, or <code>null</code> not to
	 * 						filter based on keys.
	 * @param fullFilter	A filter over all columns that output tuples must satisfy,
	 * 						or <code>null</code> to output all tuples that pass the key
	 * 						filter
	 * @param lastEpoch		The maximum epoch of tuples to output, or <code>null</code>
	 * 						to continue until the last epoch
	 * @param queryId		The ID of the query performing the scan
	 * @param nodeId		The ID of this node in the DHT
	 * @param operatorId	The ID of the scan node in the query
	 * @param rt			Where to store information about new tuples created
	 * @param enableRecovery
	 * 						Whether to enable support for recovery from node failure
	 * @param phaseNo		
	 * @return				The scan operator
	 */
	PullScanOperator<M> beginScan(String relation, Operator<M> dest, WhichInput outputDest, Predicate keyFilter, Filter<? super QpTuple<M>> fullFilter, Integer lastEpoch,
			int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, boolean enableRecovery, int phaseNo);
	
	/**
	 * Start a scan of a table in the TupleStore.
	 * 
	 * @param relation		The ID of the table to scan
	 * @param dest			Where to send the results
	 * @param outputDest	Which input to send the results to
	 * @param keyFilter		A filter over the keys columns, or <code>null</code> not to
	 * 						filter based on keys.
	 * @param fullFilter	A filter over all columns that output tuples must satisfy,
	 * 						or <code>null</code> to output all tuples that pass the key
	 * 						filter
	 * @param lastEpoch		The maximum epoch of tuples to output, or <code>null</code>
	 * 						to continue until the last epoch
	 * @param queryId		The ID of the query performing the scan
	 * @param nodeId		The ID of this node in the DHT
	 * @param operatorId	The ID of the scan node in the query
	 * @param rt			Where to store information about new tuples created
	 * @param enableRecovery
	 * 						Whether to enable support for recovery from node failure
	 * @param phaseNo		
	 * @return				The scan operator
	 */
	PullScanOperator<M> beginScan(int relation, Operator<M> dest, WhichInput outputDest, Predicate keyFilter, Filter<? super QpTuple<M>> fullFilter, Integer lastEpoch,
			int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, boolean enableRecovery, int phaseNo);
	
	PullScanOperator<M> beginScan(int relation, ByteArraySet keys, Filter<? super QpTuple<M>> fullFilter, Operator<M> componentOf, IdRangeSet ranges, int phaseNo);

	public interface TupleSink<M> {
		void deliverTuple(QpTuple<M> tuple);
		void tupleNotFound(QpTupleKey key);
	}
	
	
	void clear() throws TupleStoreException;
	void close() throws TupleStoreException;
	
	void addTable(QpSchema ns);
	
	void dropTable(String name) throws TupleStoreException;
	void clearTable(String name) throws TupleStoreException;
	
	Map<String, QpSchema> getTables();
	
	void printStats(PrintStream ps) throws TupleStoreException;
}
