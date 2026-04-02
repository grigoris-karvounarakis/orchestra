package edu.upenn.cis.orchestra.p2pqp;

import edu.upenn.cis.orchestra.p2pqp.DHTService.EpochNum;

interface KeyReceiver {
	/**
	 * Receive the keys for relation that satisfy a key predicate
	 * and fall into this node's id space
	 * 
	 * @param phaseNo	The sequence number of the set of pages. May be more than zero if
	 * 					recovery happens
	 * @param page		The ID of the page
	 * @param keyRanges	The ranges in the DHT key space that the keys fall in
	 * @param keys		The tuples that satisfy the key predicate and ID space constraint
	 */
	void receiveKeys(int phaseNo, EpochNum page, IdRangeSet keyRanges, KeySource keys);
	
	/**
	 * Begin a new phase in this operator.
	 * 
	 * @param phaseNo			The phase number, starting from zero
	 * @param keyRanges			The ranges in the key space that this operator will scan
	 * 							in this phase
	 */
	void beginPhase(int phaseNo, IdRangeSet keyRanges);
	
	int getRelationId();
}
