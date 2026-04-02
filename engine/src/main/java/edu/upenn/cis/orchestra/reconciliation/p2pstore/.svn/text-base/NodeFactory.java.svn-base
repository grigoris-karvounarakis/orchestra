package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import rice.p2p.commonapi.Node;

/**
 * @author netaylor
 *
 */
public interface NodeFactory {
	/**
	 * Allocate a node in the P2P network
	 * 
	 * @return The node itself
	 */
	public Node getNode();

	/**
	 * Deallocate a node in the P2P network
	 * 
	 * @param n	The node to deallocate. It becomes invalid after
	 * this function is called.
	 */
	public void shutdownNode(Node n);

	/**
	 * Get an IdFactory for the network type created by this NodeFactory. The
	 * IdFactory must have a public zero-argument constructor so that a copy
	 * can be created by the comparator used for the BerkeleyDB database.
	 * 
	 * @return
	 */
	public IdFactory getIdFactory();
}