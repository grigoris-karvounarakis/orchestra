/**
 * 
 */
package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;

public class QueryFailure extends Exception {
	private static final long serialVersionUID = 1L;

	QueryFailure(InetSocketAddress node, Exception e) {
		super("Failure during query processing at " + node, e);
	}

	QueryFailure(String msg, Exception e) {
		super(msg,e);
	}

	QueryFailure(String msg) {
		super(msg);
	}
}