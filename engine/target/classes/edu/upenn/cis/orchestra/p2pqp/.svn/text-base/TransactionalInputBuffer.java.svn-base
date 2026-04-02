package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;

import edu.upenn.cis.orchestra.util.InetSocketAddressCache;
import edu.upenn.cis.orchestra.util.InputBuffer;

abstract class TransactionalInputBuffer extends InputBuffer {
	TransactionalInputBuffer() {
	}
	
	TransactionalInputBuffer(InetSocketAddressCache cache) {
		super(cache);
	}
	
	/**
	 * Record that the current transaction has been read normally.
	 * 
	 * @throws IllegalStateException
	 * 						if there is data from this transaction remaining
	 * 
	 */
	abstract void endReadingTransaction() throws IOException, TransactionSizeException;
	
	/**
	 * Skip the remainder of the current transaction and
	 * proceed to the next transaction
	 */
	abstract void skipTransaction() throws IOException;
	
	static class TransactionSizeException extends Exception {
		private static final long serialVersionUID = 1L;
		final int expected;
		final int read;
		
		TransactionSizeException(int expected, int read) {
			super("Transaction has size " + expected + " but " + read + " bytes were read");
			this.expected = expected;
			this.read = read;
		}
	}
}
