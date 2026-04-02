package edu.upenn.cis.orchestra.p2pqp;

public class QueryInstantiationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public QueryInstantiationException(String err) {
		super(err);
	}

	public QueryInstantiationException(String err, Throwable cause) {
		super(err, cause);
	}
	
	public QueryInstantiationException(Throwable cause) {
		super(cause);
	}
}
