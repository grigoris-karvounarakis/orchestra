package edu.upenn.cis.orchestra.datamodel.iterators;

public class IteratorException extends Exception {
	private static final long serialVersionUID = 1L;

	public IteratorException() {
	}

	public IteratorException(String why) {
		super(why);
	}

	public IteratorException(Throwable what) {
		super(what);
	}

	public IteratorException(String why, Throwable what) {
		super(why, what);
	}

}
