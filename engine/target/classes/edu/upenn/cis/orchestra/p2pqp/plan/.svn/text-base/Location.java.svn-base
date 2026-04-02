package edu.upenn.cis.orchestra.p2pqp.plan;


public abstract class Location {
	public abstract boolean equals(Object o);
	
	public boolean isCentralized() {
		return false;
	}
	
	public boolean isDistributed() {
		return false;
	}
	
	public boolean isReplicated() {
		return false;
	}
	
	public boolean isNamed() {
		return false;
	}
	
	public String getName() {
		throw new IllegalStateException("Can only get the name of a named location");
	}
}
