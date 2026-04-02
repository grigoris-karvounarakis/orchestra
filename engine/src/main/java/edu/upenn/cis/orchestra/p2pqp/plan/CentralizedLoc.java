package edu.upenn.cis.orchestra.p2pqp.plan;

public class CentralizedLoc extends Location {
	private static final long serialVersionUID = 1L;
	private static final CentralizedLoc instance = new CentralizedLoc();
	
	public static CentralizedLoc getInstance() {
		return instance;
	}
	
	private CentralizedLoc() {
	}

	@Override
	public boolean equals(Object o) {
		return (o instanceof CentralizedLoc);
	}
	
	@Override
	public boolean isCentralized() {
		return true;
	}

}
