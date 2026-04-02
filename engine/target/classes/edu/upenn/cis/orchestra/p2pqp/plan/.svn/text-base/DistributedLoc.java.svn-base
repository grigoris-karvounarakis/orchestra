package edu.upenn.cis.orchestra.p2pqp.plan;

public class DistributedLoc extends Location {
	private static final long serialVersionUID = 1L;
	private static final DistributedLoc instance = new DistributedLoc(false);
	private static final DistributedLoc replicatedInstance = new DistributedLoc(true);
	
	private final boolean isReplicated;
	
	public static DistributedLoc getInstance() {
		return instance;
	}
	
	public static DistributedLoc getReplicatedInstance() {
		return replicatedInstance;
	}
	
	private DistributedLoc(boolean replicated) {
		isReplicated = replicated;
	}
	
	@Override
	public boolean equals(Object o) {
		return (o instanceof DistributedLoc);
	}

	@Override
	public boolean isDistributed() {
		return true;
	}
	
	@Override
	public boolean isReplicated() {
		return isReplicated;
	}
}
