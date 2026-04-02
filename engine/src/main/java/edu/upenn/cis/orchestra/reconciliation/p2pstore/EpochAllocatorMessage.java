package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import rice.p2p.commonapi.Id;

/**
 * Superclass of all messages that get routed to the EpochAllocator
 * running at a particular peer, and replies that get sent back
 * from the epoch allocator
 * 
 * 
 * @author Nick
 *
 */
abstract class EpochAllocatorMessage extends P2PMessage {
	EpochAllocatorMessage(Id id) {
		super(id);
	}

	EpochAllocatorMessage(EpochAllocatorMessage m) {
		super(m);
	}

	private static final long serialVersionUID = 1L;
}

/**
 * A request for the last epoch that the epoch allocator knows about.
 * All epochs prior to this have been published. More recent epochs may
 * me published, and they may not be.
 * 
 * 
 * @author Nick
 *
 */
class RequestLastEpoch extends EpochAllocatorMessage {
	private static final long serialVersionUID = 1L;
	RequestLastEpoch(Id id) {
		super(id);
	}
}

/**
 * A reply to a {@link RequestLastEpoch} message, it tells what the
 * last epoch the epoch allocator knows about it. It is also sent by an
 * epoch controller when it grants an epoch.
 * 
 * @author Nick
 *
 */
class LastEpochIs extends EpochAllocatorMessage {
	private static final long serialVersionUID = 1L;
	final int lastEpoch;
	LastEpochIs(int lastEpoch, RequestLastEpoch rle) {
		super(rle);
		this.lastEpoch = lastEpoch;
	}
	LastEpochIs(int lastEpoch, Id dest) {
		super(dest);
		this.lastEpoch = lastEpoch;
	}
}