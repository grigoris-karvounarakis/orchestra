package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import com.sleepycat.je.Transaction;

import rice.p2p.commonapi.Id;

class EpochAllocator {
	static final int FIRST_EPOCH = 0;
	// The last epoch this epoch allocator knows about; this may not be the last one
	// published, but the last epoch published cannot be less than this, making it a
	// good place to start trying to publish.
	private int lastEpoch;
	private P2PStore store;
	private Id epochAllocatorId;
	
	EpochAllocator(P2PStore store, Id epochAllocatorId) {
		this.store = store;
		this.lastEpoch = FIRST_EPOCH - 1;
		this.epochAllocatorId = epochAllocatorId;
	}
		
	synchronized P2PMessage processMessage(EpochAllocatorMessage eam, Transaction t, P2PStore.MessageProcessorThread processor) {
		if (eam instanceof RequestLastEpoch) {
			 return new LastEpochIs(lastEpoch, (RequestLastEpoch) eam);
		} else if (eam instanceof LastEpochIs) {
			LastEpochIs lea = (LastEpochIs) eam;
			if (lea.lastEpoch > lastEpoch) {
				lastEpoch = lea.lastEpoch;
			}
			return null;
		} else {
			return new ReplyException("Recevied unexpected Epoch Allocator message: " + eam, eam);
		}
	}
	
	synchronized void lastEpochIs(int epoch) {
		if (epoch > lastEpoch) {
			lastEpoch = epoch;
		}
	}
	
	int getLastEpoch() throws InterruptedException, UnexpectedReply {
		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
		P2PMessage request = new RequestLastEpoch(epochAllocatorId);
		store.sendMessageAwaitReply(request, new SimpleReplyContinuation<Integer>(1,replies), LastEpochIs.class);
		replies.waitUntilFinished();
		
		P2PMessage m = replies.getReply(1);
		if (m instanceof LastEpochIs) {
			LastEpochIs lei = (LastEpochIs) m;
			synchronized (this) {
				if (lei.lastEpoch > lastEpoch) {
					lastEpoch = lei.lastEpoch;
				}
				return lastEpoch;
			}
		} else {
			throw new UnexpectedReply(request,m);
		}
	}
	
	void shareLastEpoch() {
		int lastEpoch;
		synchronized (this) {
			lastEpoch = this.lastEpoch;
		}
		store.sendMessage(new LastEpochIs(lastEpoch, epochAllocatorId));
	}
}