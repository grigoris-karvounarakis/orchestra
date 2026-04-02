package edu.upenn.cis.orchestra.reconciliation.p2pstore;

class UnexpectedReply extends P2PStore.P2PStoreException {
	private static final long serialVersionUID = 1L;
	final P2PMessage request;
	final P2PMessage response;

	UnexpectedReply(P2PMessage request, P2PMessage response) {
		super("Received unexpected reply " + response + " to " + request);
		this.request = request;
		this.response = response;
	}	
}
