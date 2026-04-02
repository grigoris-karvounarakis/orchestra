package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.util.LongList;


interface SocketManagerClient {
	void sentMessagesReceived(InetSocketAddress sentTo, LongList msgIds);	
	void messagesSent(LongList msgIds);
	void messageSendingFailed(InetSocketAddress dest, LongList msgIds);
	void peerIsDead(InetSocketAddress peer);
	void peerIsNotDead(InetSocketAddress peer);
	void startedThrottling(InetSocketAddress node);
	void stoppedThrottling(InetSocketAddress node);
}
