package edu.upenn.cis.orchestra.workload;

import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.Socket;

public class LockManagerClient {
	public final int defaultPort = LockManager.LOCK_MANAGER_PORT;
	private String hostname;
	private int port;
	private Socket s;
	private Reader r;
	private PrintWriter p;
	
	public LockManagerClient(String host) {
		hostname = host;
		port = defaultPort;
		s = null;
		r = null;
		p = null;
	}
	
	public LockManagerClient(String host, int portNum) {
		hostname = host;
		port = portNum;
		s = null;
		r = null;
		p = null;
	}
	
	public void getLock(int peerID) throws Exception {
		if (s != null) {
			throw new RuntimeException("Attempt to get lock when lock is already held or requested");
		}
		s = new Socket(hostname, port);
		r = new InputStreamReader(s.getInputStream());
		p = new PrintWriter(s.getOutputStream());
		p.println(peerID);
		p.flush();
		
		StringBuffer sb = new StringBuffer();
		int c = r.read();
		while (c != '\n' && c != '\r' && c != -1) {
			sb.append((char) c);
			c = r.read();
		}

		if (! sb.toString().equals("GRANTED")) {
			throw new RuntimeException("Unexpected response from lock server: " + sb);
		}
		
		return;
	}
	
	public void releaseLock() throws Exception {
		if (s == null) {
			throw new RuntimeException("Attempt to unlock when lock was not requested");
		}
		p.println("RELEASED");
		p.flush();
		s.close();
		s = null;
		p = null;
		r = null;
	}
}
