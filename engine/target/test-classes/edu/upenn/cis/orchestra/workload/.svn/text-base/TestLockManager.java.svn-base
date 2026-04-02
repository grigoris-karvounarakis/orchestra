package edu.upenn.cis.orchestra.workload;

public class TestLockManager {
	public static void main(String[] args) throws Exception {
		StringBuffer line = new StringBuffer();
		int c = System.in.read();
		while (c != -1) {
			if ((c == '\n' || c == '\r') && line.length() > 0) {
				int peer = Integer.parseInt(line.toString());
				LockManagerClient lmc = new LockManagerClient(null);
				System.out.print("Requesting lock " + peer + "...");
				System.out.flush();
				lmc.getLock(peer);
				System.out.println("granted, released");
				lmc.releaseLock();
				line.setLength(0);
			} else if (c != '\n' && c != '\r') {
				line.append((char) c);
			}
			c = System.in.read();
		}
	}
}
