package edu.upenn.cis.orchestra.workload;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.Relation;

class PrintWorkload {
	public static void main(String args[]) throws Exception {
		FileInputStream fis = new FileInputStream(args[0]);
		ObjectInputStream ois = new ObjectInputStream(fis);
		
		Relation s = (Relation) ois.readObject();
		System.out.println(s);
		
		int numPeers = (Integer) ois.readObject();
		System.out.println("Number of peers:" + numPeers);
		
		Object lastRead = ois.readObject();
		while (lastRead instanceof WorkloadAction) {
			WorkloadAction wa = (WorkloadAction) lastRead;
			System.out.println(wa);
			lastRead = ois.readObject();
		}
		
		for (int currPeer = 0; currPeer < numPeers; ++currPeer, lastRead = ois.readObject()) {
			HashMap referenceState = (HashMap) lastRead;
			Set state = referenceState.keySet();
			System.out.println("Peer " + currPeer + " state:");
			for (Object o : state) {
				System.out.println("\t" + o);
			}
		}
		
		ois.close();
	}
}
