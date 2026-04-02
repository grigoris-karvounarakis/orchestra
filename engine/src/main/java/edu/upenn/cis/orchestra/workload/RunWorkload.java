package edu.upenn.cis.orchestra.workload;


import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TupleSet;
import edu.upenn.cis.orchestra.reconciliation.Db;

public class RunWorkload {
	ObjectInputStream ois;
	LockManagerClient lmc;

	private void parseArgs(String args[]) throws Exception {
		for (int i = 0; i < args.length; i += 2) {
			if (args[i].equals("-workload")) {
				ois = new ObjectInputStream(new FileInputStream(args[i+1]));
			} else if (args[i].equals("-lockmanager")) {
				lmc = new LockManagerClient(args[i+1]);
			} else {
				throw new IllegalArgumentException("Don't know what to do with command line argument: " + args[i]);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public void runWorkload(String args[], CreateWorkload.DatabaseFactory dbf)  throws Exception{
		parseArgs(args);
		Schema s = (Schema) ois.readObject();
		int startingPeerId = (Integer) ois.readObject();
		int numLocalPeers = (Integer) ois.readObject();
		
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setTransactional(true);
		ec.setAllowCreate(true);
		File envFile = new File("rw" + startingPeerId);
		Environment e = new Environment(envFile, ec);

		HashMap<Integer,Db> dbs = new HashMap<Integer,Db>();
		
		for (int i = 0; i < numLocalPeers; ++i) {
			dbs.put(i + startingPeerId, dbf.createDb(i + startingPeerId, s, e));
		}
						
		long startTime = System.currentTimeMillis();
		int performedCount = 0;
		Object lastRead = ois.readObject();
		while (lastRead instanceof WorkloadAction) {
			WorkloadAction wa = (WorkloadAction) lastRead;
			if (wa == null) {
				System.out.print('-');
			} else if (wa instanceof ReconcileAction) {
				System.out.print('r');
			} else if (wa instanceof ResolveAction) {
				System.out.print('R');
			} else if (wa instanceof TransactionAction) {
				System.out.print('T');
			}
			System.out.flush();
			wa.doAction(dbs, lmc);
			lastRead = ois.readObject();
			++performedCount;
			if (performedCount % 80 == 0) {
				System.out.println();
			}
		}

		long endTime = System.currentTimeMillis();
		
		
		for (int currPeer = 0; currPeer < numLocalPeers; ++currPeer, lastRead = ois.readObject()) {
			Map referenceRelations = (Map) lastRead;
			Map<String,TupleSet> computedRelations = dbs.get(currPeer + startingPeerId).getState();
			
			for (String relName : computedRelations.keySet()) {
				TupleSet computedState = computedRelations.get(relName);
				TupleSet referenceState = (TupleSet) referenceRelations.get(relName);
				
				for (Object o : referenceState) {
					Tuple t = (Tuple) o;
					Tuple found = computedState.get(t);
					if (found == null || (! t.equals(found))) {
						System.out.println("Tuple " + t + " from reference state is missing in computed state for peer "
								+ (currPeer + startingPeerId));
					}
				}
				
				for (Tuple t: computedState ) {
					Tuple found = (Tuple) referenceState.get(t);
					if (found == null || (! t.equals(found))) {
						System.out.println("Tuple " + t + " from computed state is not present in reference state for peer "
								+ (currPeer + startingPeerId));
					}
				}
			}
		}
		
		System.out.println("\nElapsed time: " + (endTime - startTime) / 1000.0 + " sec");
	}
}
