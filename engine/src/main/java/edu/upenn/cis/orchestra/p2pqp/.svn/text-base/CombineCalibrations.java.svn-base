package edu.upenn.cis.orchestra.p2pqp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CombineCalibrations implements Serializable {
	private static final long serialVersionUID = 1L;
	public final SystemCalibration localCal, remoteCal;
	public final Bandwidth localBand, remoteBand;
	public final Map<String,SystemCalibration> namedCals;
	public final Map<String,Bandwidth> namedBands;
	
	public CombineCalibrations(SystemCalibration localCal, SystemCalibration remoteCal,
			Bandwidth localBand, Bandwidth remoteBand) {
		this.localBand = localBand;
		this.remoteBand = remoteBand;
		this.localCal = localCal;
		this.remoteCal = remoteCal;
		this.namedBands = Collections.emptyMap();
		this.namedCals = Collections.emptyMap();
	}

	public CombineCalibrations(SystemCalibration localCal, Collection<SystemCalibration> remoteCal,
			Bandwidth localBand, Collection<Bandwidth> remoteBand) {
		
		if (remoteCal.isEmpty()) {
			throw new IllegalArgumentException("No remote calibrations supplied");
		}
		
		if (remoteBand.isEmpty()) {
			throw new IllegalArgumentException("No remote bandwidths supplied");
		}
		
		this.localCal = localCal;
		this.localBand = localBand;
		this.namedBands = Collections.emptyMap();
		this.namedCals = Collections.emptyMap();
		
		double joinStoresPerSecond = Double.MAX_VALUE;
		double joinProducePerSecond = Double.MAX_VALUE;		
		double aggregateInputRowPerSecond = Double.MAX_VALUE;
		double aggregateProcessFuncPerSecond = Double.MAX_VALUE;		
		double predicatesPerSecond = Double.MAX_VALUE;
		double predicateTuplesInputPerSecond = Double.MAX_VALUE;
		double predicateTuplesOutputPerSecond = Double.MAX_VALUE;
		double functionsPerSecond = Double.MAX_VALUE;
		double functionTuplesPerSecond = Double.MAX_VALUE;
		double tupleIdsPerSecond = Double.MAX_VALUE;
		double msgsSentPerSecond = Double.MAX_VALUE;
		double msgsDeliveredPerSecond = Double.MAX_VALUE;
		double versionedScanPassPredPerSecond = Double.MAX_VALUE;
		double versionedScanFailPredPerSecond = Double.MAX_VALUE;		
		double tuplesProbedPerSecond = Double.MAX_VALUE;
		double indexLookupsPerSecond = Double.MAX_VALUE;		
		double fullTuplesSerializedPerSecond = Double.MAX_VALUE;
		double fullTuplesDeserializedPerSecond = Double.MAX_VALUE;
		double keyTuplesSerializedPerSecond = Double.MAX_VALUE;
		double keyTuplesDeserializedPerSecond = Double.MAX_VALUE;
		double indexedScanTuplesReadPerSecond = Double.MAX_VALUE;

		for (SystemCalibration sc : remoteCal) {
			if (sc.joinStoresPerSecond < joinStoresPerSecond) {
				joinStoresPerSecond = sc.joinStoresPerSecond;
			}
			if (sc.joinProducePerSecond < joinProducePerSecond) {
				joinProducePerSecond = sc.joinProducePerSecond;
			}
			if (sc.aggregateInputRowPerSecond < aggregateInputRowPerSecond) {
				aggregateInputRowPerSecond = sc.aggregateInputRowPerSecond;
			}
			if (sc.aggregateProcessFuncPerSecond < aggregateProcessFuncPerSecond) {
				aggregateProcessFuncPerSecond = sc.aggregateProcessFuncPerSecond;
			}
			if (sc.predicatesPerSecond < predicatesPerSecond) {
				predicatesPerSecond = sc.predicatesPerSecond;
			}
			if (sc.predicateTuplesInputPerSecond < predicateTuplesInputPerSecond) {
				predicateTuplesInputPerSecond = sc.predicateTuplesInputPerSecond;
			}
			if (sc.predicateTuplesOutputPerSecond < predicateTuplesOutputPerSecond) {
				predicateTuplesOutputPerSecond = sc.predicateTuplesOutputPerSecond;
			}
			if (sc.functionsPerSecond < functionsPerSecond) {
				functionsPerSecond = sc.functionsPerSecond;
			}
			if (sc.functionTuplesPerSecond < functionTuplesPerSecond) {
				functionTuplesPerSecond= sc.functionTuplesPerSecond;
			}
			if (sc.tupleIdsPerSecond < tupleIdsPerSecond) {
				tupleIdsPerSecond = sc.tupleIdsPerSecond;
			}
			if (sc.msgsSentPerSecond < msgsSentPerSecond) {
				msgsSentPerSecond = sc.msgsSentPerSecond;
			}
			if (sc.msgsDeliveredPerSecond < msgsDeliveredPerSecond) {
				msgsDeliveredPerSecond = sc.msgsDeliveredPerSecond;
			}
			if (sc.versionedScanPassPredPerSecond < versionedScanPassPredPerSecond) {
				versionedScanPassPredPerSecond = sc.versionedScanPassPredPerSecond;
			}
			if (sc.versionedScanFailPredPerSecond < versionedScanFailPredPerSecond) {
				versionedScanFailPredPerSecond = sc.versionedScanFailPredPerSecond;
			}
			if (sc.tuplesProbedPerSecond < tuplesProbedPerSecond) {
				tuplesProbedPerSecond = sc.tuplesProbedPerSecond;
			}
			if (sc.indexLookupsPerSecond < indexLookupsPerSecond) {
				indexLookupsPerSecond = sc.indexLookupsPerSecond;
			}
			if (sc.fullTuplesSerializedPerSecond < fullTuplesSerializedPerSecond) {
				fullTuplesSerializedPerSecond = sc.fullTuplesSerializedPerSecond;
			}
			if (sc.fullTuplesDeserializedPerSecond < fullTuplesDeserializedPerSecond) {
				fullTuplesDeserializedPerSecond = sc.fullTuplesDeserializedPerSecond;
			}
			if (sc.keyTuplesSerializedPerSecond < keyTuplesSerializedPerSecond) {
				keyTuplesSerializedPerSecond = sc.keyTuplesSerializedPerSecond;
			}
			if (sc.keyTuplesDeserializedPerSecond < keyTuplesDeserializedPerSecond) {
				keyTuplesDeserializedPerSecond = sc.keyTuplesDeserializedPerSecond;
			}
			if (sc.indexedScanTuplesReadPerSecond < indexedScanTuplesReadPerSecond) {
				indexedScanTuplesReadPerSecond = sc.indexedScanTuplesReadPerSecond;
			}
		}

		this.remoteCal = new SystemCalibration(joinStoresPerSecond,
				joinProducePerSecond,		
				aggregateInputRowPerSecond,
				aggregateProcessFuncPerSecond,		
				predicatesPerSecond,
				predicateTuplesInputPerSecond,
				predicateTuplesOutputPerSecond,
				functionsPerSecond,
				functionTuplesPerSecond,
				tupleIdsPerSecond,
				msgsSentPerSecond,
				versionedScanPassPredPerSecond,
				versionedScanFailPredPerSecond,		
				tuplesProbedPerSecond,
				indexLookupsPerSecond,		
				fullTuplesSerializedPerSecond,
				fullTuplesDeserializedPerSecond,
				keyTuplesSerializedPerSecond,
				keyTuplesDeserializedPerSecond,
				indexedScanTuplesReadPerSecond);

		Map<String,Double> namedNodeBandwidths = new HashMap<String,Double>();
		Map<String,Double> namedNodeLatencies = new HashMap<String,Double>();
		double worstRemoteBandwidth = Double.MAX_VALUE;
		double worstRemoteLatency = 0;
		
		for (Bandwidth b : remoteBand) {
			if (b.worstRemoteBandwidth < worstRemoteBandwidth) {
				worstRemoteBandwidth = b.worstRemoteBandwidth;
			}
			if (b.worstRemoteLatency > worstRemoteLatency) {
				worstRemoteLatency = b.worstRemoteLatency;
			}
			for (String node : b.namedNodeBandwidths.keySet()) {
				Double worst = namedNodeBandwidths.get(node);
				Double curr = b.namedNodeBandwidths.get(node);
				if (worst == null || worst > curr) {
					namedNodeBandwidths.put(node, curr);
				}
			}
			for (String node : b.namedNodeLatencies.keySet()) {
				Double worst = namedNodeLatencies.get(node);
				Double curr = b.namedNodeLatencies.get(node);
				if (worst == null || worst < curr) {
					namedNodeLatencies.put(node, curr);
				}
			}
		}

		this.remoteBand = new Bandwidth(namedNodeBandwidths, namedNodeLatencies,
				worstRemoteBandwidth, worstRemoteLatency);
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
		if (args.length == 1) {
			ObjectInputStream o = new ObjectInputStream(new FileInputStream(args[0]));
			System.out.println(o.readObject());
			return;
		} else if (args.length != 5 && args.length != 3) {
			System.err.println("Arguments: outFile localCalFile localBandFile (remoteCalFile|remoteCalDirectory remoteBandFile|remoteBandDirectory)");
			System.exit(-1);
			return;
		}
		
		File localCalFile = new File(args[1]), localBandFile = new File(args[2]), remoteCal = null, remoteBand = null;
		if (args.length == 5) {
			remoteCal = new File(args[3]);
			remoteBand  = new File(args[4]);
		}
		Bandwidth localBand;
		List<Bandwidth> remoteBands = new ArrayList<Bandwidth>();
		SystemCalibration localCal;
		List<SystemCalibration> remoteCals = new ArrayList<SystemCalibration>();
		
		if (localCalFile.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(localCalFile));
			localCal = (SystemCalibration) ois.readObject();
			ois.close();
		} else {
			System.err.println("Local calibration " + localCalFile + " not found");
			System.exit(-1);
			return;
		}
		
		if (localBandFile.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(localBandFile));
			localBand = (Bandwidth) ois.readObject();
			ois.close();
		} else {
			System.err.println("Local bandwidth " + localBandFile + " not found");
			System.exit(-1);
			return;
		}
		
		if (remoteCal == null) {
			remoteCals.add(localCal);
		} else if (remoteCal.isDirectory()) {
			for (File f : remoteCal.listFiles()) {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
				remoteCals.add((SystemCalibration) ois.readObject());
				ois.close();
			}
		} else if (remoteCal.isFile()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(remoteCal));
			remoteCals.add((SystemCalibration) ois.readObject());
			ois.close();
		} else {
			System.err.println("Remote calibration directory " + remoteCal + " not found");
			System.exit(-1);
			return;
		}
		
		if (remoteBand == null) {
			remoteBands.add(localBand);
		} else if (remoteBand.isDirectory()) {
			for (File f : remoteBand.listFiles()) {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
				remoteBands.add((Bandwidth) ois.readObject());
				ois.close();
			}
		} else if (remoteBand.isFile()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(remoteBand));
			remoteBands.add((Bandwidth) ois.readObject());
			ois.close();
		} else {
			System.err.println("Remote bandwidth directory " + remoteBand + " not found");
			System.exit(-1);
			return;
		}

		CombineCalibrations cc = new CombineCalibrations(localCal, remoteCals, localBand, remoteBands);
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(args[0]));
		oos.writeObject(cc);
		oos.close();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("localCal:\n" + localCal);
		sb.append("\n");
		sb.append("remoteCal:\n" + localCal);
		sb.append("\n");
		sb.append("localBand:\n" + localBand);
		sb.append("\n");
		sb.append("remoteBand:\n" + localBand);
		sb.append("\n");
		return sb.toString();
	}
}
