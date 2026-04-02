package edu.upenn.cis.orchestra.p2pqp;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Bandwidth implements Serializable {
	private static final long serialVersionUID = 1L;
	// Bandwidth between the current node and the named node in bytes
	// per second
	public final Map<String,Double> namedNodeBandwidths;
	// Latency between the current node and the named node, in seconds
	public final Map<String,Double> namedNodeLatencies;
	// Bandwidth between the current node and the remote node with
	// the lowest bandwidth, in bytes per second
	public final double worstRemoteBandwidth;
	// Latency between the current node and the remote node with
	// the highestBandwidth, in seconds;
	public final double worstRemoteLatency;
	
	public Bandwidth(Map<String,Double> namedNodeBandwidths, Map<String,Double> namedNodeLatencies,
			double worstRemoteBandwidth,
			double worstRemoteLatency) {
		this.namedNodeBandwidths = Collections.unmodifiableMap(new HashMap<String,Double>(namedNodeBandwidths));
		this.namedNodeLatencies = Collections.unmodifiableMap(new HashMap<String,Double>(namedNodeLatencies));
		this.worstRemoteBandwidth = worstRemoteBandwidth;
		this.worstRemoteLatency = worstRemoteLatency;
	}
	
	public double getTransmissionTime(double numBytes) {
		return worstRemoteLatency + (numBytes / worstRemoteBandwidth);
	}
	
	public double getTransmissionTime(double numBytes, String location) {
		return namedNodeLatencies.get(location) + (numBytes / namedNodeBandwidths.get(location)); 
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Bandwidth: " + worstRemoteBandwidth + " " + namedNodeBandwidths + "\n");
		sb.append("Latency: " + worstRemoteLatency + " " + namedNodeLatencies);
		return sb.toString();
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException {
		if (args.length == 1) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(args[0]));
			Bandwidth b = (Bandwidth) ois.readObject();
			System.out.println(b);
			ois.close();
		} else if (args.length == 3) {
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(args[0]));
			double bandwidth = Double.parseDouble(args[1]) * 1024;
			double latency = Double.parseDouble(args[2]) / 1000;
			Map<String,Double> noNamedNodes = Collections.emptyMap();
			Bandwidth b = new Bandwidth(noNamedNodes,noNamedNodes,bandwidth,latency);
			oos.writeObject(b);
			oos.close();
		} else {
			System.err.println("Usage:\toutfile bandwidth latency\n\tinfile\nbandwidth is in kb/sec\nlatency is in msec");
			System.exit(-1);
		}
	}
}
