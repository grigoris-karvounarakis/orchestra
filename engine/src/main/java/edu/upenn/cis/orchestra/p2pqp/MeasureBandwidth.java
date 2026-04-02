package edu.upenn.cis.orchestra.p2pqp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.IntType;
import static edu.upenn.cis.orchestra.p2pqp.Calibrate.computeAvg;

public class MeasureBandwidth {
	private static final int numTrials = 10;
	// Data size, kilobytes
	private static int dataSize = 1024;
	private static byte COMMENCE_SENDING = 17, DONE = 112;

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		InputStream in = null;
		OutputStream os = null;
		Integer port = null;
		for (int i = 0; i < args.length; i += 2) {
			String option = args[i].toLowerCase();
			String value = args[i + 1];
			if (option.equals("-listen")) {
				port = Integer.parseInt(value);
			} else if (option.equals("-input")) {
				in = new FileInputStream(value);
			} else if (option.equals("-output")) {
				os = new FileOutputStream(value);
			} else {
				System.err.println("Usage: (-listen portno)|([-input file] [-output file])");
				System.exit(-1);
			}
		}
		if (port != null) {
			runServer(port);
		} else {
			if (in == null) {
				in = System.in;
			}
			Bandwidth b = doPings(in);
			if (os != null) {
				ObjectOutputStream oos = new ObjectOutputStream(os);
				oos.writeObject(b);
				oos.close();
			}
			System.out.println(b);
		}
	}

	static Bandwidth doPings(InputStream in) throws IOException, InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		Set<InetSocketAddress> hosts = new HashSet<InetSocketAddress>();
		Map<String,InetSocketAddress> namedNodes = new HashMap<String,InetSocketAddress>();
		String line;
		while ((line = reader.readLine()) != null) {
			if (line.length() == 0) {
				continue;
			}
			String[] split = line.split("[\n\t\r]");
			String name = null;
			String hostAndPort;
			if (split.length == 1) {
				hostAndPort = split[0];
			} else if (split.length == 2) {
				hostAndPort = split[0];
				name = split[1];
			} else {
				throw new RuntimeException("Invalid line: " + line);
			}
			split = hostAndPort.split(":");
			if (split.length != 2) {
				throw new RuntimeException("Invalid host and port: " + hostAndPort);
			}
			String hostName = split[0];
			int port = Integer.parseInt(split[1]);
			InetSocketAddress isa = new InetSocketAddress(InetAddress.getByName(hostName), port);
			if (name == null) {
				hosts.add(isa);
			} else {
				namedNodes.put(name, isa);
			}
		}

		Bandwidth b = getBandwidth(hosts, namedNodes);
		return b;
	}

	static Bandwidth getBandwidth(Collection<InetSocketAddress> hosts, Map<String,InetSocketAddress> namedNodes) throws IOException, InterruptedException {
		double latency = 0.0, bandwidth = Double.MAX_VALUE;
		for (InetSocketAddress isa : hosts) {
			ConnectionData cd = getConnectionData(isa);
			System.out.printf("%s: bandwidth %.2f bytes/sec, latency %.5f seconds\n", isa.getHostName(), cd.bandwidth, cd.latency);
			if (latency < cd.latency) {
				latency = cd.latency;
			}
			if (bandwidth > cd.bandwidth) {
				bandwidth = cd.bandwidth;
			}
		}

		Map<String,Double> latencies = new HashMap<String,Double>(namedNodes.size());
		Map<String,Double> bandwidths = new HashMap<String,Double>(namedNodes.size());
		for (Map.Entry<String, InetSocketAddress> me : namedNodes.entrySet()) {
			ConnectionData cd = getConnectionData(me.getValue());
			System.out.printf("%s (%s): bandwidth %.2f bytes/sec, latency %.5f seconds\n", me.getKey(), me.getValue().getHostName(), cd.bandwidth, cd.latency);
			latencies.put(me.getKey(), cd.latency);
			bandwidths.put(me.getKey(), cd.bandwidth);
		}

		System.out.flush();
		
		return new Bandwidth(bandwidths, latencies, bandwidth, latency);
	}

	public static ConnectionData getConnectionData(InetSocketAddress isa) throws IOException, InterruptedException {
		byte[] dataLengthBytes = new byte[IntType.bytesPerInt];

		List<Double> latencies = new ArrayList<Double>(numTrials);
		List<Double> sendTimes = new ArrayList<Double>(numTrials);

		Socket s = new Socket(isa.getAddress(), isa.getPort());
		OutputStream os = s.getOutputStream();
		InputStream is = s.getInputStream();
		for (int j = 0; j < numTrials; ++j) {
			long startTime = System.nanoTime();

			os.write(COMMENCE_SENDING);
			os.flush();

			is.read(dataLengthBytes);
			long replyTime = System.nanoTime();

			int dataLength = IntType.getValFromBytes(dataLengthBytes);

			int remaining = 1024 * dataLength;
			byte[] data = new byte[1024];
			while (remaining > 0) {
				int read = is.read(data);
				if (read == -1) {
					throw new RuntimeException("EOF with " + remaining + " bytes to read");
				}
				remaining -= read;
				if (remaining < 1024) {
					data = new byte[remaining];
				}
			}
			long doneTime = System.nanoTime();
			int next = is.read();
			if (next != DONE) {
				throw new RuntimeException("Expected DONE after data stream");
			}

			double latency = (replyTime - startTime) / 2e9;
			double sendTime = (doneTime - replyTime) / 1e9;
			latencies.add(latency);
			sendTimes.add(sendTime);
		}
		os.write(DONE);
		os.flush();
		
		s.close();
		double latency = computeAvg(latencies);
		double sendTime = computeAvg(sendTimes);
		double bandwidth = dataSize * 1024 / sendTime;
		return new ConnectionData(bandwidth, latency);
	}

	public static class ConnectionData {
		// Bandwidth in bytes/sec
		public final double bandwidth;
		// Latency in seconds
		public final double latency;

		ConnectionData(double bandwidth, double latency) {
			this.bandwidth = bandwidth;
			this.latency = latency;
		}
	}

	static void runServer(int port) throws IOException {
		ServerSocket ss = null;

		byte[] data = new byte[1024];
		new Random().nextBytes(data);

		try {
			ss = new ServerSocket(port);
			for ( ; ; ) {
				Socket s = ss.accept();
				OutputStream os = s.getOutputStream();
				InputStream is = s.getInputStream();

				System.out.println("Processing ping from " + s.getInetAddress());
				for ( ; ; ) {
					int i = is.read();
					if (i == DONE) {
						break;
					} else if (i != COMMENCE_SENDING) {
						throw new RuntimeException("Recevied unexpected byte " + i);
					}
					os.write(IntType.getBytes(dataSize));
					os.flush();
					for (i = 0; i < dataSize; ++i) {
						os.write(data);
					}
					os.write(DONE);
					os.flush();
				}
				s.shutdownOutput();
				System.out.println("Processed ping from " + s.getInetAddress());
				s.close();
			}
		} finally {
			if (ss != null) {
				ss.close();
			}
		}
	}
}
