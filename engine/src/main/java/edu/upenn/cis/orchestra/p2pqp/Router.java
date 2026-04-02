package edu.upenn.cis.orchestra.p2pqp;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class Router implements Serializable {
	private static final long serialVersionUID = 1L;

	public static class NodeInfo implements Comparable<NodeInfo>, Serializable {
		private static final long serialVersionUID = 1L;
		public final Id id;
		public final InetSocketAddress qpAddress;

		public NodeInfo(Id id, InetSocketAddress qpAddress) {
			this.id = id;
			this.qpAddress = qpAddress;
		}

		public String toString() {
			return "ID: " + id + " Address: " + qpAddress;
		}

		public int compareTo(NodeInfo arg0) {
			return id.compareTo(arg0.id);
		}

		public void serialize(OutputBuffer buf) {
			id.serialize(buf);
			buf.writeInetSocketAddress(qpAddress);
		}

		public static NodeInfo deserialize(InputBuffer buf) {
			Id id = Id.deserialize(buf);
			InetSocketAddress qpAddress = buf.readInetSocketAddress();
			return new NodeInfo(id,qpAddress);
		}
	}
	
	private static class RangeInfo implements Comparable<RangeInfo>, Serializable {
		private static final long serialVersionUID = 1L;
		private final IdRange idRange;
		private final InetSocketAddress qpAddress;

		private RangeInfo(IdRange idRange, InetSocketAddress qpAddress) {
			this.idRange = idRange;
			this.qpAddress = qpAddress;
		}
		
		private RangeInfo(Id CCW, Id CW, InetSocketAddress qpAddress) {
			this(new IdRange(CCW, CW), qpAddress);
		}

		public String toString() {
			return idRange + ": "  + qpAddress;
		}

		public int compareTo(RangeInfo arg0) {
			return idRange.compareTo(arg0.idRange);
		}

		private void serialize(OutputBuffer buf) {
			idRange.serialize(buf);
			buf.writeInetSocketAddress(qpAddress);
		}

		private static RangeInfo deserialize(InputBuffer buf) {
			IdRange idRange = IdRange.deserialize(buf);
			InetSocketAddress qpAddress = buf.readInetSocketAddress();
			return new RangeInfo(idRange,qpAddress);
		}		
	}

	public enum Type {
		CHORD, PASTRY, EVEN
	};
	

	public static Router createRouter(Collection<NodeInfo> nis, int replicationFactor, Type routerType) {
		if (replicationFactor < 1 || replicationFactor %2 != 1) {
			throw new IllegalArgumentException("Replication factor must be odd");
		}
		List<NodeInfo> nodes = new ArrayList<NodeInfo>(nis);
		Collections.sort(nodes);
		
		final int numNodes = nodes.size();
		RangeInfo[] ownedRanges = new RangeInfo[numNodes];
		if (routerType == Type.CHORD) {
			for (int i = 0; i < numNodes - 1; ++i) {
				ownedRanges[i] = new RangeInfo(nodes.get(i).id, nodes.get(i+1).id, nodes.get(i).qpAddress); 
			}
			ownedRanges[numNodes-1] = new RangeInfo(nodes.get(numNodes-1).id, nodes.get(0).id, nodes.get(numNodes-1).qpAddress);
		} else if (routerType == Type.EVEN) {
			BigInteger allocSize = Id.MAX_BIGINT.divide(BigInteger.valueOf(numNodes));
			BigInteger start = BigInteger.ZERO;
			Id startId = new Id(start);
			for (int i = 0; i < numNodes - 1; ++i) {
				BigInteger next = start.add(allocSize);
				Id nextId = new Id(next);
				ownedRanges[i] = new RangeInfo(startId, nextId, nodes.get(i).qpAddress);
				start = next;
				startId = nextId;
			}
			ownedRanges[numNodes - 1] = new RangeInfo(startId, Id.ZERO, nodes.get(numNodes - 1).qpAddress);
		} else if (routerType == Type.PASTRY) {
			if (numNodes == 1) {
				ownedRanges[0] = new RangeInfo(IdRange.full(), nodes.get(0).qpAddress);
			} else {
				Id lower = nodes.get(numNodes - 1).id.findHalfway(nodes.get(0).id);
				Id firstStart = lower;
				for (int i = 0; i < numNodes - 1; ++i) {
					Id upper = nodes.get(i).id.findHalfway(nodes.get(i+1).id);
					ownedRanges[i] = new RangeInfo(lower, upper, nodes.get(i).qpAddress);
					lower = upper;
				}
				ownedRanges[numNodes -1] = new RangeInfo(lower, firstStart, nodes.get(numNodes-1).qpAddress);
			}
		} else {
			throw new IllegalStateException("Need to support router type " + routerType);
		}
		Arrays.sort(ownedRanges);
		
		RangeInfo availableRanges[];
		if (replicationFactor == 1) {
			availableRanges = ownedRanges;
		} else if (replicationFactor >= numNodes) {
			availableRanges = new RangeInfo[numNodes];
			for (int i = 0; i < numNodes; ++i) {
				availableRanges[i] = new RangeInfo(IdRange.full(), nodes.get(i).qpAddress);
			}
		} else if (routerType == Type.PASTRY || routerType == Type.CHORD || routerType == Type.EVEN) {
			int half = replicationFactor / 2;
			availableRanges = new RangeInfo[numNodes];
			for (int i = 0; i < numNodes; ++i) {
				int start = i - half;
				if (start < 0) {
					start += numNodes;
				}
				int end = i + half;
				if (end >= numNodes) {
					end -= numNodes;
				}
				availableRanges[i] = new RangeInfo(ownedRanges[start].idRange.getCCW(), ownedRanges[end].idRange.getCW(), ownedRanges[i].qpAddress);
			}
		} else {
			throw new IllegalStateException("Need to support router type " + routerType);
		}
		Arrays.sort(availableRanges);
		return new Router(ownedRanges, availableRanges, nis, replicationFactor, routerType);
	}
	
	private Router(RangeInfo[] ownedRanges, RangeInfo[] availableRanges, Collection<NodeInfo> nis, int replicationFactor, Type routerType) {
		if (replicationFactor < 1 || replicationFactor %2 != 1) {
			throw new IllegalArgumentException("Replication factor must be odd");
		}
		this.ownedRanges = ownedRanges;
		this.availableRanges = availableRanges;
		this.replicationFactor = replicationFactor;
		this.routerType = routerType;
		this.nis = new ArrayList<NodeInfo>(nis);
		Map<InetSocketAddress,IdRangeSet> findOwnedRanges = new HashMap<InetSocketAddress,IdRangeSet>();
		for (RangeInfo ri : ownedRanges) {
			IdRangeSet owned = findOwnedRanges.get(ri.qpAddress);
			if (owned == null) {
				owned = IdRangeSet.empty();
				findOwnedRanges.put(ri.qpAddress, owned);
			}
			owned.add(ri.idRange);
		}
		this.findOwnedRanges = Collections.unmodifiableMap(findOwnedRanges);
		
		Map<InetSocketAddress,Id> findNodeId = new HashMap<InetSocketAddress,Id>(nis.size());
		for (NodeInfo ni : nis) {
			findNodeId.put(ni.qpAddress, ni.id);
		}
		this.findNodeId = Collections.unmodifiableMap(findNodeId);
	}
	
	private final Type routerType;
	private final int replicationFactor;
	private final RangeInfo[] ownedRanges;
	private final RangeInfo[] availableRanges;
	private final Map<InetSocketAddress,IdRangeSet> findOwnedRanges;
	private final Map<InetSocketAddress,Id> findNodeId;
	private final Collection<NodeInfo> nis;
	
	public IdRangeSet getOwnedRanges(InetSocketAddress isa) {
		IdRangeSet found = findOwnedRanges.get(isa);
		if (found == null) {
			return null;
		} else {
			return found.clone();
		}
	}
	
	public InetSocketAddress getDest(Id id) {
		return ownedRanges[getRangeInfoIndex(this.ownedRanges, id)].qpAddress;
	}
	
	public Set<InetSocketAddress> getDests(Id id) {
		Set<InetSocketAddress> retval = new HashSet<InetSocketAddress>();
		int centerIndex = getRangeInfoIndex(availableRanges, id);
		retval.add(availableRanges[centerIndex].qpAddress);
		int lowIndex = centerIndex, highIndex = centerIndex;
		final int end = availableRanges.length - 1;
		for ( ; ; ) {
			int lower = lowIndex - 1;
			if (lower < 0) {
				lower = end;
			}
			if (lower == centerIndex) {
				break;
			}
			if (availableRanges[lower].idRange.contains(id)) {
				lowIndex = lower;
				retval.add(availableRanges[lowIndex].qpAddress);
			} else {
				break;
			}
		}
		for ( ; ; ) {
			int higher = highIndex + 1;
			if (higher > end) {
				higher = 0;
			}
			if (higher == centerIndex) {
				break;
			}
			if (availableRanges[higher].idRange.contains(id)) {
				highIndex = higher;
				retval.add(availableRanges[highIndex].qpAddress);
			} else {
				break;
			}
		}
		return retval;
	}
	
	public Set<InetSocketAddress> getDests(IdRange range) {
		if (range.isFull()) {
			return findOwnedRanges.keySet();
		} else if (range.isEmpty()) {
			return Collections.emptySet();
		}
		
		final int startIndex = getRangeInfoIndex(ownedRanges, range.getCCW());
		int endIndex = getRangeInfoIndex(ownedRanges, range.getCW());
		if (! ownedRanges[endIndex].idRange.intersects(range)) {
			--endIndex;
			if (endIndex < 0) {
				endIndex = ownedRanges.length - 1;
			}
		}
		Set<InetSocketAddress> retval = new HashSet<InetSocketAddress>();
		if (endIndex >= startIndex) {
			// Relevant nodes do not wrap
			for (int i = startIndex; i <= endIndex; ++i) {
				retval.add(ownedRanges[i].qpAddress);
			}
		} else {
			// Relevant nodes wrap
			for (int i = 0; i <= endIndex; ++i) {
				retval.add(ownedRanges[i].qpAddress);
			}
			for (int i = startIndex; i < ownedRanges.length; ++i) {
				retval.add(ownedRanges[i].qpAddress);
			}
		}
		
		return retval;
	}
	
	public Set<InetSocketAddress> getParticipants() {
		return findOwnedRanges.keySet();
	}
	
	public int size() {
		return findOwnedRanges.size();
	}
	
	public void serialize(OutputBuffer buf) {
		buf.writeInt(routerType.ordinal());
		buf.writeInt(replicationFactor);
		buf.writeInt(ownedRanges.length);
		for (RangeInfo ri : ownedRanges) {
			ri.serialize(buf);
		}
		buf.writeInt(availableRanges.length);
		for (RangeInfo ri : availableRanges) {
			ri.serialize(buf);
		}
		buf.writeInt(nis.size());
		for (NodeInfo ni : nis) {
			ni.serialize(buf);
		}
	}
	
	private static final Type[] routerTypes = Type.values();
	
	public static Router deserialize(InputBuffer buf) {
		Type routerType = routerTypes[buf.readInt()];
		int replicationFactor = buf.readInt();
		int numOwnedRanges = buf.readInt();
		RangeInfo ownedRanges[] = new RangeInfo[numOwnedRanges];
		for (int i = 0; i < numOwnedRanges; ++i) {
			ownedRanges[i] = RangeInfo.deserialize(buf);
		}
		int numAvailableRanges = buf.readInt();
		RangeInfo[] availableRanges = new RangeInfo[numAvailableRanges];
		for (int i = 0; i < numAvailableRanges; ++i) {
			availableRanges[i] = RangeInfo.deserialize(buf);
		}
		int numNodes = buf.readInt();
		Collection<NodeInfo> nis = new ArrayList<NodeInfo>(numNodes);
		for (int i = 0; i < numNodes; ++i) {
			nis.add(NodeInfo.deserialize(buf));
		}
		return new Router(ownedRanges, availableRanges, nis, replicationFactor, routerType);
	}
	
	Router getRouterWithout(Set<InetSocketAddress> failedNodes) {
		Collection<NodeInfo> nis = new ArrayList<NodeInfo>();
		for (NodeInfo ni : this.nis) {
			if (! failedNodes.contains(ni.qpAddress)) {
				nis.add(ni);
			}
		}
		return createRouter(nis, this.replicationFactor, this.routerType);
	}
	
	public Collection<NodeInfo> getNodeInfo() {
		return Collections.unmodifiableCollection(this.nis);
	}
	
	Router createRecoveryRouter(Set<InetSocketAddress> failedNodes) {
		IdRangeSet failedRanges = IdRangeSet.empty();
		for (InetSocketAddress failedNode : failedNodes) {
			failedRanges.add(this.getOwnedRanges(failedNode));
		}
		
		// TODO: do something more sophisticated here, this is a hack
		// A correct solution would probably express this as a linear programming
		// optimization problem subject to constraints
		
		Map<InetSocketAddress,IdRangeSet> newOwnedRanges = new HashMap<InetSocketAddress,IdRangeSet>(findOwnedRanges.size() - failedNodes.size());
		
		for (Map.Entry<InetSocketAddress, IdRangeSet> me : findOwnedRanges.entrySet()) {
			if (! failedNodes.contains(me.getKey())) {
				newOwnedRanges.put(me.getKey(), me.getValue().clone());
			}
		}
		
		List<RangeInfo> availableRangesList = new ArrayList<RangeInfo>(availableRanges.length);
		for (RangeInfo ri : this.availableRanges) {
			if (! failedNodes.contains(ri.qpAddress)) {
				availableRangesList.add(ri);
			}
		}
		
		RangeInfo[] newAvailableRanges = availableRangesList.toArray(new RangeInfo[availableRangesList.size()]);
		
		List<NodeInfo> newNis = new ArrayList<NodeInfo>(nis.size());
		for (NodeInfo ni : nis) {
			if (! failedNodes.contains(ni.qpAddress)) {
				newNis.add(ni);
			}
		}
		
		for (IdRange failed : failedRanges) {
			int centerIndex = this.getRangeInfoIndex(newAvailableRanges, failed.getCCW());
			int lowIndex = centerIndex, highIndex = centerIndex;
			final int end = newAvailableRanges.length - 1;
			for ( ; ; ) {
				int lower = lowIndex - 1;
				if (lower < 0) {
					lower = end;
				}
				if (newAvailableRanges[lower].idRange.intersects(failed)) {
					lowIndex = lower;
				} else {
					break;
				}
				if (lower == centerIndex) {
					break;
				}
			}
			for ( ; ; ) {
				int higher = highIndex + 1;
				if (higher == centerIndex) {
					break;
				}
				if (higher > end) {
					higher = 0;
				}
				if (newAvailableRanges[higher].idRange.intersects(failed)) {
					highIndex = higher;
				} else {
					break;
				}
			}
			int numRanges = highIndex - lowIndex + 1;
			if (numRanges <= 0) {
				numRanges += newAvailableRanges.length;
			}
			
			IdRange[] partitioned = failed.split(numRanges);

			for (int i = 0; i < numRanges; ++i) {
				int availableIndex = lowIndex + i;
				RangeInfo curr = newAvailableRanges[availableIndex % newAvailableRanges.length];
				if (curr.idRange.contains(partitioned[i])) {
					newOwnedRanges.get(curr.qpAddress).add(curr.idRange.intersect(partitioned[i]));
				} else {
					throw new IllegalStateException("Couldn't partition ranges for recovery router");
				}
			}
		}
		
		List<RangeInfo> ownedRangesList = new ArrayList<RangeInfo>();
		for (Map.Entry<InetSocketAddress, IdRangeSet> me : newOwnedRanges.entrySet()) {
			for (IdRange range : me.getValue()) {
				ownedRangesList.add(new RangeInfo(range, me.getKey()));
			}
		}
		
		RangeInfo[] ownedRangesArray = ownedRangesList.toArray(new RangeInfo[ownedRangesList.size()]);
		Arrays.sort(ownedRangesArray);

		return new Router(ownedRangesArray, newAvailableRanges, newNis, replicationFactor, routerType);
	}
	
	private int getRangeInfoIndex(RangeInfo[] array, Id id) {
		final int end = array.length - 1;
		int min = 0, max = end;

		
		while (max - min > 1) {
			int half = (min + max) / 2;
			RangeInfo pivot = array[half];
			if (pivot.idRange.contains(id)) {
				max = half;
				min = half;
			} else if (id.compareTo(pivot.idRange.getCCW()) < 0) {
				max = half - 1;
				if (max < min) {
					max = min;
				}
			} else {
				min = half + 1;
				if (min > max) {
					min = max;
				}
			}
		}

		if (array[min].idRange.contains(id)) {
			return min;
		} else if (array[max].idRange.contains(id)) {
			return max;
		}
		if (min == 0 || min == end || max == 0 || max == end) {
			if (array[end].idRange.contains(id)) {
				return end;
			}
			if (array[0].idRange.contains(id)) {
				return 0;
			}
		}

		throw new IllegalStateException("Couldn't find node for " + id + ", known ranges are " + array);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (RangeInfo ri : this.ownedRanges) {
			sb.append(ri);
			sb.append(' ');
			sb.append(findNodeId.get(ri.qpAddress));
			sb.append('\n');
		}
		InetSocketAddress maxNode = null, minNode = null;
		double maxFrac = 0.0, minFrac = Double.MAX_VALUE;
		for (Map.Entry<InetSocketAddress, IdRangeSet> me : this.findOwnedRanges.entrySet()) {
			final InetSocketAddress node = me.getKey();
			final double frac = me.getValue().remainingFrac();
			if (frac > maxFrac) {
				maxFrac = frac;
				maxNode = node;
			}
			if (frac < minFrac) {
				minFrac = frac;
				minNode = node;
			}
		}
		sb.append("Max frac is " + maxFrac + " at " + maxNode + "\n");
		sb.append("Min frac is " + minFrac + " at " + minNode + "\n");
		
		double ratio = maxFrac * this.size();
		sb.append("Max node has " + ratio + " times even distribution");
		return sb.toString();
	}
	
	public String getAvailableRanges() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < availableRanges.length - 1; ++i) {
			sb.append(availableRanges[i]);
			sb.append('\n');
		}
		sb.append(availableRanges[availableRanges.length - 1]);
		return sb.toString();
	}
}
