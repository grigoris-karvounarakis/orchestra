package edu.upenn.cis.orchestra.p2pqp;

import static edu.upenn.cis.orchestra.p2pqp.Id.ZERO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;

public class SearchTree {
	private final List<Node> nodes = new ArrayList<Node>();
	private final int root;

	public static final int DEFAULT_ARITY = 5;

	/**
	 * Build a search tree from a serialized tree
	 * 
	 * @param byte[]			The serialized tree
	 */
	SearchTree(byte[] serialized) {
		ByteBufferReader bbr = new ByteBufferReader(serialized);
		root = bbr.readInt();
		while (! bbr.hasFinished()) {
			byte[] node = bbr.readByteArray();
			Node newNode = new Node(node);
			nodes.add(newNode);
		}
	}

	/**
	 * Build a new search tree, using page numbers starting
	 * at <code>newRootPageId</code>
	 * 
	 * @param pageBots					The minimum DHT id value that appears on each page
	 */
	SearchTree (List<Id> pageBots) {
		this(DEFAULT_ARITY, pageBots);
	}

	/**
	 * Build a new search tree, using page numbers starting
	 * at <code>newRootPageId</code>
	 * 
	 * @param arity						The fan-out of the tree
	 * @param pageBots					The minimum DHT id value that appears on each page
	 */
	SearchTree(final int arity, List<Id> pageBots) {
		if (arity < 3) {
			throw new IllegalArgumentException("Arity must be at least 3");
		}
		final int size = pageBots.size(); 

		List<Id> boundaries = pageBots.subList(1, pageBots.size());
		List<Integer> pointers = new ArrayList<Integer>(boundaries.size() + 1);

		for (int i = 0; i < size; ++i) {
			pointers.add(-(i+1));
		}

		// Need to recursively build tree

		// TODO: make the tree more balanced if we decide
		// we want to do incremental maintenance 
		while (pointers.size() > arity) {
			int sizeGuess = pointers.size() / arity + 1;
			int boundariesPos = 0, pointersPos = 0;
			List<Id> newBoundaries = new ArrayList<Id>(sizeGuess);
			List<Integer> newPointers = new ArrayList<Integer>(sizeGuess + 1);
			while (pointersPos < pointers.size()) {
				int pointersRemaining = pointers.size() - pointersPos;
				int numPointers = pointersRemaining > arity ? arity : pointersRemaining;
				newPointers.add(addNode(boundaries.subList(boundariesPos, boundariesPos + numPointers - 1), pointers.subList(pointersPos, pointersPos + numPointers)));
				pointersPos += numPointers;
				boundariesPos += numPointers - 1;
				if (boundariesPos < boundaries.size()) {
					newBoundaries.add(boundaries.get(boundariesPos));
					boundariesPos++;
				}
			}
			boundaries = newBoundaries;
			pointers = newPointers;
		}

		// Build top node
		root = addNode(boundaries, pointers);
	}


	private class Node {
		private byte[] serialized;

		private Id ids[];
		private int[] pointers;

		Node(List<Id> ids, List<Integer> pointers) {
			this.ids = ids.toArray(new Id[ids.size()]);
			this.pointers = new int[pointers.size()];
			int count = 0;
			for (int pointer : pointers) {
				this.pointers[count++] = pointer;
			}
			if (pointers.isEmpty() || pointers.size() != ids.size() + 1) {
				throw new IllegalArgumentException("Invalid keys and pointers");
			}
		}

		Node(byte[] serialized) {
			this.serialized = serialized;
		}

		byte[] serialize() {
			if (serialized != null) {
				return serialized;
			}
			ByteBufferWriter bbw = new ByteBufferWriter();
			bbw.addToBuffer(ids.length);
			for (Id id : ids) {
				bbw.addToBufferNoLength(id.getMSBBytes());
			}
			for (int p : pointers) {
				bbw.addToBuffer(p);
			}
			return bbw.getByteArray();
		}

		private void deserialize() {
			if (ids != null) {
				return;
			}
			ByteBufferReader bbr = new ByteBufferReader(serialized);
			int numIds = bbr.readInt();
			ids = new Id[numIds];
			pointers = new int[numIds + 1];
			for (int i = 0; i < numIds; ++i) {
				byte[] id = bbr.readByteArrayNoLength(Id.idLengthBytes);
				ids[i] = Id.fromMSBBytes(id);
			}
			for (int i = 0; i <= numIds; ++i) {
				int nodeId = bbr.readInt();
				pointers[i] = nodeId;
			}
		}

		int route(Id probe) {
			deserialize();
			if (ids.length == 0) {
				return pointers[0];
			}
			int pos = Arrays.binarySearch(ids, probe);
			int pointer;
			if (pos < 0) {
				pointer = -pos - 1;
			} else {
				pointer = pos + 1;
			}
			return pointers[pointer];
		}

		public String toString() {
			if (ids == null) {
				deserialize();
			}
			return "(" + Arrays.toString(ids) + "," + Arrays.toString(pointers) + ")";
		}
		
		private void getRelevantPages(IdRange range, Collection<Integer> pages) {
			deserialize();
			final int numPointers = pointers.length;
			if (numPointers == 1) {
				SearchTree.this.getRelevantPages(range, pages, pointers[0]);
				return;
			}
			
			if (range.intersects(new IdRange(ZERO,ids[0]))) {
				SearchTree.this.getRelevantPages(range, pages,pointers[0]);
			}
			
			for (int i = 1; i < numPointers - 1; ++i) {
				IdRange pointerRange = new IdRange(ids[i-1],ids[i]);
				if (range.intersects(pointerRange)) {
					SearchTree.this.getRelevantPages(range,pages,pointers[i]);
				}
			}
			
			if (range.intersects(new IdRange(ids[numPointers-2],ZERO))) {
				SearchTree.this.getRelevantPages(range,pages,pointers[numPointers-1]);
			}
		}

	}

	private void getRelevantPages(IdRange range, Collection<Integer> pages, int pointer) {
		if (pointer < 0) {
			pages.add(-pointer - 1);
		} else {
			nodes.get(pointer).getRelevantPages(range, pages);
		}
	}
	
	private int addNode(List<Id> keys, List<Integer> pointers) {
		nodes.add(new Node(keys, pointers));
		return nodes.size() - 1;
	}

	byte[] getTree() {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(root);
		for (Node n : nodes) {
			bbw.addToBuffer(n.serialize());
		}
		return bbw.getByteArray();
	}

	int getIndexPage(Id id) {
		int p = root;
		while (p >= 0) {
			Node n = nodes.get(p);
			p = n.route(id);
		}
		return -p-1;
	}

	int getIndexPage(QpTuple<?> key) {
		return getIndexPage(key.getQPid());
	}

	List<Integer> getRelevantPages(IdRange range) {
		List<Integer> pages = new ArrayList<Integer>();
		nodes.get(root).getRelevantPages(range, pages);
		return pages;
	}	

	private List<Id> getPageBottoms() {
		List<Id> pageBots = new ArrayList<Id>();
		pageBots.add(Id.ZERO);
		addIds(root, pageBots);
		return pageBots;
	}
	
	List<IdRange> getPageRanges() {
		List<Id> pageBottoms = getPageBottoms();
		final int numPages = pageBottoms.size();
		List<IdRange> pageRanges = new ArrayList<IdRange>(numPages);
		for (int i = 0; i < numPages - 1; ++i) {
			pageRanges.add(new IdRange(pageBottoms.get(i), pageBottoms.get(i + 1)));
		}
		pageRanges.add(new IdRange(pageBottoms.get(numPages - 1), Id.ZERO));
		
		return pageRanges;
	}
	
	private void addIds(int p, List<Id> pageBots) {
		if (p < 0) {
			return;
		}
		Node n = nodes.get(p);
		if (n == null) {
			throw new RuntimeException("Could not get tree node for pointer " + p);
		}
		n.deserialize();
		for (int i = 0; i < n.ids.length; ++i) {
			addIds(n.pointers[i], pageBots);
			pageBots.add(n.ids[i]);
		}
		addIds(n.pointers[n.pointers.length-1],pageBots);
	}
}
