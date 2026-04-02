package edu.upenn.cis.orchestra.p2pqp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.p2pqp.DHTService.DHTException;
import edu.upenn.cis.orchestra.util.StreamOutputBuffer;

class SpillingTupleCollection<M> {
	static class FullTuples<M> {
		final Id id;
		final List<QpTuple<M>> tuples;

		FullTuples(Id id, List<QpTuple<M>> tuples) {
			this.id = id;
			this.tuples = tuples;
		}
	}


	private final ScratchFileGenerator sfg;
	private List<File> scratchFiles = new ArrayList<File>();
	private File currentFilename = null;
	private StreamOutputBuffer currFile = null;

	private final long maxFileSize;
	private final byte[] idScratchBytes;

	private boolean sorted = true;
	private final QpSchema schema;

	private int size = 0;

	private final int numProcessorsToUse;

	SpillingTupleCollection(ScratchFileGenerator sfg, QpSchema schema) {
		this(sfg,schema,Runtime.getRuntime().availableProcessors());
	}

	SpillingTupleCollection(ScratchFileGenerator sfg, QpSchema schema, int numProcessors) {
		this.sfg = sfg;

		this.numProcessorsToUse = numProcessors;
		long maxMemory = Runtime.getRuntime().maxMemory();
		if (maxMemory == Long.MAX_VALUE) {
			maxFileSize = 256 * 1024 * 1024 / numProcessorsToUse; // 512MB
		} else {
			maxFileSize = maxMemory / (10 * numProcessorsToUse);
		}
		this.schema = schema;

		idScratchBytes = new byte[IntType.bytesPerInt * schema.getNumHashCols()];
	}

	public void finalize() {
		if (currFile != null) {
			try {
				currFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			scratchFiles.add(currentFilename);
			currFile = null;
			currentFilename = null;
		}
		for (File f : scratchFiles) {
			f.delete();
		}
		scratchFiles.clear();
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		return size;
	}

	private Collection<List<QpTuple<M>>> clashes;

	static class KeyViolation extends DHTException {
		private static final long serialVersionUID = 1L;
		KeyViolation(String clashes) {
			super("Found key violations in stored tuples: " + clashes);
		}
	}

	public Iterator<FullTuples<M>> iterator() throws KeyViolation, InterruptedException {
		if (isEmpty()) {
			List<FullTuples<M>> empty = Collections.emptyList();
			return empty.iterator();
		}
		try {
			if (! sorted) {
				clashes = sort();
			}
			if (! clashes.isEmpty()) {
				throw new KeyViolation(clashes.toString());
			}
			if (scratchFiles.size() != 1) {
				throw new IllegalStateException("Should have one sorted scratch file after sorting");
			}
			return new Iterator<FullTuples<M>>() {
				DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(scratchFiles.get(0)), 256 * 1024));
				FullTuples<M> next;
				{
					dis.readInt();
					if (! dis.readBoolean()) {
						throw new IllegalStateException("Scratch file " + scratchFiles.get(0) + " is not sorted");
					}
					readNext();
				}

				@Override
				public boolean hasNext() {
					return next != null;
				}

				@Override
				public FullTuples<M> next() {
					if (next == null) {
						throw new NoSuchElementException();
					}
					try {
						FullTuples<M> retval = next;
						readNext();
						return retval;
					} catch (IOException ex) {
						throw new RuntimeException("Error in SerializedTuples Iterator", ex);
					}
				}

				private void readNext() throws IOException {
					Id id;
					try {
						id = Id.readFrom(dis);
					} catch (EOFException ex) {
						next = null;
						dis.close();
						return;
					}
					int numTuples = dis.readInt();
					List<QpTuple<M>> tuples = new ArrayList<QpTuple<M>>(numTuples);
					for (int i = 0; i < numTuples; ++i) {
						int tupleLen = dis.readInt();
						byte[] tuple = new byte[tupleLen];
						dis.readFully(tuple);
						QpTuple<M> t = QpTuple.fromStoreBytes(schema, tuple);
						tuples.add(t);
					}
					next = new FullTuples<M>(id, tuples);
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}


				public void finalize() {
					try {
						dis.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};
		} catch (IOException e) {
			throw new RuntimeException("Caught IOException while creating iterator", e);
		}
	}
	private int numRecordsInFile;

	void add(QpTuple<M> t) throws IOException {
		if (currentFilename == null) {
			currentFilename = sfg.getFile();
			currFile = new StreamOutputBuffer(new BufferedOutputStream(new FileOutputStream(currentFilename), 128 * 1024));
			// Placeholder for number of records in file
			currFile.writeInt(0);
			// Indicate that this file is not sorted (yet)
			currFile.writeBoolean(false);
			numRecordsInFile = 0;
		}
		Id id = t.getQPid(idScratchBytes);
		id.writeTo(currFile);
		t.putStoreBytes(currFile);
		++numRecordsInFile;
		++size;
		if (currFile.size() >= maxFileSize) {
			closeCurrentFile();
		}
		sorted = false;
	}

	private void closeCurrentFile() throws IOException {
		currFile.close();
		currFile = null;
		RandomAccessFile raf = new RandomAccessFile(currentFilename,"rw");
		raf.seek(0);
		raf.writeInt(numRecordsInFile);
		raf.close();
		scratchFiles.add(currentFilename);
		currentFilename = null;
	}

	private static final int maxOpenFiles = 100;

	private static class IdAndTuple implements Comparable<IdAndTuple> {
		final Id id;
		final byte[] tuple;

		private IdAndTuple(Id id, byte[] tuple) {
			this.id = id;
			this.tuple = tuple;
		}

		@Override
		public int compareTo(IdAndTuple o) {
			return id.compareTo(o.id);
		}
	}

	private class SortThread extends Thread {
		final File inFile;
		final boolean noMerge;
		File outFile;
		List<List<QpTuple<M>>> clashes = new ArrayList<List<QpTuple<M>>>();
		IOException err;
		final Queue<SortThread> toStart;

		SortThread(File inFile, boolean noMerge, Queue<SortThread> toStart) {
			this.inFile = inFile;
			this.noMerge = noMerge;
			this.toStart = toStart;
		}

		public void run() {
			try {
				DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(inFile), 128 * 1024));
				int numRecords = dis.readInt();
				boolean isSorted = dis.readBoolean();
				if (isSorted) {
					dis.close();
					outFile = inFile;
					return;
				}
				File sortedFilename = sfg.getFile();
				System.err.println("Sorting temp file " + inFile + " to "  + sortedFilename);
				final IdAndTuple[] records = new IdAndTuple[numRecords];
				int pos = 0;
				for ( ; ; ) {
					Id id;
					try {
						id = Id.readFrom(dis);
					} catch (EOFException ex) {
						break;
					}
					int tupleLen = dis.readInt();
					byte[] tupleBytes = new byte[tupleLen];
					dis.readFully(tupleBytes);
					records[pos++] = new IdAndTuple(id, tupleBytes);
				}
				dis.close();
				inFile.delete();
				Arrays.sort(records);

				DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(sortedFilename), 128 * 1024));
				dos.writeInt(numRecords);
				// Indicate that file is sorted
				dos.writeBoolean(true);
				List<byte[]> forSameId = new ArrayList<byte[]>();
				for (pos = 0; pos < records.length; ++pos) {
					IdAndTuple curr = records[pos];
					forSameId.clear();
					forSameId.add(curr.tuple);
					while (pos + 1 < records.length && records[pos+1].id.equals(curr.id)) {
						++pos;
						forSameId.add(records[pos].tuple);
					}
					curr.id.writeTo(dos);
					dos.writeInt(forSameId.size());
					for (byte[] tuple : forSameId) {
						dos.writeInt(tuple.length);
						dos.write(tuple);
						if (noMerge) {
							clashes.addAll(findClashes(forSameId));
						}
					}
				}
				dos.close();
				outFile = sortedFilename;
				System.err.println("Finished sorting temp file " + inFile + " to "  + outFile);
			} catch (IOException ex) {
				this.err = ex;
			} finally {
				Thread t;
				synchronized (toStart) {
					t = toStart.poll();
				}
				if (t != null) {
					t.start();
				}
			}
		}
	}

	private Collection<List<QpTuple<M>>> sort() throws IOException, InterruptedException {
		if (currentFilename != null) {
			closeCurrentFile();
		}

		// Sort the individual scratch files
		final boolean noMerge = scratchFiles.size() == 1;
		// Collections of clashing tuples
		List<List<QpTuple<M>>> retval = new ArrayList<List<QpTuple<M>>>();
		Queue<SortThread> toStart = new ArrayDeque<SortThread>(scratchFiles.size());
		System.err.println("Starting to sort, data files: " + scratchFiles);

		List<SortThread> sts = new ArrayList<SortThread>();


		for (File f : scratchFiles) {
			SortThread st = new SortThread(f, noMerge, toStart); 
			sts.add(st);
			toStart.add(st);
		}

		for (int i = 0; i < numProcessorsToUse; ++i) {
			Thread t;
			synchronized (toStart) {
				t = toStart.poll();
			}
			if (t != null) {
				t.start();
			}
		}

		for (SortThread st : sts) {
			st.join();
		}
		List<File> sorted = new ArrayList<File>(scratchFiles.size());
		for (SortThread st : sts) {
			if (st.err != null) {
				throw st.err;
			}
			retval.addAll(st.clashes);
			sorted.add(st.outFile);
		}
		scratchFiles = sorted;

		class OpenFile implements Comparable<OpenFile> {
			final File f;
			final int numRecords;
			final DataInputStream input;
			Id nextId;
			private int numRemainingForNextId;

			OpenFile(File f) throws IOException {
				this.f = f;
				input = new DataInputStream(new BufferedInputStream(new FileInputStream(f), 64 * 1024));
				numRecords = input.readInt();
				boolean isSorted = input.readBoolean();
				if (! isSorted) {
					throw new IllegalStateException("File should be sorted");
				}
				nextId = Id.readFrom(input);
				numRemainingForNextId = input.readInt();
			}

			byte[] readNextTuple() throws IOException {
				int tupleLength = input.readInt();
				byte[] retval = new byte[tupleLength];
				input.readFully(retval);
				--numRemainingForNextId;
				if (numRemainingForNextId > 0) {
					return retval;
				}
				try {
					nextId = Id.readFrom(input);
					numRemainingForNextId = input.readInt();
				} catch (EOFException ex) {
					nextId = null;
					input.close();
					f.delete();
				}
				return retval;
			}

			@Override
			public int compareTo(OpenFile o) {
				return nextId.compareTo(o.nextId);
			}
		}

		// Merge sorted files into one sorted file, iteratively if necessary
		while (scratchFiles.size() > 1) {
			List<List<File>> filesToMerge = new ArrayList<List<File>>(scratchFiles.size() / maxOpenFiles + 1);
			filesToMerge.add(new ArrayList<File>(maxOpenFiles));
			for (File f : scratchFiles) {
				if (filesToMerge.get(filesToMerge.size() - 1).size() >= maxOpenFiles) {
					filesToMerge.add(new ArrayList<File>(maxOpenFiles));
				}
				filesToMerge.get(filesToMerge.size() - 1).add(f);
			}
			final boolean lastMerge = filesToMerge.size() == 1;
			List<File> merged = new ArrayList<File>(scratchFiles.size() / maxOpenFiles + 1);
			for (List<File> toMerge : filesToMerge) {
				File out = sfg.getFile();
				System.err.println("Merging sorted temp files " + toMerge + " to " + out);
				int totalNumRecords = 0;
				PriorityQueue<OpenFile> findNextTuple = new PriorityQueue<OpenFile>(toMerge.size());
				for (File f : toMerge) {
					findNextTuple.add(new OpenFile(f));
				}
				for (OpenFile of : findNextTuple) {
					totalNumRecords += of.numRecords;
				}
				DataOutputStream outHandle = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(out), 512 * 1024));
				outHandle.writeInt(totalNumRecords);
				// Merged file is sorted
				outHandle.writeBoolean(true);

				final List<byte[]> forSameId = new ArrayList<byte[]>();
				OpenFile curr = findNextTuple.remove();
				Id currId = curr.nextId;
				forSameId.add(curr.readNextTuple());
				if (curr.nextId != null) {
					findNextTuple.add(curr);
				}
				curr = null;
				while (! findNextTuple.isEmpty()) {
					curr = findNextTuple.remove();
					if (! curr.nextId.equals(currId)) {
						currId.writeTo(outHandle);
						outHandle.writeInt(forSameId.size());
						for (byte[] tuple : forSameId) {
							outHandle.writeInt(tuple.length);
							outHandle.write(tuple);
						}
						if (lastMerge) {
							retval.addAll(findClashes(forSameId));
						}
						forSameId.clear();
						currId = curr.nextId;
					}
					forSameId.add(curr.readNextTuple());
					if (curr.nextId != null) {
						findNextTuple.add(curr);
					}
				}
				if (lastMerge) {
					retval.addAll(findClashes(forSameId));
				}
				currId.writeTo(outHandle);
				outHandle.writeInt(forSameId.size());
				for (byte[] tuple : forSameId) {
					outHandle.writeInt(tuple.length);
					outHandle.write(tuple);
				}
				forSameId.clear();
				currId = null;
				outHandle.close();
				merged.add(out);
			}
			scratchFiles = merged;
		}
		this.sorted = true;
		return retval;
	}

	private Collection<List<QpTuple<M>>> findClashes(List<byte[]> serialized) {
		if (serialized.size() == 1) {
			return Collections.emptyList();
		}
		Map<QpTupleKey,List<QpTuple<M>>> clashes = new HashMap<QpTupleKey,List<QpTuple<M>>>();
		boolean found = false;
		for (byte[] b : serialized) {
			QpTuple<M> t = QpTuple.fromStoreBytes(schema, b);
			QpTupleKey key = t.getKeyTuple(0);
			List<QpTuple<M>> ts = clashes.get(key);
			if (ts == null) {
				ts = new ArrayList<QpTuple<M>>(2);
				clashes.put(key, ts);
			} else {
				found = true;
			}
			ts.add(t);
		}
		if (! found) {
			return Collections.emptyList();
		}
		Iterator<List<QpTuple<M>>> it = clashes.values().iterator();
		while (it.hasNext()) {
			List<QpTuple<M>> s = it.next();
			if (s.size() == 1) {
				it.remove();
			}
		}
		return clashes.values();
	}
}
