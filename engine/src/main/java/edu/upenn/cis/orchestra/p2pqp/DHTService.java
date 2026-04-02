package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.zip.Deflater;

import org.apache.log4j.Logger;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.p2pqp.SpillingTupleCollection.FullTuples;
import edu.upenn.cis.orchestra.p2pqp.TupleStore.ConstraintViolation;
import edu.upenn.cis.orchestra.p2pqp.messages.ConstraintViolationMsg;
import edu.upenn.cis.orchestra.p2pqp.messages.DHTMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.DoesNotHaveQuery;
import edu.upenn.cis.orchestra.p2pqp.messages.GetTree;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPageIs;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPagesAre;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPagesAreAt;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPagesAreData;
import edu.upenn.cis.orchestra.p2pqp.messages.InsertTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.KeysSent;
import edu.upenn.cis.orchestra.p2pqp.messages.RemoveTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyException;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyFailure;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySuccess;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyTimeout;
import edu.upenn.cis.orchestra.p2pqp.messages.RequestIndexPage;
import edu.upenn.cis.orchestra.p2pqp.messages.RequestIndexPages;
import edu.upenn.cis.orchestra.p2pqp.messages.RequestSendKeys;
import edu.upenn.cis.orchestra.p2pqp.messages.ScanTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.SendIndexPage;
import edu.upenn.cis.orchestra.p2pqp.messages.SendIndexPages;
import edu.upenn.cis.orchestra.p2pqp.messages.SendIndexPagesAreAt;
import edu.upenn.cis.orchestra.p2pqp.messages.SendTree;
import edu.upenn.cis.orchestra.predicate.Byteification;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.Cache;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.LRUCache;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class DHTService<M> {
	public static final int NUM_DHT_THREADS = 2;

	public static class DHTException extends Exception {
		private static final long serialVersionUID = 1L;

		DHTException(String what) {
			super(what);
		}

		DHTException(String what, Exception why) {
			super(what,why);
		}

		DHTException(Exception why) {
			super(why);
		}
	}

	public static class InconsistentUpdates extends DHTException {
		private static final long serialVersionUID = 1L;

		InconsistentUpdates(String missingKeys, String conflictsWithKeys) {
			super("Update to relation depend on missing keys: " + missingKeys + " and/or conflicts with existing keys: " + conflictsWithKeys);
		}
	}

	private QpApplication<M> app;
	int currentEpoch;
	// Both of these contain tuples with the current epoch
	Map<Integer,SpillingTupleCollection<M>> currentAdditions;
	Map<Integer,SpillingTupleCollection<M>> currentDeletions;
	Set<Integer> relationsFinishedForEpoch;
	private final MetadataFactory<M> mdf;

	private final Logger logger = Logger.getLogger(this.getClass());

	private final Environment env;
	@SuppressWarnings("unused")
	private final TableNameGenerator tng;
	private final ScratchFileGenerator sfg;

	private Database indexDb;
	private final DatabaseConfig dc;
	private final String indexDbName;
	public static int numTuplesPerPage = 5000;
	public static boolean locatePagesWithData = true;

	private final Cache<CacheKey,QpMessage> indexPagesAreCache;
	private final Cache<CacheKey,QpMessage> indexPagesCache;
	private final Cache<TreeKey,byte[]> treeCache;

	private int sendBlockSize = 500;

	DHTService(QpApplication<M> app, Environment env, TableNameGenerator tng, ScratchFileGenerator sfg, String indexDbName) throws DatabaseException {
		this.app = app;
		this.mdf = app.mdf;
		currentEpoch = Integer.MIN_VALUE;
		relationsFinishedForEpoch = new HashSet<Integer>();
		currentAdditions = new HashMap<Integer,SpillingTupleCollection<M>>();
		currentDeletions = new HashMap<Integer,SpillingTupleCollection<M>>();

		this.env = env;
		this.tng = tng;
		this.sfg = sfg;
		this.indexDbName = indexDbName;
		dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
		dc.setDeferredWrite(true);
		EnvironmentConfig ec = env.getConfig();
		dc.setReadOnly(ec.getReadOnly());
		indexDb = env.openDatabase(null, indexDbName, dc);

		// Set default cache size of 15 MB
		indexPagesAreCache = new LRUCache<CacheKey,QpMessage>(15 * 1024 * 1024, new LRUCache.GetSize<QpMessage>() {
			public int getSize(QpMessage m) {
				if (m instanceof IndexPagesAreAt) {
					return 50;
				} else if (m instanceof IndexPagesAre) {
					return ((IndexPagesAre) m).approxSize();
				} else {
					String error;
					if (m == null) {
						error = "Should not have a null entry in IndexService.indexEntryCache";
					} else {
						error = "Should not have an entry of type " + m.getClass().getName() + " in IndexService.indexPagesAreCache";
					}
					if (logger == null) {
						System.err.println(error);
					} else {
						logger.warn(error);
					}
					return 0;
				}
			}
		});

		// Set default cache size to be 10% of max memory
		long maxMemory = Runtime.getRuntime().maxMemory();
		if (maxMemory == Long.MAX_VALUE) {
			// 4GB
			maxMemory = 4 * 1024 * 1024 * 1024;
		}
		indexPagesCache = new LRUCache<CacheKey,QpMessage>((int)(0.1 * maxMemory), new LRUCache.GetSize<QpMessage>() {
			public int getSize(QpMessage m) {
				if (m instanceof IndexPageIs) {
					return ((IndexPageIs) m).getCompressedDataLength();
				} else {
					String error;
					if (m == null) {
						error = "Should not have a null entry in IndexService.indexEntryCache";
					} else {
						error = "Should not have an entry of type " + m.getClass().getName() + " in IndexService.indexPagesCache";
					}
					if (logger == null) {
						System.err.println(error);
					} else {
						logger.warn(error);
					}
					return 0;
				}
			}
		});

		treeCache = new LRUCache<TreeKey,byte[]>(5 * 1024 * 1024, new LRUCache.GetSize<byte[]>() {
			@Override
			public int getSize(byte[] data) {
				return data.length + 50;
			}
		});

	}

	public void setSendBlockSize(int newBlockSize) {
		if (newBlockSize <= 0) {
			throw new IllegalArgumentException("Sending block size must be positive");
		}
		this.sendBlockSize = newBlockSize;
	}

	public void addTuple(QpTuple<M> t, int epoch) throws DHTException, IOException  {
		addTuples(t.getSchema().relId, Collections.singleton(t), epoch);
	}

	public void addMutableTuples(int relId, Collection<QpMutableTuple<M>> ts, int epoch) throws DHTException, IOException {
		List<QpTuple<M>> cleaned = new ArrayList<QpTuple<M>>(ts.size());
		for (QpMutableTuple<M> t : ts) {
			cleaned.add(new QpTuple<M>(t, t.metadata, mdf));
		}
		addTuples(relId, cleaned.iterator(), epoch);
	}

	public void addTuples(int relId, Collection<QpTuple<M>> ts, int epoch) throws DHTException, IOException {		
		addTuples(relId, ts.iterator(), epoch);
	}

	public void addTuples(int relId, Iterator<QpTuple<M>> ts, int epoch) throws DHTException, IOException {	
		addTuples(relId, ts, epoch, null);
	}

	public void addTuples(int relId, Iterator<QpTuple<M>> ts, int epoch, TupleLoadingObserver tlo) throws DHTException, IOException {
		if (currentAdditions.isEmpty() && currentDeletions.isEmpty()) {
			if (epoch < currentEpoch) {
				throw new DHTException("Cannot add tuple from epoch " + epoch + " after previous epoch " + currentEpoch);
			}
			// Advance to new epoch
			currentEpoch = epoch;
			relationsFinishedForEpoch.clear();
		} else if (epoch != currentEpoch) {
			throw new DHTException("Cannot add tuple for epoch " + epoch + " during epoch " + currentEpoch);
		}
		if (this.relationsFinishedForEpoch.contains(relId)) {
			throw new IllegalArgumentException("Relation " + app.store.getSchema(relId) + " is already finished for epoch " + epoch);
		}

		SpillingTupleCollection<M> stc = currentAdditions.get(relId);
		final QpSchema schema = app.store.getSchema(relId);
		if (stc == null) {
			stc = new SpillingTupleCollection<M>(sfg, schema);
			currentAdditions.put(relId, stc);
		}

		final int granularity = tlo == null ? -1 : tlo.getTupleCountGranularity();
		int modCount = stc.size() % granularity;
		while (ts.hasNext()) {
			stc.add(ts.next());
			if (++modCount == granularity) {
				modCount = 0;
				tlo.loadedTupleCountIs(stc.size());
			}
		}
	}

	public void deleteTuple(QpTuple<M> t, int epoch) throws DHTException, IOException {
		deleteTuples(t.getSchema().relId, Collections.singleton(t), epoch);
	}

	public void deleteTuples(int relId, Collection<QpTuple<M>> ts, int epoch) throws DHTException, IOException {
		if (currentAdditions.isEmpty() && currentDeletions.isEmpty()) {
			if (epoch < currentEpoch) {
				throw new DHTException("Cannot add tuple during epoch " + epoch + " after previous epoch " + currentEpoch);
			}
			currentEpoch = epoch;
			relationsFinishedForEpoch.clear();
		} else if (epoch != currentEpoch) {
			throw new DHTException("Cannot delete tuple during epoch " + epoch + " since current epoch is " + currentEpoch);
		}
		if (this.relationsFinishedForEpoch.contains(relId)) {
			throw new IllegalArgumentException("Relation " + app.store.getSchema(relId) + " is already finished for epoch " + epoch);
		}
		SpillingTupleCollection<M> stc = currentAdditions.get(relId);
		final QpSchema schema = app.store.getSchema(relId);
		if (stc == null) {
			stc = new SpillingTupleCollection<M>(sfg, schema);
			currentAdditions.put(relId, stc);
		}
		for (QpTuple<M> t : ts) {
			stc.add(t);
		}
	}

	public void finishEpochForRelation(String relName) throws DHTException, InterruptedException {
		finishEpochForRelation(relName,null);
	}

	public void finishEpochForRelation(String relName, TupleLoadingObserver tlo) throws DHTException, InterruptedException {
		int relId = app.store.getSchema(relName).relId;
		SpillingTupleCollection<M> additions = currentAdditions.remove(relId);
		SpillingTupleCollection<M> deletions = currentDeletions.remove(relId);

		try {
			if (currentEpoch == app.getFirstEpochForTable(relId)) {
				recordAllTuplesForEpoch(relId, currentEpoch, additions, tlo, 0);
			} else {
				recordChangedTuplesForEpoch(relId, currentEpoch, deletions, additions, tlo);
			}
		} catch (IOException e) {
			throw new DHTException("Error updating index for table " + relName, e);
		}
		if (additions != null) {
			additions.finalize();
		}
		if (deletions != null) {
			deletions.finalize();
		}
		relationsFinishedForEpoch.add(relId);

	}

	public void finishEpochForRelationNonIncremental(String relName) throws DHTException, InterruptedException {
		finishEpochForRelationNonIncremental(relName, null);
	}

	public void finishEpochForRelationNonIncremental(String relName, TupleLoadingObserver tco) throws DHTException, InterruptedException {
		int relId = app.store.getSchema(relName).relId;
		SpillingTupleCollection<M> additions = currentAdditions.remove(relId);
		try {
			recordAllTuplesForEpoch(relId, currentEpoch, additions, tco);
		} catch (IOException e) {
			throw new DHTException("Error updating index for table " + relName, e);
		}
		additions.finalize();
		relationsFinishedForEpoch.add(relId);
	}

	void processMessage(DHTMessage m) {
		DatabaseEntry key, value = new DatabaseEntry();
		OperationStatus os;
		try {
			if (m instanceof RequestIndexPages) {
				RequestIndexPages rip = (RequestIndexPages) m;
				key = getKey(rip.relId, rip.epoch);
				os = indexDb.get(null, key, value, null);
				if (os == OperationStatus.SUCCESS) {
					byte[] result = value.getData();
					if (result[0] == 0) {
						// redirect
						int locEpoch = IntType.getValFromBytes(result, 1);
						app.sendMessage(new IndexPagesAreAt(rip,locEpoch));
					} else {
						if (rip.wantNonRedirect) {
							app.sendMessage(new IndexPagesAre(rip,result, 1, result.length - 1));
						} else {
							// Are just trying to determine if this epoch has
							// actual data or just a redirect
							app.sendMessage(new IndexPagesAreData(rip));
						}
					}
				} else {
					app.sendMessage(new ReplyFailure(m,"Could not find index entry in local DB for relation '" + rip.relId + "', epoch " + rip.epoch, true));
				}
			} else if ((m instanceof SendIndexPages) || (m instanceof SendIndexPagesAreAt)) {
				byte[] data;
				int relId;
				int epoch;
				if (m instanceof SendIndexPages) {
					SendIndexPages sip = (SendIndexPages) m;
					relId = sip.relId;
					epoch = sip.epoch;
					data = new byte[sip.data.length + 1];
					data[0] = 1;
					System.arraycopy(sip.data, 0, data, 1, sip.data.length);
				} else {
					// redirect
					SendIndexPagesAreAt sipaa = (SendIndexPagesAreAt) m;
					data = new byte[IntType.bytesPerInt + 1];
					data[0] = 0;
					System.arraycopy(IntType.getBytes(sipaa.locEpoch), 0, data, 1, IntType.bytesPerInt);
					relId = sipaa.relId;
					epoch = sipaa.epoch;
				}
				key = getKey(relId, epoch);
				value.setData(data);
				os = indexDb.putNoOverwrite(null, key, value);
				if (os == OperationStatus.KEYEXIST) {
					indexDb.get(null, key, value, null);
					if (Arrays.equals(value.getData(), data)) {
						// Ignore double insertion of same data
						os = OperationStatus.SUCCESS;
					}
				}
				if (os == OperationStatus.SUCCESS) {
					app.sendReplySuccess(m);
				} else {
					app.sendMessage(new ReplyFailure(m, "Record already exists for (" + relId + "," + epoch + ")", false));
				}					
			} else if (m instanceof RequestIndexPage) {
				RequestIndexPage rip = (RequestIndexPage) m;
				key = getKey(rip.relId, rip.epoch, rip.number);
				os = indexDb.get(null, key, value, null);
				if (os == OperationStatus.SUCCESS) {
					byte[] data = value.getData();
					Id pageBot = Id.fromMSBBytes(data, 0);
					Id pageTop = Id.fromMSBBytes(data, Id.idLengthBytes);
					int numKeys = IntType.getValFromBytes(data, 2 * Id.idLengthBytes);
					app.sendMessage(new IndexPageIs(rip, new IdRange(pageBot, pageTop), numKeys, new ByteArrayWrapper(data, 2 * Id.idLengthBytes + IntType.bytesPerInt, data.length - 2 * Id.idLengthBytes - IntType.bytesPerInt)));
				} else {
					app.sendMessage(new ReplyFailure(rip, "Could not find index page (" + rip.relId + "," + rip.epoch + "," + rip.number + ")", true));
				}
			} else if (m instanceof SendIndexPage) {
				SendIndexPage sip = (SendIndexPage) m;
				IdRange range = sip.pageRange;
				if (range.isEmpty()) {
					app.sendMessage(new ReplyFailure(m, "Page range is empty", false));
				} else {
					key = getKey(sip.relId, sip.epoch, sip.number);
					final byte[] data = sip.getData();
					// Construct combined entry
					byte[] dataToPut = new byte[data.length + 2 * Id.idLengthBytes + IntType.bytesPerInt];
					if (range.isFull()) {
						Id.ZERO.copyIntoMSB(dataToPut, 0);
						Id.ZERO.copyIntoMSB(dataToPut, Id.idLengthBytes);
					} else {
						range.getCCW().copyIntoMSB(dataToPut, 0);
						range.getCW().copyIntoMSB(dataToPut, Id.idLengthBytes);
					}
					IntType.putBytes(sip.numKeys, dataToPut, 2 * Id.idLengthBytes);
					System.arraycopy(data, 0, dataToPut, 2 * Id.idLengthBytes + IntType.bytesPerInt, data.length);
					value.setData(dataToPut);
					os = indexDb.put(null, key, value);
					if (os == OperationStatus.SUCCESS) {
						app.sendReplySuccess(m);
					} else {
						app.sendMessage(new ReplyFailure(m, "Record already exists for (" + sip.relId + "," + sip.epoch + "," + sip.number +")", false));
					}
				}
			} else if (m instanceof RequestSendKeys) {
				final RequestSendKeys rsk = (RequestSendKeys) m;
				if (app.queryFinished(rsk.queryId)) {
					app.sendMessage(new DoesNotHaveQuery(rsk, rsk.queryId));
				} else {
					key = getKey(rsk.relId, rsk.epoch, rsk.number);
					os = indexDb.get(null, key, value, null);
					if (os == OperationStatus.SUCCESS) {
						synchronized (app.startCountsForQueries) {
							if (! app.startCountsForQueries.containsKey(rsk.queryId)) {
								long currCount = app.socketManager.getTotalBytesSent();
								app.startCountsForQueries.put(rsk.queryId, currCount);
							}
						}
						QpSchema schema = app.store.getSchema(rsk.relId);
						Filter<? super QpTupleKey> f = null;
						if (rsk.keyPredBytes != null) {
							f = Byteification.getPredicateFromBytes(schema, rsk.keyPredBytes);
						}
						byte[] data = value.getData();
						IdRange pageRange = new IdRange(Id.fromMSBBytes(data, 0), Id.fromMSBBytes(data, Id.idLengthBytes));
						int totalNumTuples = IntType.getValFromBytes(data, 2 * Id.idLengthBytes);

						// Determine if one node owns the entire page. If so, things are
						// much simpler
						InetSocketAddress wholeOwner = null;
						if (rsk.dests.length == 1) {
							wholeOwner = rsk.dests[0];
						}

						final int numDests = rsk.dests.length;


						class DataAndCount {
							final byte[] data;
							final int offset, length;
							final int count;

							DataAndCount(byte[] data, int offset, int length, int count) {
								this.data = data;
								this.offset = offset;
								this.length = length;
								this.count = count;
							}

							DataAndCount(byte[] data, int count) {
								this.data = data;
								this.offset = 0;
								this.length = data.length;
								this.count = count;
							}
						}
						Map<InetSocketAddress,DataAndCount> toSend;

						if (wholeOwner != null && rsk.destRangesToSend[0].contains(pageRange) && f == null) {
							// We can send the whole page without examining it
							toSend = Collections.singletonMap(wholeOwner, new DataAndCount(data, 2 * Id.idLengthBytes + IntType.bytesPerInt, data.length - 2 * Id.idLengthBytes - IntType.bytesPerInt, totalNumTuples));
						} else {
							ScratchInputBuffer sib = new ScratchInputBuffer(data, 2 * Id.idLengthBytes + IntType.bytesPerInt, data.length - 2 * Id.idLengthBytes - IntType.bytesPerInt);
							class WriterAndCount {
								final ScratchOutputBuffer writer = new ScratchOutputBuffer();
								int count = 0;

								DataAndCount getDataAndCount() {
									return new DataAndCount(writer.getData(), count);
								}
							}

							final Map<InetSocketAddress,WriterAndCount> writers;
							if (wholeOwner == null) {
								writers = new HashMap<InetSocketAddress,WriterAndCount>(rsk.dests.length);
								for (InetSocketAddress dest : rsk.dests) {
									writers.put(dest, new WriterAndCount());
								}
							} else {
								writers = Collections.singletonMap(wholeOwner, new WriterAndCount());
							}
							final byte[] scratch = new byte[IntType.bytesPerInt * schema.getNumHashCols()];
							while (! sib.finished()) {
								QpTupleKey t = QpTupleKey.deserialize(schema, sib);
								if (f != null && (! f.eval(t))) {
									continue;
								}
								Id id = null;
								id = t.getQPid(scratch);
								InetSocketAddress dest = null;
								for (int i = 0; i < numDests; ++i) {
									if (rsk.destRangesToSend[i].contains(id)) {
										dest = rsk.dests[i];
										break;
									}
								}
								if (dest == null) {
									continue;
								}
								WriterAndCount wac = writers.get(dest);
								if (wac == null) {
									throw new IllegalStateException();
								}
								t.getBytes(wac.writer);
								++wac.count;
							}
							if (wholeOwner == null) {
								toSend = new HashMap<InetSocketAddress,DataAndCount>(rsk.dests.length);
								for (Map.Entry<InetSocketAddress, WriterAndCount> me : writers.entrySet()) {
									toSend.put(me.getKey(), me.getValue().getDataAndCount());
								}
							} else {
								toSend = Collections.singletonMap(wholeOwner, writers.get(wholeOwner).getDataAndCount());
							}
						}




						final List<InetSocketAddress> dests = new ArrayList<InetSocketAddress>(toSend.keySet());
						final HashSet<InetSocketAddress> unreceivedDests = new HashSet<InetSocketAddress>(toSend.keySet());
						unreceivedDests.remove(app.localAddr);
						final boolean allLocal = unreceivedDests.isEmpty();
						for (Map.Entry<InetSocketAddress, DataAndCount> me : toSend.entrySet()) {
							final InetSocketAddress dest = me.getKey();
							ReplyContinuation rc = new ReplyContinuation() {
								private boolean finished;
								synchronized public boolean isFinished() {
									return finished;
								}

								synchronized public void processReply(QpMessage m) {
									// We're only doing this for retries etc.
									finished = true;
								}

								public void received() {
									synchronized (unreceivedDests) {
										if (unreceivedDests.remove(dest) && unreceivedDests.isEmpty()) {
											if (logger.isInfoEnabled()) {
												logger.info("Finished sending ScanTuplesMessages for relation " + rsk.relId + " page (" + rsk.epoch + "," + rsk.number + ") to " + dests + " for query " + rsk.queryId);
											}
											try {
												app.sendMessage(new KeysSent(rsk));
											} catch (IOException e) {
												logger.error("Error sending KeysSent to " + rsk.getOrigin(), e);
											} catch (InterruptedException e) {
												return;
											}
										}
									}
								}
							};
							IdRangeSet idRangesForPageAtNode = null;
							if (wholeOwner == null) {
								for (int i = 0; i < numDests; ++i) {
									if (dest.equals(rsk.dests[i])) {
										idRangesForPageAtNode = rsk.destRangesToSend[i];
										break;
									}
								}
							} else {
								idRangesForPageAtNode = rsk.destRangesToSend[0];
							}
							DataAndCount dac = me.getValue();
							boolean local = dest.equals(app.localAddr);
							QpMessage stm = new ScanTuplesMessage(dest, rsk.relId, rsk.phaseNo, rsk.queryId, rsk.operatorId, dac.data, dac.offset, dac.length, rsk.epoch, rsk.number, idRangesForPageAtNode, dac.count, local ? Deflater.NO_COMPRESSION : rsk.compressionLevel);
							if (local) {
								stm.setOrigin(app.localAddr);
								app.processMessage(stm);
							} else {
								app.sendMessageAwaitReply(stm, rc, ReplySuccess.class, ReplyTimeout.class);
							}
						}
						if (logger.isDebugEnabled()) {
							logger.debug("ScanTuplesMessages for relation " + rsk.relId + " page (" + rsk.epoch + "," + rsk.number + ") enqueued to " + dests + " for query " + rsk.queryId);
						}
						if (allLocal) {
							if (logger.isInfoEnabled()) {
								logger.info("Finished sending ScanTuplesMessages for relation " + rsk.relId + " page (" + rsk.epoch + "," + rsk.number + ") to " + dests + " for query " + rsk.queryId);
							}
							try {
								app.sendMessage(new KeysSent(rsk));
							} catch (IOException e) {
								logger.error("Error sending KeysSent to " + rsk.getOrigin(), e);
							} catch (InterruptedException e) {
							}							
						}
					} else {
						app.sendMessage(new ReplyFailure(rsk, "Could not request keys because could not find index page (" + rsk.relId + "," + rsk.epoch + "," + rsk.number + ")", true));
					}
				}
			} else if (m instanceof SendTree) {
				SendTree stp = (SendTree) m;
				key = getTreePageKey(stp.relId, stp.treeNo);
				value.setData(stp.data);
				os = indexDb.putNoOverwrite(null, key, value);
				if (os == OperationStatus.KEYEXIST) {
					app.sendMessage(new ReplyFailure(stp, "Tree (" + stp.relId + "," + stp.treeNo + ") already in database", false));
				} else {
					app.sendReplySuccess(stp);
				}
			} else if (m instanceof GetTree) {
				GetTree gtp = (GetTree) m;
				key = getTreePageKey(gtp.relId, gtp.treeNo);
				os = indexDb.get(null, key, value, null);
				if (os == OperationStatus.SUCCESS) {
					app.sendMessage(new SendTree(gtp, value.getData()));
				} else {
					app.sendMessage(new ReplyFailure(gtp, "Tree (" + gtp.relId + "," + gtp.treeNo + ") not found in database", true));
				}
			} else if (m instanceof InsertTuplesMessage) {
				InsertTuplesMessage itm = (InsertTuplesMessage) m;
				Iterator<QpTuple<M>> toInsert = itm.getTuples(app.store);
				Map<QpTuple<M>,ConstraintViolation> cvs = app.store.addTuples(toInsert, itm.epoch);
				if (cvs == null || cvs.isEmpty()) { 
					app.sendReplySuccess(m);
				} else {

					app.sendMessage(new ConstraintViolationMsg(itm, cvs));
				}
			} else if (m instanceof RemoveTuplesMessage) {
				RemoveTuplesMessage rtm = (RemoveTuplesMessage) m;
				app.store.deleteTuples(rtm.getKeys(app.store));
				app.sendReplySuccess(m);
			} else {
				logger.error("DHT Service doesn't know what to do with message " + m);
			}
		} catch (Exception e) {
			logger.error("Error processing DHT message", e);
			try {
				app.sendMessage(new ReplyException(m,"Error processing DHTMessage", e, false));
			} catch (IOException ioe) {
				logger.error(ioe);
			} catch (InterruptedException ie) {
				return;
			}
		}
	}

	private static class CacheKey {
		final int relation;
		final int epoch;
		final int number;

		CacheKey(int relation, int epoch, int number) {
			this.relation = relation;
			this.epoch = epoch;
			this.number = number;
		}

		CacheKey(int relation, int epoch) {
			this(relation,epoch,-1);
		}

		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}

			CacheKey ck = (CacheKey) o;

			return (epoch == ck.epoch && number == ck.number && relation == ck.relation);
		}

		public int hashCode() {
			return relation + 37 * epoch + 1601 * number;
		}
	}

	private static class TreeKey {
		final int relId;
		final int treeNum;

		TreeKey(int relId, int treeNum) {
			this.relId = relId;
			this.treeNum = treeNum;
		}

		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}

			TreeKey tk = (TreeKey) o;

			return (relId == tk.relId && treeNum == tk.treeNum);
		}

		public int hashCode() {
			return relId + 1601 * treeNum;
		}
	}

	void close() throws DHTException {
		try {
			indexDb.close();
		} catch (DatabaseException e) {
			throw new DHTException("Error stopping index service", e);
		}
	}

	private interface IndexPageSink {
		void totalNumTuplesIs(int numTuples, int numPages);
		void deliverPage(EpochNum pageId, ByteArrayWrapper contents);
		void processException(Exception e);
	}

	public interface TupleSink {
		void totalNumTuplesIs(int numTuples, int numPages);
		/**
		 * Deliver a page of tuples. The array may contain extra
		 * null elements at the end.
		 * 
		 * @param contents
		 */
		void deliverTuples(QpTuple<?>[] contents);
		void processException(Exception e);
	}

	public interface KeyTupleSink {
		void totalNumTuplesIs(int numTuples, int numPages);
		/**
		 * Deliver a page of tuples. The array may contain extra
		 * null elements at the end.
		 * 
		 * @param contents
		 */
		void deliverTuples(QpTupleKey[] contents);
		void processException(Exception e);
	}

	private void getIndexPages(final int relId, int epoch, final IndexPageSink ips) throws DHTException, InterruptedException, IOException {
		IndexEntrySink ies = new IndexEntrySink() {

			public void deliverResult(IndexEntry ie) {

				ips.totalNumTuplesIs(ie.numTuples, ie.indexPages.size());

				for (final EpochNumAndId en : ie.indexPages) {
					final CacheKey epochNumCK = new CacheKey(relId, en.epoch, en.num);
					QpMessage cachedReply = indexPagesCache.probe(epochNumCK);
					if (cachedReply == null) {
						if (logger.isTraceEnabled()) {
							logger.trace("Probing network for keys for " + en + " for " + relId + " (page ID " + en.id + ")");
						}
						ReplyContinuation pageRc = new ReplyContinuation() {
							private boolean finished = false;
							synchronized public boolean isFinished() {
								return finished;
							}

							synchronized public void processReply(QpMessage m) {
								if (finished) {
									return;
								}
								finished = true;
								if (m instanceof IndexPageIs) {
									indexPagesCache.store(epochNumCK, m);
									ips.deliverPage(en.getEpochNum(), ((IndexPageIs) m).getData());
								} else {
									ips.processException(new DHTException("Received unexpected reply to RequestIndexPage for " + en + ": " + m));
								}
							}

						};
						try {
							app.sendMessageAwaitReply(new RequestIndexPage(DHTService.this.getId(relId, en.epoch, en.num), relId, en.epoch, en.num), pageRc, IndexPageIs.class);
						} catch (Exception e) {
							ips.processException(e);
						}
					} else {
						logger.trace("Found cached keys for " + en + " for " + relId);
						if (cachedReply instanceof IndexPageIs) {
							ips.deliverPage(en.getEpochNum(), ((IndexPageIs) cachedReply).getData());
						} else {
							ips.processException(new DHTException("Received unexpected cached reply to RequestIndexPage for " + en + ": " + cachedReply));
						}
					}
				}
			}

			public void processException(Exception e) {
				ips.processException(e);
			}
		};

		getIndexEntry(ies, relId, epoch);
	}



	public void getTuplesInRelation(final int relId, final int epoch, final KeyTupleSink ts) {
		if (ts == null) {
			throw new NullPointerException("ts cannot be null");
		}
		final QpSchema schema = app.store.getSchema(relId);
		try {
			getIndexPages(relId, epoch, new IndexPageSink() {
				public void deliverPage(EpochNum pageId, ByteArrayWrapper contents) {
					try {
						QpTupleKey tuples[] = new QpTupleKey[numTuplesPerPage];
						final ScratchInputBuffer sib = new ScratchInputBuffer(contents.array, contents.offset, contents.length);
						int count = 0;
						while (! sib.finished()) {
							if (count >= tuples.length) {
								QpTupleKey newTuples[] = new QpTupleKey[tuples.length * 2];
								System.arraycopy(tuples, 0, newTuples, 0, tuples.length);
								tuples = newTuples;
							}
							tuples[count++] = QpTupleKey.deserialize(schema, sib);
						}
						ts.deliverTuples(tuples);
					} catch (Exception e) {
						processException(e);
						return;
					}
				}

				public void totalNumTuplesIs(int numTuples, int numPages) {
					ts.totalNumTuplesIs(numTuples, numPages);
				}

				public void processException(Exception e) {
					e.printStackTrace();
					ts.processException(e);
				}
			});
		} catch (Exception e) {
			ts.processException(e);
		}
	}

	void sendKeyTuples(final Router router, final int relId, int epoch, final int queryId, final int operatorId, Predicate keyColsFilter, final RecordTuples rt, final IdRangeSet rangesToSend, final int phaseNo, final int compressionLevel) throws DHTException, InterruptedException {
		final QpSchema schema = app.store.getSchema(relId);
		if (schema == null) {
			throw new IllegalArgumentException("Couldn't get schema for relation " + relId);
		}
		final byte[] keyPredBytes;
		if (keyColsFilter == null) {
			keyPredBytes = null;
		} else {
			keyPredBytes = Byteification.getPredicateBytes(schema, keyColsFilter);
		}

		IndexEntrySink ies = new IndexEntrySink() {

			public void deliverResult(final IndexEntry ie) {
				SearchTreeSink sts = new SearchTreeSink() {
					@Override
					public void deliver(SearchTree st) {
						final HashSet<EpochNum> failedPages = new HashSet<EpochNum>();
						final HashMap<EpochNum,IdRange> pageRanges = new HashMap<EpochNum,IdRange>();
						List<IdRange> ranges = st.getPageRanges();

						if (ranges.size() != ie.indexPages.size()) {
							throw new IllegalStateException("Search tree gives " + ranges.size() + " page ranges but index entry records " + ie.indexPages.size() + " index pages for relation " + app.store.getSchema(relId).getName());
						}

						Iterator<IdRange> rangeIt = ranges.iterator();
						Iterator<EpochNumAndId> pageIt = ie.indexPages.iterator();

						while (rangeIt.hasNext()) {
							pageRanges.put(pageIt.next().getEpochNum(), rangeIt.next());
						}

						final List<EpochNumAndId> pagesToSend;
						if (rangesToSend == null) {
							// Send all pages
							pagesToSend = new ArrayList<EpochNumAndId>(ie.indexPages);
						} else {
							// Only send pages that intersect the desired ranges
							pagesToSend = new ArrayList<EpochNumAndId>();
							final int numPages = ie.indexPages.size();
							for (int i = 0; i < numPages; ++i) {
								if (rangesToSend.intersects(ranges.get(i))) {
									pagesToSend.add(ie.indexPages.get(i));
								}
							}
						}
						Collections.shuffle(pagesToSend);

						logger.info("Started sending key tuples for relation " + relId + " operator " + operatorId + " phase " + phaseNo);
						final HashSet<EpochNum> remainingPages = new HashSet<EpochNum>(pagesToSend.size());
						for (EpochNumAndId en : pagesToSend) {
							remainingPages.add(en.getEpochNum());
						}
						final HashSet<EpochNum> remainingToSend = new HashSet<EpochNum>(remainingPages);
						final HashSet<EpochNum> remainingToBeReceived = new HashSet<EpochNum>(remainingPages);
						for (final EpochNumAndId en : pagesToSend) {
							final IdRange range = pageRanges.get(en.getEpochNum());
							Set<InetSocketAddress> destsSet = router.getDests(range);
							List<InetSocketAddress> destsList = new ArrayList<InetSocketAddress>(destsSet.size());
							List<IdRangeSet> destsRanges = new ArrayList<IdRangeSet>(destsSet.size());
							for (InetSocketAddress owner : destsSet) {
								IdRangeSet owned = router.getOwnedRanges(owner);
								owned.intersect(range);
								owned.intersect(rangesToSend);

								if (! owned.isEmpty()) {
									destsList.add(owner);
									destsRanges.add(owned);
								}
							}
							QpMessage m = new RequestSendKeys(en.id, destsList, destsRanges, relId, en.epoch, en.num, queryId, operatorId, keyPredBytes, phaseNo, compressionLevel);
							ReplyContinuation rc = new ReplyContinuation() {
								private boolean finished;
								synchronized public boolean isFinished() {
									return finished;
								}

								public synchronized void processReply(QpMessage m) {
									finished = true;
									boolean failure = false;
									if (m instanceof KeysSent) {
									} else if (m instanceof DoesNotHaveQuery) {
										// Query was aborted
										return;
									} else {
										failure = true;
										Exception e = new RuntimeException("Received unexpected reply to RequestSendKeys for relation " + relId + " " + en + ": " + m);
										rt.reportException(e);
									}
									synchronized (remainingPages) {
										remainingPages.remove(en.getEpochNum());
										if (failure) {
											failedPages.add(en.getEpochNum());
										}
										if (remainingPages.isEmpty()) {
											if (failedPages.isEmpty()) {
												logger.info("Successfully sent key tuples for relation " + relId + " operator " + operatorId + " phase " + phaseNo);
											} else {
												logger.info("Sent key tuples for relation " + relId + " operator " + operatorId + " phase " + phaseNo + " but had failed pages: " + failedPages);
											}
										}
									}
								}

								void sent() {
									synchronized (remainingToSend) {
										if (remainingToSend.remove(en.getEpochNum()) && (remainingToSend.size() % 100 == 0)) {
											logger.info(remainingToSend.size() + " RequestSendKeys remaining to be sent for relation " + relId + " operator " + operatorId + " phase " + phaseNo);
										}
									}
								}

								void received() {
									synchronized (remainingToBeReceived) {
										if (remainingToBeReceived.remove(en.getEpochNum()) && (remainingToBeReceived.size() % 100 == 0)) {
											logger.info(remainingToBeReceived.size() + " RequestSendKeys remaining to be received for relation " + relId + " operator " + operatorId + " phase " + phaseNo);
										}
									}
								}
							};
							try {
								app.sendMessageAwaitReply(m, rc, KeysSent.class, DoesNotHaveQuery.class);
							} catch (Exception e) {
								rt.reportException(e);
							}
						}
						if (logger.isInfoEnabled()) {
							logger.info("Enqueued requests to send key tuples for relation " + relId + " operator " + operatorId + " phase " + phaseNo);
						}
					}

					@Override
					public void error(Exception ex) {
						processException(ex);
					}

				};

				try {
					DHTService.this.getTree(relId, ie.treeNum, sts);
				} catch (IOException e) {
					processException(e);
				} catch (InterruptedException e) {
					processException(e);
				}
			}

			public void processException(Exception e) {
				rt.reportException(e);
			}

		};

		try {
			getIndexEntry(ies, relId, epoch);
		} catch (IOException e1) {
			rt.reportException(e1);
		}
	}

	private static class IndexEntry {
		final int numTuples;
		final int treeNum;
		final List<EpochNumAndId> indexPages;

		IndexEntry(int numTuples, int treeNum, List<EpochNumAndId> indexPages) {
			this.numTuples = numTuples;
			this.treeNum = treeNum;
			this.indexPages = indexPages;
		}
	}

	private interface IndexEntrySink {
		void processException(Exception e);
		void deliverResult(IndexEntry ie);
	}

	private IndexEntry getIndexEntry(int relId, int epoch) throws InterruptedException, DHTException, IOException {
		class WaitingIndexEntrySink implements IndexEntrySink {
			IndexEntry ie = null;
			Exception e = null;


			public synchronized void deliverResult(IndexEntry ie) {
				this.ie = ie;
				notify();
			}

			public synchronized void processException(Exception e) {
				this.e = e;
				notify();
			}

			synchronized IndexEntry getValue() throws InterruptedException, IOException, DHTException {
				while (ie == null && e == null) {
					wait();
				}
				if (ie != null) {
					return ie;
				} else if (e instanceof DHTException) {
					throw (DHTException) e;
				} else if (e instanceof InterruptedException) {
					throw (InterruptedException) e;
				} else if (e instanceof IOException) {
					throw (IOException) e;
				} else {
					throw new DHTException("Error retrieving index page", e);
				}
			}
		}

		WaitingIndexEntrySink sink = new WaitingIndexEntrySink();
		getIndexEntry(sink, relId, epoch);
		return sink.getValue();
	}

	private void getIndexEntry(final IndexEntrySink ies, final int relId, final int epoch) throws DHTException, IOException, InterruptedException {
		QpMessage m = indexPagesAreCache.probe(new CacheKey(relId, epoch));

		ReplyContinuation rc = new ReplyContinuation() {
			private boolean done = false;
			synchronized public boolean isFinished() {
				return done;
			}

			public void processReply(QpMessage m) {
				done = true;
				if (!((m instanceof IndexPagesAre) || (m instanceof IndexPagesAreAt))) {
					ies.processException(new DHTException("Received unexpected reply to a RequestIndexPages(" + relId + "," + epoch + ") message from " + m.getOrigin() + ": " + m));
					return;
				}

				if (m instanceof IndexPagesAreAt) {
					indexPagesAreCache.store(new CacheKey(relId, epoch), m);
					IndexPagesAreAt ipaa = (IndexPagesAreAt) m;
					m = indexPagesAreCache.probe(new CacheKey(relId, ipaa.epoch));
					if (m == null) {
						try {
							app.sendMessageAwaitReply(new RequestIndexPages(getId(relId,ipaa.epoch), relId, ipaa.epoch, true), new ProcessIndexPagesAre(ipaa.epoch), IndexPagesAre.class);
						} catch (Exception e) {
							ies.processException(e);
						}
					} else {
						// It's cached
						new ProcessIndexPagesAre().processReply(m);
					}
				} else {
					new ProcessIndexPagesAre(epoch).processReply(m);
				}
			}

			class ProcessIndexPagesAre extends ReplyContinuation {
				private boolean finished = false;
				private final int epoch;
				private final boolean storeResult;

				ProcessIndexPagesAre(int epoch) {
					this.storeResult = true;
					this.epoch = epoch;
				}

				ProcessIndexPagesAre() {
					this.storeResult = false;
					this.epoch = Integer.MIN_VALUE;
				}

				public synchronized boolean isFinished() {
					return finished;
				}

				public synchronized void processReply(QpMessage m) {
					finished = true;
					if (m instanceof IndexPagesAreAt) {
						ies.processException(new DHTException("Should not find a redirect following a redirection in cache"));
						return;
					} else if (!(m instanceof IndexPagesAre)) {
						ies.processException(new DHTException("Received unexpected reply to a RequestIndexPages message from cache: " + m));
						return;
					}
					IndexPagesAre ipa = (IndexPagesAre) m;
					IndexEntry ie = new IndexEntry(ipa.numTuples, ipa.treeNum, Collections.unmodifiableList(ipa.getPageIds()));
					if (storeResult) {
						indexPagesAreCache.store(new CacheKey(relId, epoch), m);
					}
					ies.deliverResult(ie);
					logger.trace("Processed page ids for " + relId);
				}

			}
		};

		if (m == null) {
			logger.trace("Probing network for page ids for " + relId);
			app.sendMessageAwaitReply(new RequestIndexPages(getId(relId,epoch), relId, epoch, true), rc, IndexPagesAre.class, IndexPagesAreAt.class);
		} else {
			logger.trace("Found cached page ids for " + relId);
			rc.processReply(m);
		}
	}

	private class MessagesStatus {
		int remainingMessages = 0;
		List<Exception> exs = new ArrayList<Exception>();
		List<InetSocketAddress> exNodes = new ArrayList<InetSocketAddress>();
		Map<QpTuple<M>,ConstraintViolation> errors = new HashMap<QpTuple<M>,ConstraintViolation>();
	};

	private class UpdateReplyContinuation extends ReplyContinuation {
		private boolean finished;
		private final MessagesStatus info;

		UpdateReplyContinuation(MessagesStatus info) {
			this.info = info;
		}

		synchronized public boolean isFinished() {
			return finished;
		}

		synchronized public void processReply(QpMessage m) {
			finished = true;
			synchronized (info) {
				--info.remainingMessages;
				if (info.remainingMessages == 0) {
					info.notify();
				}
				if (m instanceof ReplySuccess) {
				} else {
					if (m instanceof ConstraintViolationMsg) {
						ConstraintViolationMsg cvm = (ConstraintViolationMsg) m;
						Map<QpTuple<M>,ConstraintViolation> cvs = cvm.getErrorTuples(app.store);
						info.errors.putAll(cvs);
					} else if (m instanceof ReplyException) {
						ReplyException re = (ReplyException) m; 
						info.exs.add(new RuntimeException(re.what, re.why));
						info.exNodes.add(m.getOrigin());
					} else {
						info.exs.add(new RuntimeException("Unexpected reply " + m + " from " + m.getOrigin()));
						info.exNodes.add(m.getOrigin());
					}
				}
			}
		}
	}

	private static class BufferAndCount {
		final ScratchOutputBuffer buf = new ScratchOutputBuffer();
		int count = 0;
		void reset() {
			buf.reset();
			count = 0;
		}
	}


	/**
	 * Record the tuples in the first epoch of a relation
	 * 
	 * @param relId			The relation ID
	 * @param epoch			The epoch in which the relation began
	 * @param keys			The key tuples of that relation
	 * @throws DHTException
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	private void recordAllTuplesForEpoch(int relId, int epoch, SpillingTupleCollection<M> keys, TupleLoadingObserver tlo, int treeNum) throws DHTException, InterruptedException, IOException {
		List<Id> pageBots = new ArrayList<Id>();

		final Set<Long> pendingStores = new HashSet<Long>();
		final Map<EpochNum,Collection<QpMessage>> errors = new HashMap<EpochNum,Collection<QpMessage>>();

		final Router r = app.getRouter();

		int numTuples = 0;
		pageBots.add(Id.ZERO);
		int pageNum = 0;
		int pageEstimate = keys.size() / numTuplesPerPage;

		final MessagesStatus info = new MessagesStatus();

		final Map<InetSocketAddress,BufferAndCount> toSend = new HashMap<InetSocketAddress,BufferAndCount>();

		int sentCount = 0, sentCountWindow = 0;
		int pageNumWindow = 0;
		final int tupleGranularity = tlo == null ? -1 : tlo.getTupleCountGranularity();
		final int indexGranularity = tlo == null ? -1 : tlo.getIndexPageGranularity();

		ScratchOutputBuffer sob = new ScratchOutputBuffer();

		List<EpochNumAndId> pages = new ArrayList<EpochNumAndId>();
		if (keys != null && (! keys.isEmpty())) {
			numTuples = keys.size();

			int pageSize = 0;

			Iterator<FullTuples<M>> it = keys.iterator();
			while (it.hasNext()) {
				FullTuples<M> ft = it.next();
				if (pageSize >= numTuplesPerPage) {
					IdRange range = new IdRange(pageBots.get(pageBots.size() - 1), it.hasNext() ? ft.id : Id.ZERO);
					Id pageId = locatePagesWithData ? range.getCCW().findHalfway(range.getCW()) : getId(relId, epoch, pageNum);
					pages.add(new EpochNumAndId(epoch, pageNum, pageId));
					recordPage(pageId, relId, epoch, pageNum++, range, sob.getData(), pageSize, pendingStores, errors);
					sob.reset();
					pageSize = 0;
					pageBots.add(ft.id);
					if (tlo != null) {
						++pageNumWindow;
						if (pageNumWindow == indexGranularity) {
							pageNumWindow = 0;
							tlo.processedIndexPages(pageNum, pageEstimate);
						}
					}
				}
				for (QpTuple<M> t : ft.tuples) {
					t.getKeyTuple(epoch).getBytes(sob);
				}
				pageSize += ft.tuples.size();

				Set<InetSocketAddress> addrs = r.getDests(ft.id);
				for (InetSocketAddress addr : addrs) {
					BufferAndCount tuples = toSend.get(addr);
					if (tuples == null) {
						tuples = new BufferAndCount();
						toSend.put(addr, tuples);
					}
					ScratchOutputBuffer buf = tuples.buf;
					for (QpTuple<M> t : ft.tuples) {
						t.putStoreBytes(buf);
					}
					tuples.count += ft.tuples.size();
					if (tuples.count >= sendBlockSize) {
						app.sendMessageAwaitReply(new InsertTuplesMessage(addr, relId, epoch, tuples.buf), new UpdateReplyContinuation(info),
								ReplySuccess.class, ConstraintViolationMsg.class );
						synchronized (info) {
							++info.remainingMessages;
						}
						tuples.reset();
					}
				}
				sentCount += ft.tuples.size();
				sentCountWindow += ft.tuples.size();
				if (tlo != null && sentCountWindow >= tupleGranularity) {
					sentCountWindow %= tupleGranularity;
					tlo.sentTupleCountIs(sentCount, keys.size());
				}
			}
			if (tlo != null) {
				tlo.sentTupleCountIs(sentCount, keys.size());
			}

			if (pageSize != 0) {
				// Deal with a final partial page
				IdRange range = new IdRange(pageBots.get(pageBots.size() - 1), Id.ZERO);
				Id pageId;
				if (range.isFull() || (! locatePagesWithData)) {
					pageId = getId(relId, epoch, pageNum);
				} else {
					pageId = range.getCCW().findHalfway(range.getCW());
				}
				pages.add(new EpochNumAndId(epoch, pageNum, pageId));
				recordPage(pageId, relId, epoch, pageNum++, range, sob.getData(), pageSize, pendingStores, errors);
			}
			if (tlo != null) {
				tlo.processedIndexPages(pageNum, pageEstimate);
			}
			for (Map.Entry<InetSocketAddress, BufferAndCount> me : toSend.entrySet()) {
				BufferAndCount tuples = me.getValue();
				if (tuples.count == 0) {
					continue;
				}
				InetSocketAddress addr = me.getKey();
				app.sendMessageAwaitReply(new InsertTuplesMessage(addr, relId, epoch, tuples.buf), new UpdateReplyContinuation(info),
						ReplySuccess.class, ConstraintViolationMsg.class );
				synchronized (info) {
					++info.remainingMessages;
				}
				tuples.reset();
			}

		}

		synchronized (pendingStores) {
			while (! pendingStores.isEmpty()) {
				pendingStores.wait();
			}
		}
		if (! errors.isEmpty()) {
			throw new DHTException("Error storing index pages for relation " + relId + " , epoch " + epoch + ": " + errors);
		}

		try {
			synchronized (info) {
				while (info.remainingMessages > 0) {
					info.wait();
				}
			}

			if (!(info.errors.isEmpty() && info.exs.isEmpty())) {
				StringBuilder sb = new StringBuilder();
				for (Map.Entry<QpTuple<M>, ConstraintViolation> me : info.errors.entrySet()) {
					sb.append("Error inserting tuple " + me.getKey() + ": " + me.getValue() + "\n");
				}
				for (int i = 0; i < info.exs.size(); ++i) {
					Exception e = (Exception) info.exs.get(i);
					if (e != null) {
						e.printStackTrace();
					}
					sb.append("Error at node " + info.exNodes.get(i) + ": " + e + "\n");
				}

				throw new DHTException(sb.toString());
			}

		} catch (InterruptedException e) {
			throw new DHTException("Interrupted while trying to await replies to insertions", e);
		}


		final int numPages = pages.size();
		List<IdRange> pageRanges = new ArrayList<IdRange>(pages.size());
		for (int i = 0; i < numPages - 1; ++i) {
			pageRanges.add(new IdRange(pageBots.get(i), pageBots.get(i+1)));
		}
		pageRanges.add(new IdRange(pageBots.get(pageBots.size() - 1), Id.ZERO));


		SearchTree st = new SearchTree(pageBots);

		recordPagesForEpoch(relId, epoch, pages, numTuples, 0);
		recordTree(relId, 0, st);
	}

	/**
	 * Replace the contents of an existing relation
	 * 
	 * @param relId			The relation ID
	 * @param epoch			The epoch for which to set the contents of the relation
	 * @param keys			The key tuples of that relation
	 * @throws DHTException
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	private void recordAllTuplesForEpoch(int relId, int epoch, SpillingTupleCollection<M> keys, TupleLoadingObserver tco) throws DHTException, InterruptedException, IOException {
		IndexEntry ie;
		try {
			ie = getIndexEntry(relId, epoch - 1);
		} catch (IOException e1) {
			throw new DHTException("Error retrieving index entry", e1);
		}
		recordAllTuplesForEpoch(relId, epoch, keys, tco, ie.treeNum + 1);
	}


	private void recordChangedTuplesForEpoch(int relId, int epoch, SpillingTupleCollection<M> removed, SpillingTupleCollection<M> added, TupleLoadingObserver tlo) throws DHTException, InterruptedException, IOException {
		int firstEpoch;
		try {
			firstEpoch = app.getFirstEpochForTable(relId);
		} catch (IllegalArgumentException iae) {
			throw new DHTException("Couldn't determine first epoch for relation " + relId, iae);
		}
		if (epoch == firstEpoch) {
			throw new DHTException("Cannot record changed tuples for first epoch");
		}

		if ((removed == null || removed.isEmpty()) && (added == null || added.isEmpty())) {
			// Refer to the previous epoch's index, either by pointing to it directly
			// (if it actually holds a list of pages) or my pointing to the epoch
			// it points to
			ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);

			try {
				app.sendMessageAwaitReply(new RequestIndexPages(getId(relId,epoch - 1),relId,epoch - 1,false), new SimpleReplyContinuation<Integer>(1,replies), IndexPagesAreAt.class, IndexPagesAreData.class);
				replies.waitUntilFinished();
			} catch (InterruptedException e) {
				throw new DHTException("Interrupted while requesting index pages", e);
			} catch (IOException e) {
				throw new DHTException("Error sending RequestIndexPages", e);
			}

			QpMessage m = replies.getReply(1);
			int newLoc;
			if (m instanceof IndexPagesAreAt) {
				newLoc = ((IndexPagesAreAt) m).epoch;
			} else if (m instanceof IndexPagesAreData) {
				newLoc = epoch - 1;
			} else {
				throw new DHTException("Received unexpected reply to RequestIndexPages while computing redirect: " + m);
			}

			try {
				recordPagesLocationForEpoch(relId, epoch, newLoc);
			} catch (IOException e) {
				throw new DHTException("Error recoding redirect for epoch", e);
			}
			return;
		}

		QpSchema.Source schemas = app.getStore();
		QpSchema schema = schemas.getSchema(relId);

		final IndexEntry ie;
		try {
			ie = getIndexEntry(relId, epoch - 1);
		} catch (IOException e1) {
			throw new DHTException("Error retrieving index entry", e1);
		}


		SearchTree st = this.getTree(schema.relId, ie.treeNum);

		final Collection<FullTuples<M>> empty = Collections.emptyList();
		Iterator<FullTuples<M>> removedIt = removed == null ? empty.iterator() : removed.iterator(), addedIt = added == null ? empty.iterator() : added.iterator();
		FullTuples<M> nextRemoved, nextAdded;
		if (removedIt.hasNext()) {
			nextRemoved = removedIt.next();
		} else {
			nextRemoved = null;
		}
		if (addedIt.hasNext()) {
			nextAdded = addedIt.next();
		} else {
			nextAdded = null;
		}
		EpochNum currPage = null;
		final Map<QpTupleKey,Integer> currentPage = new HashMap<QpTupleKey,Integer>(numTuplesPerPage);

		Set<QpTuple<M>> conflictsWithKey = new HashSet<QpTuple<M>>();
		Set<QpTuple<M>> missingKey = new HashSet<QpTuple<M>>();

		while (nextRemoved != null || nextAdded != null) {
			boolean processAdded = (nextRemoved == null || (nextAdded != null && nextAdded.id.compareTo(nextRemoved.id) < 0));
			EpochNumAndId enai = ie.indexPages.get(st.getIndexPage(processAdded ? nextAdded.id : nextRemoved.id));
			EpochNum en = enai.getEpochNum();
			if (currPage == null || (! en.equals(currPage))) {
				currentPage.clear();
				currPage = en;
				IndexPage ip = this.getIndexPage(enai.id, relId, en.epoch, en.num);
				for (QpTupleKey t : ip) {
					currentPage.put(t.changeEpoch(0), t.epoch);
				}
			}

			if (processAdded){
				for (QpTuple<M> t : nextAdded.tuples) {
					if (currentPage.containsKey(t.getKeyTuple(0))) {
						conflictsWithKey.add(t);
					}
				}
				if (addedIt.hasNext()) {
					nextAdded = addedIt.next();
				} else {
					nextAdded = null;
				}
			} else {
				for (QpTuple<M> t : nextRemoved.tuples) {
					if (! currentPage.containsKey(t.getKeyTuple(0))) {
						missingKey.add(t);
					}
				}
				if (removedIt.hasNext()) {
					nextRemoved = removedIt.next();
				} else {
					nextRemoved = null;
				}
			}
		}

		if (! (conflictsWithKey.isEmpty() && missingKey.isEmpty())) {
			throw new InconsistentUpdates(missingKey.toString(), conflictsWithKey.toString());
		}

		final MessagesStatus info = new MessagesStatus();
		final Map<InetSocketAddress,BufferAndCount> toInsert = new HashMap<InetSocketAddress,BufferAndCount>();
		final Map<InetSocketAddress,BufferAndCount> toRemove = new HashMap<InetSocketAddress,BufferAndCount>();

		int sentCount = 0, sentCountWindow = 0;
		int pageNumWindow = 0;
		final int tupleGranularity = tlo == null ? -1 : tlo.getTupleCountGranularity();
		final int indexGranularity = tlo == null ? -1 : tlo.getIndexPageGranularity();

		removedIt = removed == null ? empty.iterator() : removed.iterator();
		addedIt = added == null ? empty.iterator() : added.iterator();
		if (removedIt.hasNext()) {
			nextRemoved = removedIt.next();
		} else {
			nextRemoved = null;
		}
		if (addedIt.hasNext()) {
			nextAdded = addedIt.next();
		} else {
			nextAdded = null;
		}

		List<EpochNumAndId> pages = new ArrayList<EpochNumAndId>(ie.indexPages);

		int lastIndex = -1;
		int lastPage = -1;
		IdRange currRange = null;
		SerializedTupleSet lastIndexPage = null;

		final Set<Long> pendingStores = new HashSet<Long>();
		final Map<EpochNum,Collection<QpMessage>> errors = new HashMap<EpochNum,Collection<QpMessage>>();
		final Router r = app.getRouter();

		while (nextRemoved != null || nextAdded != null) {
			boolean processAdded = (nextRemoved == null || (nextAdded != null && nextAdded.id.compareTo(nextRemoved.id) < 0));
			int currIndex = st.getIndexPage(processAdded ? nextAdded.id : nextRemoved.id);
			if (currIndex != lastIndex) {
				lastIndex = currIndex;
				if (lastPage >= 0) {
					ScratchOutputBuffer sob = new ScratchOutputBuffer();
					int pageSize = 0;
					for (SerializedTupleSet.SerializedKeyTuples skt : lastIndexPage) {
						pageSize += skt.tuples.size();
						for (QpTupleKey key : skt.tuples) {
							key.getBytes(sob);
						}
					}
					Id lastId = pages.get(lastIndex).id;
					recordPage(lastId, relId, epoch, lastPage, currRange, sob.getData(), pageSize, pendingStores, errors);
					if (tlo != null) {
						++pageNumWindow;
						if (pageNumWindow == indexGranularity) {
							pageNumWindow = 0;
							tlo.processedIndexPages(currIndex + 1, ie.indexPages.size());
						}
					}
				}
				++lastPage;
				Id currId = pages.get(currIndex).id;
				pages.set(currIndex, new EpochNumAndId(epoch,lastPage,currId));
				EpochNumAndId enai = ie.indexPages.get(currIndex);
				IndexPage ip = this.getIndexPage(enai.id, relId, enai.epoch, enai.num);
				lastIndexPage = new SerializedTupleSet(schema);
				currRange = ip.range;
				for (QpTupleKey key : ip) {
					lastIndexPage.add(key);
				}
			}

			Set<InetSocketAddress> addrs = r.getDests(nextAdded.id);
			if (processAdded){
				for (QpTuple<M> t : nextAdded.tuples) {
					if (! lastIndexPage.add(nextAdded.id, t.getKeyTuple(epoch))) {
						throw new IllegalStateException("Should never attempt to overwrite an existing tuple");
					}
				}
				for (InetSocketAddress addr : addrs) {
					BufferAndCount tuples = toInsert.get(addr);
					if (tuples == null) {
						tuples = new BufferAndCount();
						toInsert.put(addr, tuples);
					}
					OutputBuffer buf = tuples.buf;
					for (QpTuple<M> t : nextAdded.tuples) {
						t.putStoreBytes(buf);
					}
					tuples.count += nextAdded.tuples.size();
					if (tuples.count >= sendBlockSize) {
						app.sendMessageAwaitReply(new InsertTuplesMessage(addr, relId, epoch, tuples.buf), new UpdateReplyContinuation(info),
								ReplySuccess.class, ConstraintViolationMsg.class );
						synchronized (info) {
							++info.remainingMessages;
						}
						tuples.reset();
					}
				}
				sentCount += nextAdded.tuples.size();
				sentCountWindow += nextAdded.tuples.size();
				if (tlo != null && sentCountWindow >= tupleGranularity) {
					sentCountWindow %= tupleGranularity;
					tlo.sentTupleCountIs(sentCount, added.size());
				}

				if (addedIt.hasNext()) {
					nextAdded = addedIt.next();
				} else {
					nextAdded = null;
				}
			} else {
				for (QpTuple<M> t : nextRemoved.tuples) {
					if (! lastIndexPage.removeKey(nextRemoved.id, t.getKeyTuple(epoch))) {
						throw new IllegalStateException("Should never attempt to remove a nonexistant tuple");
					}
				}
				for (InetSocketAddress addr : addrs) {
					BufferAndCount tuples = toRemove.get(addr);
					if (tuples == null) {
						tuples = new BufferAndCount();
						toRemove.put(addr, tuples);
					}
					OutputBuffer buf = tuples.buf;
					for (QpTuple<M> t : nextRemoved.tuples) {
						t.getKeyTuple(epoch).getBytes(buf);
					}
					tuples.count += nextRemoved.tuples.size();
					if (tuples.count >= sendBlockSize) {
						app.sendMessageAwaitReply(new RemoveTuplesMessage(addr, relId, tuples.buf), new UpdateReplyContinuation(info),
								ReplySuccess.class, ConstraintViolationMsg.class );
						synchronized (info) {
							++info.remainingMessages;
						}
						tuples.reset();
					}
				}
				if (removedIt.hasNext()) {
					nextRemoved = removedIt.next();
				} else {
					nextRemoved = null;
				}
			}
		}

		// Record last index page
		ScratchOutputBuffer sob = new ScratchOutputBuffer();
		int pageSize = 0;
		for (SerializedTupleSet.SerializedKeyTuples skt : lastIndexPage) {
			pageSize += skt.tuples.size();
			for (QpTupleKey key : skt.tuples) {
				key.getBytes(sob);
			}
		}

		Id lastId = pages.get(lastIndex).id;
		recordPage(lastId, relId, epoch, lastPage, currRange, sob.getData(), pageSize, pendingStores, errors);

		if (tlo != null) {
			tlo.processedIndexPages(ie.indexPages.size(), ie.indexPages.size());
		}

		// Insert remaining buffered tuples
		for (Map.Entry<InetSocketAddress, BufferAndCount> me : toInsert.entrySet()) {
			BufferAndCount tuples = me.getValue();
			if (tuples.count == 0) {
				continue;
			}
			InetSocketAddress addr = me.getKey();
			app.sendMessageAwaitReply(new InsertTuplesMessage(addr, relId, epoch, tuples.buf), new UpdateReplyContinuation(info),
					ReplySuccess.class, ConstraintViolationMsg.class );
			synchronized (info) {
				++info.remainingMessages;
			}
			tuples.reset();
		}
		// Remove remaining buffered tuples
		for (Map.Entry<InetSocketAddress, BufferAndCount> me : toRemove.entrySet()) {
			BufferAndCount tuples = me.getValue();
			if (tuples.count == 0) {
				continue;
			}
			InetSocketAddress addr = me.getKey();
			app.sendMessageAwaitReply(new RemoveTuplesMessage(addr, relId, tuples.buf), new UpdateReplyContinuation(info),
					ReplySuccess.class, ConstraintViolationMsg.class );
			synchronized (info) {
				++info.remainingMessages;
			}
			tuples.reset();
		}

		synchronized (pendingStores) {
			while (! pendingStores.isEmpty()) {
				pendingStores.wait();
			}
		}

		if (! errors.isEmpty()) {
			throw new DHTException("Error recording changes index pages for relation " + relId + ", epoch " + epoch + ": " + errors);
		}

		try {
			synchronized (info) {
				while (info.remainingMessages > 0) {
					info.wait();
				}
			}

			if (!(info.errors.isEmpty() && info.exs.isEmpty())) {
				StringBuilder sb = new StringBuilder();
				for (Map.Entry<QpTuple<M>, ConstraintViolation> me : info.errors.entrySet()) {
					sb.append("Error inserting tuple " + me.getKey() + ": " + me.getValue() + "\n");
				}
				for (int i = 0; i < info.exs.size(); ++i) {
					Exception e = (Exception) info.exs.get(i);
					if (e != null) {
						e.printStackTrace();
					}
					sb.append("Error at node " + info.exNodes.get(i) + ": " + e + "\n");
				}

				throw new DHTException(sb.toString());
			}

		} catch (InterruptedException e) {
			throw new DHTException("Interrupted while trying to await replies to insertions and deletions", e);
		}


		int relationSize = ie.numTuples;
		if (added != null) {
			relationSize += added.size();
		}
		if (removed != null) {
			relationSize -= removed.size();
		}
		recordPagesForEpoch(relId,epoch,pages,relationSize,ie.treeNum);
	}


	private static class IndexPage implements Iterable<QpTupleKey> {
		private final byte[] pageContents;
		private final int offset;
		private final int length;
		final IdRange range;
		final QpSchema schema;

		IndexPage(QpSchema schema, ByteArrayWrapper pageContents, IdRange pageRange) {
			this.pageContents = pageContents.array;
			this.offset = pageContents.offset;
			this.length = pageContents.length;
			this.range = pageRange;
			this.schema = schema;
		}

		public Iterator<QpTupleKey> iterator() {
			return new Iterator<QpTupleKey>() {
				ScratchInputBuffer sib = new ScratchInputBuffer(pageContents, offset, length);

				public boolean hasNext() {
					return (! sib.finished());
				}

				public QpTupleKey next() {
					if (! hasNext()) {
						throw new NoSuchElementException();
					}

					return QpTupleKey.deserialize(schema, sib);
				}

				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
	}

	private IndexPage getIndexPage(Id id, int relId, int epoch, int num) throws DHTException {
		QpMessage m = new RequestIndexPage(id, relId, epoch, num);
		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
		try {
			app.sendMessageAwaitReply(m, new SimpleReplyContinuation<Integer>(1, replies), IndexPageIs.class);
			replies.waitUntilFinished();
		} catch (Exception e) {
			throw new DHTException("Error retrieving index page", e);
		}
		QpMessage reply = replies.getReply(1);
		if (reply instanceof IndexPageIs) {
			IndexPageIs ipi = (IndexPageIs) reply;
			return new IndexPage(app.getStore().getSchema(relId), ipi.getData(), ipi.pageRange);
		} else {
			throw new DHTException("Received unexpected reply to " + m + ": " + reply);
		}
	}

	private void recordPagesForEpoch(int relId, int epoch, List<EpochNumAndId> pages, int numTuples, int treeNum) throws DHTException, IOException {
		Set<InetSocketAddress> dests = app.getRouter().getDests(getId(relId,epoch));
		final ReplyHolder<Long> replies = new ReplyHolder<Long>(dests.size());
		try {
			for (InetSocketAddress dest : dests) {
				QpMessage m = new SendIndexPages(dest, relId, epoch, pages, numTuples, treeNum);
				ReplyContinuation rc = new SimpleReplyContinuation<Long>(m.messageId, replies);
				app.sendMessageAwaitReply(m, rc, ReplySuccess.class);
			}
			replies.waitUntilFinished();
		} catch (InterruptedException ie) {
			throw new DHTException("Interrupted while sending index pages or awaiting confirmation", ie);
		}

		for (QpMessage m : replies) {
			if (! (m instanceof ReplySuccess)) {
				throw new DHTException("Received unexpected reply to sending index pages: " + m);
			}
		}
	}

	private void recordPagesLocationForEpoch(int relId, int epoch, int locationEpoch) throws DHTException, IOException {
		final ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);

		ReplyContinuation rc = new SimpleReplyContinuation<Integer>(1,replies);
		try {
			app.sendMessageAwaitReply(new SendIndexPagesAreAt(getId(relId,epoch), relId, epoch, locationEpoch), rc, ReplySuccess.class);
			replies.waitUntilFinished();
		} catch (InterruptedException ie) {
			throw new DHTException("Interrupted while sending index pages or awaiting confirmation", ie);
		}

		QpMessage m = replies.getReply(1);
		if (! (m instanceof ReplySuccess)) {
			throw new DHTException("Received unexpected reply to sending index pages: " + m);
		}
	}

	private void recordPage(Id pageId, int relId, final int epoch, final int num, IdRange range, byte[] page, int numKeys, final Set<Long> pending, final Map<EpochNum,Collection<QpMessage>> errors) throws DHTException, IOException, InterruptedException {
		final Router r = app.getRouter();
		Set<InetSocketAddress> dests = r.getDests(pageId);
		for (InetSocketAddress addr : dests) {
			SendIndexPage sip = new SendIndexPage(addr, relId, epoch, num, range, page, numKeys);
			final long messageId = sip.messageId;
			app.sendMessageAwaitReply(sip, new ReplyContinuation() {
				private boolean finished = false;
				public synchronized boolean isFinished() {
					return finished;
				}

				public synchronized void processReply(QpMessage m) {
					finished = true;
					synchronized (pending) {
						pending.remove(messageId);
						if (pending.isEmpty()) {
							pending.notifyAll();
						}
						if (! (m instanceof ReplySuccess)) {
							EpochNum en = new EpochNum(epoch, num);
							Collection<QpMessage> replies = errors.get(en);
							if (replies == null) {
								replies = new ArrayList<QpMessage>();
								errors.put(en, replies);
							}
							replies.add(m);
						}
					}
				}

			}, ReplySuccess.class);
		}
	}

	private void recordTree(int relId, int treeNo, SearchTree st) throws IOException, InterruptedException, DHTException {
		final ReplyHolder<Long> replies = new ReplyHolder<Long>(0);

		final Router r = app.getRouter();

		byte[] pageContents = st.getTree();
		Id dest = getTreeId(relId, treeNo);
		Set<InetSocketAddress> nodes = r.getDests(dest);
		replies.addMoreRepliesExpected(nodes.size());
		for (InetSocketAddress node : nodes) {
			QpMessage m = new SendTree(node, relId, treeNo, pageContents);
			ReplyContinuation rc = new SimpleReplyContinuation<Long>(m.messageId, replies);
			app.sendMessageAwaitReply(m, rc, ReplySuccess.class);
		}
		replies.waitUntilFinished();

		for (QpMessage m : replies) {
			if (!(m instanceof ReplySuccess)) {
				throw new DHTException("Received unexpected reply to SendTree: " + m);
			}
		}
	}

	public static class EpochNum implements Comparable<EpochNum> {
		public final int epoch;
		public final int num;

		public EpochNum(int epoch, int num) {
			this.epoch = epoch;
			this.num = num;
		}

		public boolean equals(Object o) {
			if (o == null) {
				return false;
			}
			EpochNum ren = (EpochNum) o;
			return (epoch == ren.epoch && num == ren.num);
		}

		public int hashCode() {
			return epoch + 1601 * num;
		}

		public String toString() {
			return "(" + epoch + "," + num + ")";
		}

		public int compareTo(EpochNum en) {
			if (epoch != en.epoch) {
				return epoch - en.epoch;
			}
			return num - en.num;
		}
	}

	public static class EpochNumAndId {
		final int epoch;
		final int num;
		final Id id;

		EpochNumAndId(int epoch, int num, Id id) {
			this.epoch = epoch;
			this.num = num;
			this.id = id;
		}

		public void serialize(OutputBuffer out) {
			out.writeInt(epoch);
			out.writeInt(num);
			id.serialize(out);
		}

		public static EpochNumAndId deserialize(InputBuffer in) {
			int epoch = in.readInt();
			int num = in.readInt();
			Id id = Id.deserialize(in);
			return new EpochNumAndId(epoch, num, id);
		}

		EpochNum getEpochNum() {
			return new EpochNum(epoch, num);
		}

		public int hashCode() {
			throw new UnsupportedOperationException();
		}

		public boolean equals(Object o) {
			throw new UnsupportedOperationException();
		}

		public String toString() {
			return "(" + epoch + "," + num + ")";
		}
	}

	private Id getTreeId(int rel, int treePageNum) {
		return getId(rel, treePageNum,TREE);
	}

	private Id getId(int rel, int epoch) {
		return getId(rel,epoch,PAGES_FOR_EPOCH);
	}

	private Id getId(int rel, int epoch, int number) {
		byte[] key = new byte[3 * IntType.bytesPerInt];
		IntType.putBytes(rel, key, 0);
		IntType.putBytes(epoch, key, IntType.bytesPerInt);
		IntType.putBytes(number, key, 2 * IntType.bytesPerInt);
		return Id.fromContent(key);
	}

	private static final int PAGES_FOR_EPOCH = 1000, PAGE_CONTENTS = 2000, TREE = 3000;

	private DatabaseEntry getKey(int relId, int epoch) {
		byte[] key = new byte[3 * IntType.bytesPerInt];
		IntType.putBytes(relId, key, 0);
		IntType.putBytes(epoch, key, IntType.bytesPerInt);
		IntType.putBytes(PAGES_FOR_EPOCH, key, 2 * IntType.bytesPerInt);
		return new DatabaseEntry(key);
	}

	private DatabaseEntry getKey(int relId, int epoch, int number) {
		byte[] key = new byte[4 * IntType.bytesPerInt];
		IntType.putBytes(relId, key, 0);
		IntType.putBytes(epoch, key, IntType.bytesPerInt);
		IntType.putBytes(PAGE_CONTENTS, key, 2 * IntType.bytesPerInt);
		IntType.putBytes(number, key, 3 * IntType.bytesPerInt);
		return new DatabaseEntry(key);
	}

	private DatabaseEntry getTreePageKey(int relId, int treeNo) {
		byte[] key = new byte[3 * IntType.bytesPerInt];
		IntType.putBytes(relId, key, 0);
		IntType.putBytes(treeNo, key, IntType.bytesPerInt);
		IntType.putBytes(TREE, key, 2 * IntType.bytesPerInt);
		return new DatabaseEntry(key);
	}

	void clear() throws DHTException {
		try {
			indexDb.close();
			env.truncateDatabase(null, indexDbName, false);
			indexDb = env.openDatabase(null, indexDbName, dc);
		} catch (DatabaseException de) {
			throw new DHTException("Error resetting BerkeleyDB database", de);
		}
		indexPagesCache.reset();
		indexPagesAreCache.reset();
		currentEpoch = Integer.MIN_VALUE;
		currentAdditions.clear();
		currentDeletions.clear();
		relationsFinishedForEpoch.clear();
	}

	SearchTree getTree(int relId, int treeNo) throws IOException, InterruptedException {
		final TreeKey tk = new TreeKey(relId, treeNo);
		byte[] serialized = treeCache.probe(tk);
		if (serialized != null ) {
			return new SearchTree(serialized);
		}
		QpMessage m = new GetTree(getTreeId(relId,treeNo),relId,treeNo);
		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
		app.sendMessageAwaitReply(m, new SimpleReplyContinuation<Integer>(1,replies), SendTree.class);
		replies.waitUntilFinished();

		QpMessage reply = replies.getReply(1);
		if (reply instanceof SendTree) {
			serialized = ((SendTree) reply).data;
			treeCache.store(tk, serialized);
			return new SearchTree(serialized);
		} else {
			throw new RuntimeException("Received unexpected reply to " + m + ": " + reply);
		}
	}

	void getTree(int relId, int treeNo, final SearchTreeSink sts) throws IOException, InterruptedException {
		final TreeKey tk = new TreeKey(relId, treeNo);
		byte[] serialized = treeCache.probe(tk);
		if (serialized != null ) {
			sts.deliver( new SearchTree(serialized));
			return;
		}
		final QpMessage request = new GetTree(getTreeId(relId,treeNo),relId,treeNo);
		app.sendMessageAwaitReply(request, new ReplyContinuation() {
			private boolean done = false;
			@Override
			public synchronized boolean isFinished() {
				return done;
			}

			@Override
			public synchronized void processReply(QpMessage m) {
				if (done) {
					return;
				}
				done = true;
				if (m instanceof SendTree) {
					byte[] serialized = ((SendTree) m).data;
					treeCache.store(tk, serialized);
					sts.deliver(new SearchTree(serialized));
				} else {
					sts.error(new RuntimeException("Received unexpected reply to " + request + ": " + m));
				}
			}

		}, SendTree.class);
	}

	interface SearchTreeSink {
		void deliver(SearchTree st);
		void error(Exception ex);
	}

	public Collection<EpochNum> getPagesForRelation(int relId, int epoch) throws InterruptedException, DHTException, IOException {
		Collection<EpochNumAndId> enais = this.getIndexEntry(relId, epoch).indexPages;
		List<EpochNum> retval = new ArrayList<EpochNum>(enais.size());
		for (EpochNumAndId enai : enais) {
			retval.add(enai.getEpochNum());
		}
		return retval;
	}

	public int getCardinality(int relId, int epoch) throws InterruptedException, DHTException, IOException {
		return getIndexEntry(relId, epoch).numTuples;
	}

	RelationInfo getRelationInfo(int relId, int epoch) throws InterruptedException, DHTException, IOException {
		IndexEntry ie = this.getIndexEntry(relId, epoch);
		SearchTree st = this.getTree(relId, ie.treeNum);

		return new RelationInfo(ie.indexPages, st.getPageRanges());
	}

	public static class RelationInfo {
		public final List<EpochNum> pages;
		public final List<IdRange> pageRanges;
		public final List<Id> pageIds;

		RelationInfo(List<EpochNumAndId> pages, List<IdRange> pageRanges) {
			this.pages = new ArrayList<EpochNum>(pages.size());
			this.pageIds = new ArrayList<Id>(pages.size());
			for (EpochNumAndId enai : pages) {
				this.pages.add(enai.getEpochNum());
				this.pageIds.add(enai.id);
			}
			this.pageRanges = new ArrayList<IdRange>(pageRanges);
		}
	}
}
