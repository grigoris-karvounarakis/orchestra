package edu.upenn.cis.orchestra.p2pqp;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.p2pqp.TransactionalInputBuffer.TransactionSizeException;
import edu.upenn.cis.orchestra.p2pqp.messages.BeginNewQueryPhase;
import edu.upenn.cis.orchestra.p2pqp.messages.CheckRelation;
import edu.upenn.cis.orchestra.p2pqp.messages.ConnectMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.ConstraintViolationMsg;
import edu.upenn.cis.orchestra.p2pqp.messages.DistributeQueryMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.DoesNotHaveQuery;
import edu.upenn.cis.orchestra.p2pqp.messages.DummyMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.EndOfStreamMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.GarbageCollect;
import edu.upenn.cis.orchestra.p2pqp.messages.GetNodeInfo;
import edu.upenn.cis.orchestra.p2pqp.messages.GetTree;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPageIs;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPagesAre;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPagesAreAt;
import edu.upenn.cis.orchestra.p2pqp.messages.IndexPagesAreData;
import edu.upenn.cis.orchestra.p2pqp.messages.InsertTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.KeysSent;
import edu.upenn.cis.orchestra.p2pqp.messages.LocalRelationIs;
import edu.upenn.cis.orchestra.p2pqp.messages.MissingTuplesAre;
import edu.upenn.cis.orchestra.p2pqp.messages.NodeInfoIs;
import edu.upenn.cis.orchestra.p2pqp.messages.QueryTornDown;
import edu.upenn.cis.orchestra.p2pqp.messages.RecordTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.RemoveTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyException;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyFailure;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySuccess;
import edu.upenn.cis.orchestra.p2pqp.messages.RequestIndexPage;
import edu.upenn.cis.orchestra.p2pqp.messages.RequestIndexPages;
import edu.upenn.cis.orchestra.p2pqp.messages.RequestSendKeys;
import edu.upenn.cis.orchestra.p2pqp.messages.ScanTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.SendIndexPage;
import edu.upenn.cis.orchestra.p2pqp.messages.SendIndexPages;
import edu.upenn.cis.orchestra.p2pqp.messages.SendIndexPagesAreAt;
import edu.upenn.cis.orchestra.p2pqp.messages.SendKnownNodes;
import edu.upenn.cis.orchestra.p2pqp.messages.SendTree;
import edu.upenn.cis.orchestra.p2pqp.messages.ShippedTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.TearDownQueryMessage;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class QpMessageSerialization {
	private final Logger logger = Logger.getLogger(this.getClass());
	private final ThreadLocal<ScratchOutputBuffer> outputBufs;
	private final ThreadLocal<ScratchInputBuffer> inputBufs;

	QpMessageSerialization() {

		outputBufs = new ThreadLocal<ScratchOutputBuffer>() {
			protected ScratchOutputBuffer initialValue() {
				return new ScratchOutputBuffer();
			}

			public ScratchOutputBuffer get() {
				ScratchOutputBuffer sob = super.get();
				sob.reset();
				return sob;
			}
		};

		inputBufs = new ThreadLocal<ScratchInputBuffer>() {
			protected ScratchInputBuffer initialValue() {
				return new ScratchInputBuffer();
			}
		};
	}

	private static Map<Class<? extends QpMessage>,Integer> findId
	= new HashMap<Class<? extends QpMessage>,Integer>();

	private static Map<Integer,Class<? extends QpMessage>> findClass
	= new HashMap<Integer,Class<? extends QpMessage>>();

	private static void registerMsgClass(int id, Class<? extends QpMessage> msgClass) {
		if (id < Short.MIN_VALUE || id > Short.MAX_VALUE || id == 0) {
			String err = "Id " + id + " is not in the ranges [" + Short.MIN_VALUE + ",-1] and [1," + Short.MAX_VALUE + "]";
			System.err.println(err);
			throw new IllegalArgumentException(err);
		}
		if (findId.containsKey(msgClass)) {
			String err = "Class " + msgClass.getName() + " is already given ID " + findId.get(msgClass);
			System.err.println(err);
			throw new IllegalArgumentException(err);
		}
		if (findClass.containsKey(id)) {
			String err = "Id " + id + " is already assigned to class " + findClass.get(id).getName();
			System.err.println(err);
			throw new IllegalArgumentException(err);
		}
		findId.put(msgClass, id);
		findClass.put(id, msgClass);
	}

	static int getId(Class<? extends QpMessage> msgClass) {
		Integer id = findId.get(msgClass);
		if (id == null) {
			throw new IllegalArgumentException("Couldn't find ID for class " + msgClass.getName());
		}
		return id;
	}

	public QpMessage deserialize(byte[] data, InetSocketAddress origin) throws SerializationException {
		ScratchInputBuffer buf = inputBufs.get();
		buf.reset(data);

		QpMessage m = deserialize(buf, origin);
		int remaining = buf.remaining();
		if (remaining != 0) {
			throw new SerializationException(remaining + " bytes remain after deserialization");
		}
		return m;
	}

	public QpMessage deserialize(TransactionalInputBuffer buf, InetSocketAddress origin) throws SerializationException {
		QpMessage m = deserialize((InputBuffer) buf, origin);
		try {
			buf.endReadingTransaction();
		} catch (IOException e) {
			throw new SerializationException("Error deserializing " + m, e);
		} catch (TransactionSizeException e) {
			throw new SerializationException("Error deserializing " + m, e);
		}
		return m;
	}

	private QpMessage deserialize(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		int type = buf.readInt();
		Class<? extends QpMessage> c = findClass.get(type);
		if (c == null) {
			throw new SerializationException("Cannot find class to deserialize QpMessage with ID " + type);
		}
		try {
			Constructor<? extends QpMessage> cc = c.getDeclaredConstructor(InputBuffer.class, InetSocketAddress.class);
			return cc.newInstance(buf, origin);
		} catch (NoSuchMethodException e) {
			throw new SerializationException("Could not find (InputBuffer,InetSocketAddress) constructor for class " + c.getName(), e);
		} catch (IllegalAccessException e) {
			throw new SerializationException("Error invoking (InputBuffer,InetSocketAddress) constructor on class " + c.getName(), e);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof SerializationException) {
				throw ((SerializationException) e.getCause());
			}
			throw new SerializationException("Error invoking (InputBuffer,InetSocketAddress) constructor on class " + c.getName(), e.getCause());
		} catch (IllegalArgumentException e) {
			throw new SerializationException("Error invoking (InputBuffer,InetSocketAddress) constructor on class " + c.getName(), e);
		} catch (InstantiationException e) {
			throw new SerializationException("Error invoking (InputBuffer,InetSocketAddress) constructor on class " + c.getName(), e);
		}
	}

	public byte[] serialize(QpMessage m) {
		ScratchOutputBuffer sob = outputBufs.get();
		sob.writeInt(getId(m.getClass()));
		m.serialize(sob);
		return sob.getData();
	}

	<T extends OutputBuffer & TransactionalOutputBufferControls> void serialize(T buf, QpMessage m) {
		buf.beginTransaction();
		try {
			buf.writeInt(getId(m.getClass()));
			m.serialize(buf);
			buf.commitTransaction(m.messageId);
		} catch (RuntimeException e) {
			logger.error("Error serializing message " + m, e);
			buf.rollbackTransaction();
			throw e;
		}
	}

	public static class SerializationException extends Exception {
		private static final long serialVersionUID = 1L;
		public SerializationException(String what) {
			super(what);
		}
		public SerializationException(String what, Throwable why) {
			super(what,why);
		}
	}

	static {
		registerMsgClass( 1, ReplySuccess.class);
		registerMsgClass( 2, ReplyFailure.class);
		registerMsgClass( 3, ReplyException.class);

		registerMsgClass(10, InsertTuplesMessage.class);
		registerMsgClass(11, ConstraintViolationMsg.class);
		registerMsgClass(12, RemoveTuplesMessage.class);

		registerMsgClass(20, RequestIndexPages.class);
		registerMsgClass(21, IndexPagesAre.class);
		registerMsgClass(22, IndexPagesAreAt.class);
		registerMsgClass(23, IndexPagesAreData.class);
		registerMsgClass(24, RequestIndexPage.class);
		registerMsgClass(25, RequestSendKeys.class);
		registerMsgClass(26, IndexPageIs.class);
		registerMsgClass(27, SendIndexPages.class);
		registerMsgClass(28, SendIndexPagesAreAt.class);
		registerMsgClass(29, SendIndexPage.class);
		registerMsgClass(32, SendTree.class);
		registerMsgClass(33, GetTree.class);

		registerMsgClass(50, RecordTuplesMessage.class);

		registerMsgClass(60, ShippedTuplesMessage.class);
		registerMsgClass(61, EndOfStreamMessage.class);
		
		registerMsgClass(71, ScanTuplesMessage.class);

		registerMsgClass(116, MissingTuplesAre.class);

		registerMsgClass(120, NodeInfoIs.class);

		registerMsgClass(150, DummyMessage.class);

		registerMsgClass(158, LocalRelationIs.class);
		registerMsgClass(160, DistributeQueryMessage.class);
		registerMsgClass(161, TearDownQueryMessage.class);
		registerMsgClass(162, DoesNotHaveQuery.class);
		registerMsgClass(163, BeginNewQueryPhase.class);
		registerMsgClass(164, KeysSent.class);
		registerMsgClass(165, QueryTornDown.class);
		registerMsgClass(166, GarbageCollect.class);
		
		registerMsgClass(170, CheckRelation.class);
		registerMsgClass(171, SendKnownNodes.class);
		registerMsgClass(172, ConnectMessage.class);
		registerMsgClass(173, GetNodeInfo.class);
	}

}
