package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public abstract class QpMessage {
	static final int initialNumIds = 10;
	private static final long serialVersionUID = 1L;
	private final long[] inReplyTo;
	private InetSocketAddress dest;
	private final Id destId;
	private final boolean canRetry;

	
	private static synchronized long getNextMessageId() {
		return nextMessageId++;
	}
	private static long nextMessageId = 0;

	public final long messageId;
	private InetSocketAddress from;

	
	protected static class Compression {
		private final Inflater i = new Inflater();
		private final Deflater d = new Deflater();
		private final byte[] block = new byte[1024];
		private final ByteBufferWriter bbw = new ByteBufferWriter();
		
		public byte[] compress(byte[] input, int compressionLevel) {
			return compress(input, 0, input.length, compressionLevel);
		}
		
		public byte[] compress(byte[] input, int offset, int length, int compressionLevel) {
			bbw.clear();
			d.reset();
			d.setLevel(compressionLevel);
			d.setInput(input, offset, length);
			d.finish();
			
			while (! d.finished()) {
				int toWrite = d.deflate(block);
				bbw.addToBufferNoLength(block, 0, toWrite);
			}
			byte[] retval = bbw.getByteArray();
			bbw.clear();
			return retval;
		}
		
		public byte[] decompress(byte[] input) throws DataFormatException {
			return decompress(input, 0, input.length);
		}
		public byte[] decompress(byte[] input, int offset, int length) throws DataFormatException {
			i.reset();
			i.setInput(input, offset, length);
			while (! i.finished()) {
				int toWrite = i.inflate(block);
				bbw.addToBufferNoLength(block, 0, toWrite);
			}
			byte[] retval = bbw.getByteArray();
			bbw.clear();
			return retval;
		}
	}
	
	protected static final ThreadLocal<Compression> compressors = new ThreadLocal<Compression>() {
		protected Compression initialValue() {
			return new Compression();
		}
	};

	
	/**
	 * Construct a message that will be routed to the node that owns
	 * a particular Id in the network
	 * 
	 * @param destId		The Id to route to
	 */
	protected QpMessage(Id destId) {
		this.messageId = getNextMessageId();
		this.inReplyTo = null;
		this.destId = destId;
		this.dest = null;
		canRetry = false;
	}

	/**
	 * Construct a message that is a reply to another message. It will
	 * be send back directly to the node that send the original message.
	 * canRetry is set to false.
	 * 
	 * @param inReplyTo		The message to reply to
	 */
	protected QpMessage(QpMessage inReplyTo) {
		this(inReplyTo,false);
	}

	/**
	 * Construct a message that is a reply to another message. It will
	 * be send back directly to the node that send the original message
	 * 
	 * @param inReplyTo		The message to reply to
	 * @param canRetry		If this message indicates a failure, whether
	 * 						retrying may give a success
	 */
	protected QpMessage(QpMessage inReplyTo, boolean canRetry) {
		if (inReplyTo.getOrigin() == null) {
			throw new IllegalArgumentException("Cannot construct a reply to a message without an origin");
		}
		this.messageId = getNextMessageId();
		this.inReplyTo = new long[] {inReplyTo.messageId};
		this.dest = inReplyTo.getOrigin();
		this.destId = null;
		this.canRetry = canRetry;
	}

	protected QpMessage(InetSocketAddress dest, long[] msgIds) {
		if (msgIds.length == 0) {
			throw new IllegalArgumentException("Cannot have reply to no messages");
		}
		this.messageId = getNextMessageId();
		this.inReplyTo = msgIds;
		this.dest = dest;
		this.destId = null;
		this.canRetry = false;
	}

	/**
	 * Construct a message that will be sent directly to another node
	 * 
	 * @param destNH
	 */
	protected QpMessage(InetSocketAddress dest) {
		this.messageId = getNextMessageId();
		this.inReplyTo = null;
		this.dest = dest;
		this.destId = null;
		canRetry = false;
	}

	public final boolean isReply() {
		return inReplyTo != null;
	}

	protected long[] getOrigIds() {
		if (inReplyTo == null) {
			return null;
		}
		long[] retval = new long[inReplyTo.length];
		System.arraycopy(inReplyTo, 0, retval, 0, retval.length);
		return retval;
	}
		
	protected String getOrigIdsString() {
		if (inReplyTo == null) {
			return "";
		}

		if (inReplyTo.length < 5) {
			return Arrays.toString(inReplyTo);
		} else {
			return "...";
		}
	}

	int getNumOrigIds() {
		if (inReplyTo == null) {
			return 0;
		} else {
			return inReplyTo.length;
		}
	}

	protected InetSocketAddress getDest() {
		return dest;
	}

	public void serialize(OutputBuffer buf) {
		buf.writeLong(messageId);
		if (inReplyTo == null) {
			buf.writeInt(-1);
		} else {
			buf.writeInt(inReplyTo.length);
			for (int i = 0; i < inReplyTo.length; ++i) {
				buf.writeLong(inReplyTo[i]);
			}
		}

		if (destId == null) {
			buf.writeBoolean(false);
		} else {
			buf.writeBoolean(true);
			destId.serialize(buf);
		}

		buf.writeBoolean(canRetry);
		subclassSerialize(buf);
	}

	protected QpMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		this.from = origin;
		this.messageId = buf.readLong();
		int numInReplyTo = buf.readInt();
		if (numInReplyTo < 0) {
			inReplyTo = null;
		} else {
			inReplyTo = new long[numInReplyTo];
			for (int i = 0; i < numInReplyTo; ++i) {
				inReplyTo[i] = buf.readLong();
			}
		}

		boolean hasDestId = buf.readBoolean();
		if (hasDestId) {
			destId = Id.deserialize(buf);
		} else {
			destId = null;
		}

		canRetry = buf.readBoolean();
	}


	protected abstract void subclassSerialize(OutputBuffer buf);

	static InetSocketAddress readAddress(InputBuffer buf) throws SerializationException {
		try {
			return new InetSocketAddress(InetAddress.getByAddress(buf.readBytes()),
					buf.readInt());
		} catch (UnknownHostException e) {
			throw new SerializationException("Error decoding message origin", e);
		}
	}

	final void send(InetSocketAddress from, Router r, SocketManager sm) throws IOException, InterruptedException {
		this.from = from;
		if (destId != null) {
			dest = r.getDest(destId);
		}
		sm.sendMessage(this);
	}

	protected boolean canRetry() {
		return canRetry;
	}

	protected boolean retryImmediately() {
		return false;
	}
	
	public boolean hasDestId() {
		return destId != null;
	}
	
	public enum Priority {
		// Need to be listed in order from highest to lowest
		HIGH, NORMAL
	}
	
	public Priority getPriority() {
		return Priority.NORMAL;
	}

	public InetSocketAddress getOrigin() {
		return from;
	}
	
	public void setOrigin(InetSocketAddress from) {
		this.from = from;
	}

	public boolean retryable() {
		return false;
	}
}