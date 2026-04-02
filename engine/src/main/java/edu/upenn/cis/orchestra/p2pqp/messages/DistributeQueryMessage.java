package edu.upenn.cis.orchestra.p2pqp.messages;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.zip.DataFormatException;
import java.util.zip.InflaterInputStream;

import org.xml.sax.InputSource;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.Router;
import edu.upenn.cis.orchestra.p2pqp.QpApplication.Configuration;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class DistributeQueryMessage extends QpMessage {
	private static final long serialVersionUID = 1L;

	private final byte[] qpws;
	public final int epoch;
	public final int queryId;
	public final Router queryRouter;
	public final int restartingPreviousQuery;
	public final Configuration config;

	public DistributeQueryMessage(InetSocketAddress dest, String queryPlanWithSchemas,
			int epoch, int queryId, Router queryRouter, Configuration config) {
		this(dest, queryPlanWithSchemas, epoch, queryId, queryRouter, config, Integer.MIN_VALUE);
	}
	public DistributeQueryMessage(InetSocketAddress dest, String queryPlanWithSchemas,
			int epoch, int queryId, Router queryRouter, Configuration config, int restartingPreviousQuery) {
		super(dest);
		this.config = config;
		this.restartingPreviousQuery = restartingPreviousQuery;
		this.epoch = epoch;
		this.queryId = queryId;
		this.queryRouter = queryRouter;
		try {
			if (config.compressionLevel < 0) {
				qpws = QuickLZ.compress(queryPlanWithSchemas.getBytes("UTF-8"));
			} else if (config.compressionLevel == 0) {
				qpws = queryPlanWithSchemas.getBytes("UTF-8"); 
			} else {
				qpws = compressors.get().compress(queryPlanWithSchemas.getBytes("UTF-8"), config.compressionLevel);
			}
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	private DistributeQueryMessage(InetSocketAddress dest, byte[] qpws, int epoch, int queryId, Router queryRouter, Configuration config, int restartingPreviousQuery) {
		super(dest);
		this.qpws = qpws;
		this.epoch = epoch;
		this.queryId = queryId;
		this.queryRouter = queryRouter;
		this.config = config;
		this.restartingPreviousQuery = restartingPreviousQuery;
	}

	public DistributeQueryMessage retarget(InetSocketAddress newDest) {
		return new DistributeQueryMessage(newDest, qpws, epoch, queryId, queryRouter, config, restartingPreviousQuery);
	}

	public String toString() {
		return "DistributeQueryMessage(" + queryId + ")";
	}

	public InputSource getQPWSInputSource() {
		InputSource is;
		if (this.config.compressionLevel < 0) {
			byte[] data = QuickLZ.decompress(qpws);
			is = new InputSource(new ByteArrayInputStream(data));
		} else if (this.config.compressionLevel == 0) {
			is = new InputSource(new ByteArrayInputStream(qpws));
		} else {
			is = new InputSource(new InflaterInputStream(new ByteArrayInputStream(qpws)));
		}
		is.setEncoding("UTF-8");
		return is;
	}

	public String getQueryPlansWithSchemas() {
		try {
			if (this.config.compressionLevel <= 0) {
				return new String(qpws, "UTF-8");
			} else {
				byte[] data = compressors.get().decompress(qpws);
				return new String(data, "UTF-8");
			}
		} catch (DataFormatException e) {
			throw new RuntimeException("Error decompressing query plan", e);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Could not get UTF-8 encoding", e);
		}
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeBytes(qpws);
		buf.writeInt(epoch);
		buf.writeInt(queryId);
		queryRouter.serialize(buf);
		config.serialize(buf);
		buf.writeInt(this.restartingPreviousQuery);
	}

	public DistributeQueryMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		qpws = buf.readBytes();
		epoch = buf.readInt();
		queryId = buf.readInt();
		queryRouter = Router.deserialize(buf);
		config = Configuration.deserialize(buf);
		this.restartingPreviousQuery = buf.readInt();
	}

	public Priority getPriority() {
		return Priority.HIGH;
	}
}
