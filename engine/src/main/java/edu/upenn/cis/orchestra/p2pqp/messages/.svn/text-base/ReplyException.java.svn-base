package edu.upenn.cis.orchestra.p2pqp.messages;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ReplyException extends QpMessage {
	private static final long serialVersionUID = 1L;

	public final String what;
	public final Exception why;

	public ReplyException(QpMessage origMessage, String what, Exception why, boolean canRetry) {
		super(origMessage, canRetry);
		this.what = what;
		this.why = why;
	}

	public String toString() {
		String start = "ReplyException ("+ what + ")";
		if (why == null) {
			return start;
		} else {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			pw.append(start);
			pw.append(": ");
			why.printStackTrace(pw);
			pw.close();
			return sw.toString();
		}
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeString(what);
		buf.writeObject(why);
	}

	public ReplyException(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);

		what = buf.readString();
		try {
			this.why = (Exception) buf.readObject();
		} catch (IOException e) {
			throw new SerializationException("Error retrieving constraint violations", e);
		} catch (ClassNotFoundException e) {
			throw new SerializationException("Error retrieving constraint violations", e);
		}
	}
}