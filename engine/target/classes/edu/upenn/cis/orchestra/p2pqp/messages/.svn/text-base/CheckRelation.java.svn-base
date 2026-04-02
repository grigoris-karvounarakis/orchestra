package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.Router;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class CheckRelation extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final int relId;
	public final int epoch;
	public final Router router;
	
	public CheckRelation(InetSocketAddress dest, Router router, int relId, int epoch) {
		super(dest);
		this.relId = relId;
		this.epoch = epoch;
		this.router = router;
	}
	
	public CheckRelation(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		relId = buf.readInt();
		epoch = buf.readInt();
		router = Router.deserialize(buf);
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(epoch);
		router.serialize(buf);
	}
}
