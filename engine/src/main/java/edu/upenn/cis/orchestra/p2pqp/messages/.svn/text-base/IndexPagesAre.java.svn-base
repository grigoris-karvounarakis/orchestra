package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.DHTService.EpochNumAndId;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;

public class IndexPagesAre extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final int numTuples;
	public final int treeNum;
	private final byte[] pages;

	public IndexPagesAre(RequestIndexPages request, byte[] data, int offset, int length) {
		super(request, true);
		numTuples = IntType.getValFromBytes(data, offset);
		treeNum = IntType.getValFromBytes(data, offset + IntType.bytesPerInt);
		pages = new byte[length - 2 * IntType.bytesPerInt];
		System.arraycopy(data, offset + 2 * IntType.bytesPerInt, pages, 0, length - 2 * IntType.bytesPerInt);
	}

	public String toString() {
		return "IndexPagesAre";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(numTuples);
		buf.writeInt(treeNum);
		buf.writeBytes(pages);
	}

	public int approxSize() {
		return 50 + pages.length;
	}
	
	public IndexPagesAre(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		numTuples = buf.readInt();
		treeNum = buf.readInt();
		pages = buf.readBytes();
	}
	
	public List<EpochNumAndId> getPageIds() {
		List<EpochNumAndId> retval = new ArrayList<EpochNumAndId>();
		ScratchInputBuffer buf = new ScratchInputBuffer(pages);
		while (! buf.finished()) {
			retval.add(EpochNumAndId.deserialize(buf));
		}
		return retval;
	}
}
