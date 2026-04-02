package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;

public class PidAndRecno {
	private final AbstractPeerID pid;
	private final int recno;
	
	public PidAndRecno(AbstractPeerID pid, int recno) {
		this.pid = pid;
		this.recno = recno;
	}
	
	public PidAndRecno(byte[] bytes) {
		ByteBufferReader bbr = new ByteBufferReader(null, bytes);
		pid = bbr.readPeerID();
		recno = bbr.readInt();
		if (! bbr.hasFinished()) {
			throw new IllegalArgumentException("Byte array contains extra data after PidAndRecno");
		}
	}
	
	public byte[] getBytes() {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(pid);
		bbw.addToBuffer(recno);
		return bbw.getByteArray();
	}
	
	public AbstractPeerID getPid() {
		return pid;
	}
	
	public int getRecno() {
		return recno;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		PidAndRecno par = (PidAndRecno) o;
		return (recno == par.recno && pid.equals(par.pid));
	}
	
	public int hashCode() {
		return recno + 37 * pid.hashCode();
	}
	
	public String toString() {
		return pid + "r" + recno;
	}
}
