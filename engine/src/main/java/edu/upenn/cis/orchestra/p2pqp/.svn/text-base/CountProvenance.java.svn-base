package edu.upenn.cis.orchestra.p2pqp;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;


public class CountProvenance extends TupleProvenance { 
	
	private int count;
	
	public CountProvenance() {
		count = 1;
	}

	public CountProvenance(int count) {
		setCount(count);
	}

	public void setCount(int count) {
		this.count = count;
	}
	public static CountProvenance fromBytes(byte[] bytes) {
		/*
		return new CountProvenance(((int)(buf[3]) << 24) + 
				((int)(buf[2]) << 16) +
				((int)(buf[1]) << 8) +
				((int)(buf[0])));*/
		ByteBufferReader bbr = new ByteBufferReader(null, bytes);
		int r = bbr.readInt();
		return new CountProvenance(r);
	}
	
	public byte[] getBytes() {
		/*
		byte[] buf = new byte[4];
		
		buf[3] = (byte)((count >> 24) & 0xff);
		buf[2] = (byte)((count >> 16) & 0xff);
		buf[1] = (byte)((count >> 8) & 0xff);
		buf[0] = (byte)(count & 0xff);*/
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(count);
		return bbw.getByteArray();
	}
	
	public TupleProvenance joinWith(TupleProvenance r_2, int op) {
		if (!(r_2 instanceof CountProvenance))
			return null;

		CountProvenance ret = new CountProvenance(count * ((CountProvenance)r_2).count); 
		
		return ret; 
	}
	public TupleProvenance derive(int op) {
		TupleProvenance ret = new CountProvenance(count);
		ret.setCreator(op);
		return ret;
	}
	
	public TupleProvenance unionWith(TupleProvenance r_2, int op) {
		if (!(r_2 instanceof CountProvenance))
			return null;
		
		CountProvenance ret = new CountProvenance(count + ((CountProvenance)r_2).count); 
		
		return ret; 
	}
	public TupleProvenance differenceWith (TupleProvenance r_2) {
		if (!(r_2 instanceof CountProvenance))
			return null;
		
		CountProvenance ret = new CountProvenance(count - ((CountProvenance)r_2).count); 
		
		return ret; 
	}

	/*
	public TupleProvenance TupleProvenanceSelect(TupleProvenance p){
		return new TupleProvenance(absorbProv.restrict(p.absorbProv));
	}
	public TupleProvenance TupleProvenanceProject(TupleProvenance v){
		return new TupleProvenance(absorbProv.exist(v.absorbProv));
	}
	public TupleProvenance TupleProvenanceJoinProject(TupleProvenance r_2, 
			TupleProvenance v){
		return new TupleProvenance(absorbProv.relprod(r_2.absorbProv, v.absorbProv));
	}
	public TupleProvenance TupleProvenanceRename(BDDPairing p){
		return new TupleProvenance(absorbProv.replace(p));
	}*/
	public TupleProvenance copy() {
		return new CountProvenance(count);
	}
	
	public boolean equalProvenance(TupleProvenance r_2) {
		if (!(r_2 instanceof CountProvenance))
			return false;
		
		return (count == ((CountProvenance)r_2).count);
	}
	
	public boolean contains(TupleProvenance r_2) {
		if (!(r_2 instanceof CountProvenance))
			return false;
		
		CountProvenance r2 = (CountProvenance) r_2;
		if (this.count >= r2.count) {
			return true;
		}
		return false;
	}
	
	public TupleProvenance find(TupleProvenance r_2) {
		if (!(r_2 instanceof CountProvenance))
			return null;
		
		CountProvenance r2 = (CountProvenance) r_2;
		return new CountProvenance(r2.count);
	}

	public String toString() {
		return Integer.toString(count);
	}
	
	public int size() {
		return Integer.SIZE / Byte.SIZE;
	}
	
	public CountProvenance setZero(TupleProvenance p) {
		if (!(p instanceof CountProvenance))
			return null;
		
		return new CountProvenance(count - ((CountProvenance)p).count);
	}
	
	public boolean isZero() {
		return (count == 0);
	}
}