package edu.upenn.cis.orchestra.p2pqp;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;


public class DredProvenance extends TupleProvenance { 
	boolean exist;
	
	public DredProvenance() {
		exist = true;
	}

	public DredProvenance(boolean exist) {
        this.exist = exist;
	}

	
	public static DredProvenance fromBytes(byte[] bytes) {
		ByteBufferReader bbr = new ByteBufferReader(null, bytes);
		int r = bbr.readInt();
		if (r != 0 && r != 1) {
			throw new RuntimeException("Wrong Serialization!");
		}
		return new DredProvenance(r > 0);
	}
	
	public byte[] getBytes() {
		ByteBufferWriter bbw = new ByteBufferWriter();
		int t = exist? 1: 0;
		bbw.addToBuffer(t);
		return bbw.getByteArray();
	}
	
	public TupleProvenance joinWith(TupleProvenance r_2, int op) {
		if (!(r_2 instanceof DredProvenance))
			return null;

		DredProvenance ret = new DredProvenance(exist && ((DredProvenance)r_2).exist); 
		
		return ret; 
	}
	public TupleProvenance derive(int op) {
		TupleProvenance ret = new DredProvenance(exist);
		ret.setCreator(op);
		return ret;
	}
	
	public TupleProvenance unionWith(TupleProvenance r_2, int op) {
		if (!(r_2 instanceof DredProvenance))
			return null;
		
		DredProvenance ret = new DredProvenance(exist || ((DredProvenance)r_2).exist); 
		
		return ret; 
	}
	public TupleProvenance differenceWith (TupleProvenance r_2) {
		if (!(r_2 instanceof DredProvenance))
			return null;
		
		DredProvenance ret = new DredProvenance(exist && !((DredProvenance)r_2).exist); 
		
		return ret; 
	}

	public TupleProvenance copy() {
		return new DredProvenance(exist);
	}
	
	public boolean equalProvenance(TupleProvenance r_2) {
		if (!(r_2 instanceof DredProvenance))
			return false;
		
		return (exist == ((DredProvenance)r_2).exist);
	}
	
	public boolean contains(TupleProvenance r_2) {
		if (!(r_2 instanceof DredProvenance))
			return false;
		
		return true;
	}
	
	public TupleProvenance find(TupleProvenance r_2) {
		if (!(r_2 instanceof DredProvenance))
			return null;
		
		DredProvenance ret = new DredProvenance(((DredProvenance) r_2).exist);
		return ret;
	}

	public String toString() {
		return Boolean.toString(exist);
	}
	
	public int size() {
		return 1;
	}
	
	public DredProvenance setZero(TupleProvenance r_2) {
		if (!(r_2 instanceof DredProvenance))
			return null;
		
		//Doesn't change at all
		DredProvenance ret = new DredProvenance(exist); 
		return ret;
	}
	
	public boolean isZero() {
		return (exist == false);
	}
}