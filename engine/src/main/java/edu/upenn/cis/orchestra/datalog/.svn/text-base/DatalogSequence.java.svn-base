package edu.upenn.cis.orchestra.datalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.upenn.cis.orchestra.Debug;

/**
 * Represents a sequence of Datalog programs (or sub-sequences)
 * 
 * @author zives, gkarvoun
 *
 */
public class DatalogSequence extends Datalog {
	List<Datalog> _programs;
	boolean _isRecursive;
//	public boolean _c4f;
//	public boolean _measureExecTime;
	
	/**
	 * Basic datalog sequence
	 * 
	 * @param isRecursive
	 * @param c4f Count for fixpoint in bookkeeping
	 */
	public DatalogSequence(boolean isRecursive, boolean c4f) {
		_programs = new ArrayList<Datalog>();
		_isRecursive = isRecursive;
		_c4f = c4f;
		_measureExecTime = false;
	}
	
	public DatalogSequence(boolean isRecursive, List<Datalog> p, boolean c4f) {
		_programs = new ArrayList<Datalog>();
		_isRecursive = isRecursive;
		addAll(p);
		_c4f = c4f;
		_measureExecTime = false;
	}
	
	public DatalogSequence(boolean isRecursive, List<Datalog> p, boolean c4f, boolean t) {
		_programs = new ArrayList<Datalog>();
		_isRecursive = isRecursive;
		addAll(p);
		_c4f = c4f;
		_measureExecTime = t;
	}

	public void setMeasureExecTime(boolean s){
		_measureExecTime = s;
	}
	
	public boolean measureExecTime(){
		return _measureExecTime;
	}
	
	public boolean count4fixpoint(){
		return _c4f;
	}
	
	public boolean isRecursive() {
		return _isRecursive;
	}
	
	public void setRecursive() {
		_isRecursive = true;
	}
	
	public void clearRecursive() {
		_isRecursive = false;
	}
	
	public void add(DatalogProgram p) {
		_programs.add(p);
	}

	/**
	 * Adds a sub-sequence.  If it's not recursive, we
	 * simply fold it in.  If it is recursive, we keep it nested.
	 * 
	 * @param p sub-sequence
	 */
	public void add(DatalogSequence p) {
//		if (p.isRecursive())
			_programs.add(p);
//		else
//			_programs.addAll(p.getSequence());
	}

	/*
	public void addAll(Collection<Datalog> p) {
		for (Datalog o : p)
			if (o instanceof DatalogSequence)
				add((DatalogSequence)o);
			else if (o instanceof DatalogProgram)
				add((DatalogProgram)o);
			else
				throw new RuntimeException("Incompatible object in collection!");
	}*/
	
	public void addAll(Collection<Datalog> p) {
		for (Datalog o : p)
			if (o instanceof DatalogSequence)
				add((DatalogSequence)o);
			else if (o instanceof DatalogProgram)
				add((DatalogProgram)o);
			else
				throw new RuntimeException("Incompatible object in collection!");
	}
	
	
	public Datalog get(int i) {
		return _programs.get(i);
	}
	
	public int size() {
		return _programs.size();
	}
	
	public List<Datalog> getSequence() {
		return _programs;
	}
	
	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		
		int cnt = 0;
		
		if(isRecursive())
			b.append("Recursive Sequence { \n");
		else
			b.append("Non-Recursive Sequence { \n");
			
		for (Object o : _programs) {
			if (cnt++ > 0)
				b.append("\n");
			
			b.append(o.toString());
		}
		
		b.append("} END Sequence \n");
		return new String(b);
	}

	public void printString() {
		if(isRecursive())
			Debug.println("Recursive Sequence {");
		else
			Debug.println("Non-Recursive Sequence {");
			
		for (Object o : _programs) {
			if (o instanceof DatalogProgram)
				((DatalogProgram)o).printString();
			else if (o instanceof DatalogSequence)
				((DatalogSequence)o).printString();
			else
				Debug.println(o.toString());
		}
		Debug.println("} END Sequence \n");
	}
}
