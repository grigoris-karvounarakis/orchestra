package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class VariablePosition implements Iterable<Variable> {
	private final Map<Variable,Integer> findPos;
	private final List<Variable> findVar;
	private boolean finished = false;
	
	public VariablePosition() {
		findPos = new HashMap<Variable,Integer>();
		findVar = new ArrayList<Variable>();
	}
	
	public VariablePosition(int expectedSize) {
		findPos = new HashMap<Variable,Integer>(expectedSize);
		findVar = new ArrayList<Variable>(expectedSize);
	}
	
	public void addVariable(Variable v) {
		if (finished) {
			throw new IllegalStateException("Object is already finished");
		}
		
		int pos = findVar.size();
		findVar.add(v);
		findPos.put(v, pos);
		if (v instanceof EquivClass) {
			EquivClass ec = (EquivClass) v;
			for (Variable vv : ec) {
				findPos.put(vv, pos);
			}
		}
	}
	
	public void finish() {
		finished = true;
	}
	
	public Variable getVariable(int pos) {
		return findVar.get(pos);
	}
	
	public Integer getPos(Variable v) {
		return findPos.get(v);
	}
		
	VariablePosition applyMorphism(Morphism m, RelationTypes<?,?> rt) {
		if (m == null) {
			return this;
		}
		
		VariablePosition vp = new VariablePosition(findVar.size());
		for (Variable v : findVar) {
			Variable vv = v.applyMorphism(m, rt);
			if (vv == null) {
				vv = v;
			}
			vp.addVariable(vv);
		}
		
		return vp;
	}
	
	public List<Variable> getVariables() {
		return Collections.unmodifiableList(findVar);
	}
	
	public int size() {
		return findVar.size();
	}
	
	VariablePosition updateWithEcs(Map<Variable,EquivClass> ecs) {
		VariablePosition newVP = new VariablePosition(findVar.size());
		for (Variable var : findVar) {
			Variable newVar = null;
			if (var instanceof EquivClass) {
				for (Variable v : ((EquivClass) var)) {
					newVar = ecs.get(v);
					if (newVar != null) {
						break;
					}
				}
			} else {
				newVar = ecs.get(var);
			}
			if (newVar != null) {
				var = newVar;
			}
			newVP.addVariable(var);
		}
		newVP.finish();
		return newVP;
	}

	public Iterator<Variable> iterator() {
		return Collections.unmodifiableList(findVar).iterator();
	}
}
