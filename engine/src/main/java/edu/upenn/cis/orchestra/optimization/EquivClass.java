package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.util.Bijection;
import edu.upenn.cis.orchestra.util.Pair;

/**
 * A set of variables that are known to be equal
 * by predicates in an expression
 * 
 * @author netaylor
 *
 */
class EquivClass extends Variable implements Iterable<Variable>, Comparable<EquivClass> {
	private Set<Variable> members;
	private List<Variable> membersList;
	private Type t;
	private boolean finished = false;
	private int hashcode;
	private Set<EquivClass> ecsUsed;

	EquivClass() {
		membersList = new ArrayList<Variable>();
	}

	EquivClass(Variable v) {
		members = new HashSet<Variable>();
		t = v.getType();
		members.add(v);
		finished = true;
		hashcode = members.hashCode();
	}

	public Type getType() { 
		return t;
	}

	public Iterator<Variable> iterator() {
		final Iterator<Variable> i = finished ? members.iterator() : membersList.iterator();
		return new Iterator<Variable>() {
			public boolean hasNext() {
				return i.hasNext();
			}

			public Variable next() {
				return i.next();
			}

			public void remove() {
				throw new UnsupportedOperationException("Cannot remove from an equivalence class");
			}

		};
	}

	void setFinished() {
		if (finished) {
			return;
		}
		members = new HashSet<Variable>(membersList);
		membersList = null;
		finished = true;
		hashcode = members.hashCode();
		ecsUsed = new HashSet<EquivClass>();
		ecsUsed.add(this);
		for (Variable v : this) {
			v.getEquivClassesUsed(ecsUsed);
		}
	}

	boolean isFinished() {
		return finished;
	}

	boolean contains(Variable v) {
		if (finished) {
			return members.contains(v);
		} else {
			return membersList.contains(v);
		}
	}

	int size() {
		if (finished) {
			return members.size();
		} else {
			return membersList.size();
		}
	}

	void add(Variable v) throws TypeError {
		if (finished) {
			throw new IllegalStateException("Cannot modify a finished EquivClass");
		}
		if (v == null) {
			throw new NullPointerException();
		}
		if (v instanceof EquivClass) {
			throw new IllegalArgumentException("Cannot add an equiv class to another equiv class");
		}
		Type t1 = v.getType();
		if (t == null) {
			t = t1;
			membersList.add(v);
			return;
		}
		boolean nullable = t.nullable && t1.nullable;
		boolean labeledNullable = t.labeledNullable && t1.labeledNullable;
		if (! t1.equals(t)) {
			if ((t1 instanceof DoubleType) && (t instanceof IntType) ||
					((t1 instanceof IntType) && (t instanceof DoubleType))) {
				t = IntType.create(nullable, labeledNullable);
			} else if (t1 instanceof CharType && t instanceof CharType) {
				CharType ct1 = (CharType) t1;
				CharType ct = (CharType) t;
				if (ct1.length < ct.length) {
					t = ct1;
				}
			} else if (t1 instanceof CharType && t instanceof VarCharType) {
				CharType ct = (CharType) t1;
				VarCharType vct = (VarCharType) t;
				if (vct.maxLength < ct.length) {
					t = new CharType(vct.maxLength, nullable, labeledNullable);
				} else {
					t = t1;
				}
			} else if (t1 instanceof VarCharType && t instanceof CharType) {
				CharType ct = (CharType) t;
				VarCharType vct = (VarCharType) t1;
				if (vct.maxLength < ct.length) {
					t = new CharType(vct.maxLength, nullable, labeledNullable);
				} else {
					t = ct;
				}
			} else if (t1 instanceof VarCharType && t instanceof VarCharType) {
				VarCharType v1 = (VarCharType) t1;
				VarCharType v2 = (VarCharType) t;
				if (v1.maxLength < v2.maxLength) {
					t = t1;
				}
			} else {
				throw new TypeError("Cannot equate variables of types " + t1 + " and " + t);
			}
		}

		if (t1.valueKnown()) {
			if (t.labeledNullable) {
				t = t.getWithLabelledNullable(false);
			}
			if (t.nullable) {
				t = t.getWithNullable(false);
			}
			t = t.setConstantValue(t1);
		}

		membersList.add(v);
	}

	void addAll(Iterable<Variable> vars) throws TypeError {
		for (Variable v : vars) {
			add(v);
		}
	}

	public String toString() {
		return (finished ? members : membersList) + "[" + t + "]";
	}

	public int hashCode() {
		if (finished) {
			return hashcode;
		} else {
			throw new IllegalStateException("Cannot compute the hashcode of an unfinished equivClass");
		}
	}

	public boolean equals(Object o) {
		if (! finished) {
			throw new IllegalStateException("Cannot test an unfinished equivClass for equality");
		}
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		EquivClass ec = (EquivClass) o;
		if (! ec.finished) {
			throw new IllegalStateException("Cannot test an unfinished equivClass for equality");
		}
		if (hashcode != ec.hashcode) {
			return false;
		}
		return (members.equals(ec.members));
	}

	Variable replaceChildVariable(Map<? extends Variable, ? extends Variable> mapping, boolean throughEC) throws VariableRemoved {
		if (throughEC) {
			EquivClass ec = new EquivClass();
			boolean changed = false;
			Variable member = null;

			for (Variable v : this) {
				Variable vv;
				try {
					vv = v.replaceChildVariable(mapping, false);
				} catch (VariableRemoved vr) {
					changed = true;
					continue;
				}
				try {
					member = vv == null ? v : vv;
					if (member instanceof EquivClass) {
						ec.addAll((EquivClass) member);
					} else {
						ec.add(member);
					}
				} catch (TypeError te) {
					throw new RuntimeException("Processing equiv class shouldn't cause type error", te);
				}
				changed |= (vv != null);
			}

			if (changed) {
				if (ec.size() == 0) {
					throw new VariableRemoved(this);
				} else if (ec.size() == 1) {
					return member;
				} else {
					ec.setFinished();
					if (ec.equals(this)) {
						return null;
					}
					return ec;
				}
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	/**
	 * Rename the variables in this equiv class
	 * 
	 * @param mapping
	 * @param rt
	 * @return			The modified equiv class variable (or possible
	 * 					just a variable if there is no longer an equiv class),
	 * 					or <code>null</code> if the equiv class has not changed
	 * @throws VariableNotInMapping
	 * 					If there is no mapping for this equivClass variable
	 */
	Variable applyMorphism(Morphism mapping, RelationTypes<?, ?> rt) throws VariableNotInMapping {
		if (mapping == null) {
			return null;
		}
		EquivClass newEc = new EquivClass();
		boolean foundChange = false;
		Variable member = null;
		for (Variable v : this) {
			Variable vv;
			try {
				vv = v.applyMorphism(mapping, rt);
			} catch (VariableNotInMapping mnf) {
				foundChange = true;
				continue;
			}
			try {
				if (vv == null) {
					newEc.add(v);
					member = v;
				} else {
					newEc.add(vv);
					foundChange = true;
					member = vv;
				}
			} catch (Type.TypeError te) {
				throw new RuntimeException("Renaming variables shouldn't cause type error", te);
			}
		}
		if (newEc.size() == 0) {
			throw new VariableNotInMapping(this, mapping);
		} else if (newEc.size() == 1) {
			return member;
		} else if (foundChange) {
			newEc.setFinished();
			return newEc;
		} else {
			return null;
		}
	}


	/**
	 * Split an equiv class into two equiv class based on inputs to the function
	 * 
	 * @param lhsMapping		The mapping for the left equiv class
	 * @param rhsMapping		The mapping for the right equiv class
	 * @param leftFuncs			Functions that should end up in the left side, if present 
	 * @param rightFuncs		Functions that should end up in the right side, if present
	 * @param lhsJoinVars		The list to add the left join variables to
	 * @param rhsJoinVars		The list to add the right join variables to
	 * @param joinEquivClasses	A collection to add the equiv classes used to create join variables to
	 * @param rt				The RelationTypes structure
	 * @return					A pair of <left,right> equivClasses, each of which may be null
	 * 							if the equiv class does not contain a portion on a side of the split
	 */
	Pair<EquivClass,EquivClass> splitEquivClass(Morphism lhsMapping, Morphism rhsMapping,
			Set<Function> leftFuncs, Set<Function> rightFuncs,
			List<Variable> lhsJoinVars, List<Variable> rhsJoinVars,
			Collection<EquivClass> joinEquivClasses,
			RelationTypes<?,?> rt) {
		EquivClass leftEc = new EquivClass(), rightEc = new EquivClass();
		boolean leftNonLit = false, rightNonLit = false;
		for (Variable v : this) {
			try {
				if (v instanceof LiteralVariable) {
					leftEc.add(v);
					rightEc.add(v);
				} else if (v instanceof Aggregate) {
					throw new RuntimeException("Wasn't expecting any aggregates here!");
				} else if (v instanceof Function) {
					try {
						if (leftFuncs.contains(v)) {
							Variable lv = v.applyMorphism(lhsMapping, rt);
							if (lv == null) {
								lv = v;
							}
							leftEc.add(lv);
							leftNonLit = true;
						} else if (rightFuncs.contains(v)) {
							Variable rv = v.applyMorphism(rhsMapping, rt);
							if (rv == null) {
								rv = v;
							}
							rightEc.add(rv);
							rightNonLit = true;
						} else {
							throw new RuntimeException("Can't find variable " + v);
						}
					} catch (VariableNotInMapping vnim) {
						throw new RuntimeException("Functions not partitioned correctly");
					}
				} else if (v instanceof AtomVariable) {
					try {
						Variable lv = v.applyMorphism(lhsMapping, rt);
						if (lv == null) {
							lv = v;
						}
						leftEc.add(lv);
						leftNonLit = true;
					} catch (VariableNotInMapping vnim) {
					}
					try {
						Variable rv = v.applyMorphism(rhsMapping, rt);
						if (rv == null) {
							rv = v;
						}
						rightEc.add(rv);
						rightNonLit = true;
					} catch (VariableNotInMapping vnim) {
					}					
				}
			} catch (Type.TypeError te) {
				throw new RuntimeException("Splitting equiv class should not cause type error", te);
			}
		}
		leftEc.setFinished();
		rightEc.setFinished();
		if (leftNonLit && rightNonLit) {
			if (leftEc.size() == 1) {
				for (Variable v : leftEc) {
					lhsJoinVars.add(v);
				}
			} else {
				lhsJoinVars.add(leftEc);
			}
			if (rightEc.size() == 1) {
				for (Variable v : rightEc) {
					rhsJoinVars.add(v);
				}
			} else {
				rhsJoinVars.add(rightEc);
			}
			joinEquivClasses.add(this);
		}
		if (leftEc.size() <= 1) {
			// It's a trivial equivalence class
			leftEc = null;
		}
		if (rightEc.size() <= 1) {
			rightEc = null;
		}
		return new Pair<EquivClass,EquivClass>(leftEc,rightEc);
	}

	static class NormalizedEquivClasses {
		Set<EquivClass> equivClasses;
		Map<Variable,EquivClass> findECVar;
	}

	static NormalizedEquivClasses normalizeEquivClasses(Set<EquivClass> ecs) {
		Map<Variable,Variable> findEC = new HashMap<Variable,Variable>();
		for (EquivClass ec : ecs) {
			for (Variable v : ec) {
				findEC.put(v, ec);
			}
		}
		try {
			boolean changed = true;
			while (changed) {
				changed = false;
				Set<EquivClass> newEquivClasses = new HashSet<EquivClass>(ecs.size());
				for (EquivClass ec : ecs) {
					Variable v = ec.replaceChildVariable(findEC, true);
					EquivClass ecc = null;
					if (v != null) {
						if (v instanceof EquivClass) {
							ecc = (EquivClass) v;
						} else {
							throw new RuntimeException("Variables should not be removed from equiv classes by normalization");
						}
					}
					if (ecc == null) {
						newEquivClasses.add(ec);
					} else {
						newEquivClasses.add(ecc);
						findEC.put(ec, ecc);
						changed = true;
					}
				}
				ecs =  newEquivClasses;
			}
		} catch (VariableRemoved vr) {
			throw new RuntimeException("Shouldn't get a VariableRemoved without mappings to null", vr);
		}

		Map<Variable,EquivClass> newFindEC = new HashMap<Variable,EquivClass>();

		for (EquivClass ec : ecs) {
			for (Variable v : ec) {
				newFindEC.put(v, ec);
			}
		}

		NormalizedEquivClasses retval = new NormalizedEquivClasses();
		retval.findECVar = newFindEC;
		retval.equivClasses = ecs;
		return retval;
	}

	@Override
	boolean isAggregatedComputable(Set<Variable> groupingVariables) {
		if (groupingVariables.contains(this)) {
			return true;
		}
		/*
		// Assuming the grouping variables have been normalized this
		// is not necessary
		for (Variable v : this) {
			if (v.isAggregatedComputable(groupingVariables)) {
				return true;
			}
		}
		*/
		return false;
	}

	@Override
	boolean isNonAggregatedComputable(Set<Variable> groupingVariables) {
		for (Variable v : this) {
			if (v.isNonAggregatedComputable(groupingVariables)) {
				return true;
			}
		}
		return false;
	}

	@Override
	void getEquivClassesUsed(Set<EquivClass> ecsUsed) {
		if (finished) {
			ecsUsed.addAll(ecsUsed);
		} else {
			ecsUsed.add(this);			
			for (Variable v : this) {
				v.getEquivClassesUsed(ecsUsed);
			}
		}
	}

	@Override
	boolean usesVariable(Variable v) {
		for (Variable vv : this) {
			if (vv.equals(v)) {
				return true;
			}
			if (vv.usesVariable(v)) {
				return true;
			}
		}
		return false;
	}

	/* Induce an ordering on a list of EquivClasses so that every EquivClass
	 * preceeds all those that refer to it (i.e. a topological sort)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(EquivClass other) {
		if (!(this.finished && other.finished)) {
			throw new IllegalArgumentException("Can only sort list of finished equiv classes");
		}
		return this.ecsUsed.size() - other.ecsUsed.size();
	}
	
	Integer getPosForVariable(Bijection<? extends Variable,Integer> findVarPos) {
		Iterator<Variable> it = finished ? members.iterator() : membersList.iterator();

		while (it.hasNext()) {
			Variable v = it.next();
			Integer pos = findVarPos.probeFirst(v);
			if (pos != null) {
				return pos;
			}
		}
		
		return null;
	}

	void getInputVariables(Collection<Variable> vs) {
		Collection<Variable> inputs = finished ? members : membersList;
		vs.addAll(inputs);
		for (Variable v : inputs) {
			v.getInputVariables(vs);
		}
	}	
}
