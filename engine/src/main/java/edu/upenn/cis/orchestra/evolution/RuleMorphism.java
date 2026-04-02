package edu.upenn.cis.orchestra.evolution;

import java.util.Arrays;




public class RuleMorphism {
	public Rule source;
	public Rule target;
	public MorphismType type;
	public int[] varMap;			// varMap[i] = target variable corresponding to source variable i			
	public int[] atomMap;			// atomMap[i] = target atom corresponding to source atom i
	private int position;			// varMap and atomMap are valid for 0 <= i <= position
	private int[] count;			// count[i] = # of occurrences of source variable i in
									// atoms { source.m_body[j] : 0 <= j <= position }
	private int[] inverseVarMap;	// inverseVarMap[i] = source variable corresponding to target variable i
									// (only makes sense for isomorphism, when varMap is injective)
	private int[] inverseAtomMap;	// inverseAtomMap[i] = source atom corresponding to target atom i
									// (only makes sense for isomorphism or substitution, when atomMap is injective)
	private int[] inverseCount;		// inverseCount[i] = # of occurrences of target variable i in
									// atoms { target.m_body[atomMap[j]] : 0 <= j <= position }
	private JoinVar[] joinVars;		// see isSafeSubstitution
	private enum JoinVar {
		UNKNOWN,
		MAPPED,
		UNMAPPED,
		BOTH
	}
	
	public static int NONE = Integer.MIN_VALUE;
	
	static private boolean checkSignatures(Rule source, Rule target, MorphismType type) {
		if (type == MorphismType.ISOMORPHISM) {
			if (source.m_varcount != target.m_varcount ||
					source.m_body.length != target.m_body.length || 
					source.m_head.m_attributes.length != target.m_head.m_attributes.length) {
				return false;
			} else for (int i = 0; i < source.m_body.length; i++) {
				if (source.m_body[i].m_name != target.m_body[i].m_name) {
					return false;
				}
			}
		} else if (type == MorphismType.SUBSTITUTION) {
			if (source.m_body.length > target.m_body.length) {
				return false;
			} else for (int i = 0; i < source.m_body.length; ) {
				// count the atoms with this name
				int name = source.m_body[i].m_name;
				int count = 1;
				while (++i < source.m_body.length && source.m_body[i].m_name == name) {
					count++;
				}
				for (int j = 0; j < target.m_body.length; j++) {
					if (target.m_body[j].m_name == name) {
						count--;
					}
				}
				if (count > 0) {
					return false;
				}
			}
		} else { // MorphismType.HOMOMORPHISM
			if (source.m_head.m_attributes.length != target.m_head.m_attributes.length) {
				return false;
			}
		}
		return true;
	}
	
	static public RuleMorphism first(Rule source, Rule target, MorphismType type) {
		// make sure the signatures are compatible before doing any real work
		if (!checkSignatures(source, target, type)) {
			return null;
		}
		// allocate candidate morphism and align the heads
		RuleMorphism morph = new RuleMorphism(source, target, type);
		if (type != MorphismType.SUBSTITUTION && morph.extendMorphism(source.m_head, target.m_head) == false) {
			// couldn't align the heads, so no morphism
			return null;
		} else if (morph.next() == false) {
			// couldn't find a first morphism
			return null;
		}
		// success
		return morph;
	}
	
	public boolean extendMorphism(Atom src, Atom tgt) {
		// make sure the variable mapping extends consistently
		for (int i = 0; i < src.m_attributes.length; i++) {
			if (Atom.isConstant(src.m_attributes[i])) {
				if (src.m_attributes[i] != tgt.m_attributes[i]) {
					// does not extend consistently
					return false;
				}
			} else if (count[src.m_attributes[i]] != 0 &&
					varMap[src.m_attributes[i]] != tgt.m_attributes[i]) {
				// does not extend consistently
				return false;
			} else if (type == MorphismType.ISOMORPHISM) {
				if (Atom.isConstant(tgt.m_attributes[i]) || 
					(inverseCount[tgt.m_attributes[i]] != 0 &&
					inverseVarMap[tgt.m_attributes[i]] != src.m_attributes[i])) {
					// does not extend consistently
					return false;
				}
			}
		}
		// it's consistent.  go ahead and extend the mapping.
		for (int i = 0; i < src.m_attributes.length; i++) {
			if (!Atom.isConstant(src.m_attributes[i])) {
				varMap[src.m_attributes[i]] = tgt.m_attributes[i];
				count[src.m_attributes[i]]++;
				if (type == MorphismType.ISOMORPHISM) {
					inverseVarMap[tgt.m_attributes[i]] = src.m_attributes[i];
					inverseCount[tgt.m_attributes[i]]++;
				}
			}
		}
		return true;
	}
	
	public void retractMorphism(Atom src, Atom tgt) {
		for (int i = 0; i < src.m_attributes.length; i++) {
			if (!Atom.isConstant(src.m_attributes[i])) {
				count[src.m_attributes[i]]--;
			}
		}
		if (type == MorphismType.ISOMORPHISM) {
			for (int i = 0; i < tgt.m_attributes.length; i++) {
				if (!Atom.isConstant(tgt.m_attributes[i])) {
					inverseCount[tgt.m_attributes[i]]--;
				}
			}
		}
	}
	
	public boolean pushAtom(int index) {
		Atom src = source.m_body[position];
		Atom tgt = target.m_body[index];
		if (src.m_name != tgt.m_name) {
			return false;
		} else if ((type == MorphismType.ISOMORPHISM || type == MorphismType.SUBSTITUTION) &&
			inverseAtomMap[index] != NONE) {
			// this atom is already mapped onto
			return false;
		} else if (extendMorphism(src,tgt)) {
			inverseAtomMap[index] = position;
			atomMap[position++] = index;
			return true;
		}
		return false;
	}
	
	public void popAtom() {
		Atom src = source.m_body[position-1];
		Atom tgt = target.m_body[atomMap[position-1]];
		retractMorphism(src,tgt);
		inverseAtomMap[atomMap[position-1]] = NONE;
		atomMap[position-1] = NONE;
		--position;
	}

	public boolean hasAtom(int atom) {
		for (int i = 0; i < position; i++) {
			if (atomMap[i] == atom) {
				return true;
			}
		}
		return false;
	}
	
	private boolean isSafeSubstitution() {
		// initialize the target query join variables array
		if (joinVars == null) {
			joinVars = new JoinVar[target.m_varcount];
		}
		Arrays.fill(joinVars, JoinVar.UNKNOWN);
		// compute the set of join variables
		for (int variable : target.m_head.m_attributes) {
			if (!Atom.isConstant(variable)) {
				joinVars[variable] = JoinVar.UNMAPPED;
			}
		}
		for (int i = 0; i < target.m_body.length; i++) {
			JoinVar value = inverseAtomMap[i] < 0 ? JoinVar.UNMAPPED : JoinVar.MAPPED;
			int[] attributes = target.m_body[i].m_attributes;
			for (int j = 0; j < attributes.length; j++) {
				int variable = attributes[j];
				if (!Atom.isConstant(variable)) {
					if (joinVars[variable] == JoinVar.UNKNOWN) {
						// first time we've seen it
						joinVars[variable] = value;
					} else if (joinVars[variable] != value) {
						// we've seen it before, and this is a join variable
						joinVars[variable] = JoinVar.BOTH;
					}
				}
			}
		}
		// check that each join variable occurs in the head of view
		// and that variable mapping is injective on non-join variables
		for (int i = 0; i < joinVars.length; i++) {
			if (joinVars[i] == JoinVar.BOTH) {
				// join variable.  check that it is in the mapped head.
				boolean found = false;
				int[] head = source.m_head.m_attributes;
				for (int j = 0; j < head.length; j++) {
					if (!Atom.isConstant(head[j]) && varMap[head[j]] == i) {
						// yes, in the mapped head
						found = true;
						break;
					}
				}
				if (!found) {
					// not in the mapped head, substitution is unsafe
					return false;
				}
			} else if (joinVars[i] == JoinVar.MAPPED) {
				// mapped variable.  check that only one source variable maps
				// to it.
				boolean found = false;
				for (int j = 0; j < source.m_varcount; j++) {
					if (varMap[j] == i) {
						if (found) {
							// two source variables map to i, substitution is unsafe
							return false;
						} else {
							found = true;
						}
					}
				}
			}
		}
		// check that each variable mapped to a constant also appears in head
		for (int i = 0; i < varMap.length; i++) {
			if (varMap[i] != NONE && Atom.isConstant(varMap[i])) {
				int[] head = source.m_head.m_attributes;
				boolean found = false;
				for (int j = 0; j < head.length; j++) {
					if (head[j] == i) {
						found = true;
						break;
					}
				}
				if (!found) {
					// not in the head, substitution is unsafe
					return false;
				}
			}
		}
		return true;
	}
	
	public RuleMorphism(Rule source, Rule target, MorphismType type) {
		this.source = source;
		this.target = target;
		this.type = type;
		this.varMap = new int[source.m_varcount];
		this.atomMap = new int[source.m_body.length];
		this.count = new int[source.m_varcount];
		this.inverseVarMap = new int[target.m_varcount];
		this.inverseCount = new int[target.m_varcount];
		this.inverseAtomMap = new int[target.m_body.length];
		Arrays.fill(varMap, NONE);
		Arrays.fill(atomMap, NONE);
		Arrays.fill(inverseVarMap, NONE);
		Arrays.fill(inverseAtomMap, NONE);
	}
	
	public RuleMorphism(RuleMorphism m) {
		source = m.source;
		target = m.target;
		type = m.type;
		varMap = m.varMap.clone();
		atomMap = m.atomMap.clone();
		position = m.position;
	}

	public boolean next() {
		int current;
		if (position == 0) {
			current = -1;
		} else {
			assert(position == source.m_body.length);
			current = atomMap[position-1];
			popAtom();
		}
		while (position < source.m_body.length) {
			boolean success = false;
			for (int i = current+1; i < target.m_body.length; i++) {
				if (pushAtom(i)) {
					if (type == MorphismType.SUBSTITUTION && 
							position == source.m_body.length && 
							!isSafeSubstitution()) {
						popAtom();
					} else {
						success = true;
						break;
					}
				}
			}
			if (success) {
				current = -1;
			} else if (position > 0) {
				current = atomMap[position-1];
				popAtom();
			} else {
				assert(position == 0);
				return false;
			}
		}
		assert(position == source.m_body.length);
		return true;
	}

//	private boolean first() {
//		while (0 <= position && position < source.m_body.length) {
//			boolean success = false;
//			for (int i = atomMap[position]+1; i < source.m_body.length; i++) {
//				if (pushAtom(i)) {
//					if (type == MorphismType.SUBSTITUTION && 
//							position == source.m_body.length && 
//							!isSafeSubstitution()) {
//						popAtom();
//					} else {
//						success = true;
//						break;
//					}
//				}
//			}
//			if (!success) {
//				popAtom();
//			}
//		}
//		assert(position == -1 || position == source.m_body.length);
//		return position == source.m_body.length;
//	}
	
	public String toString() {
		return varMap.toString();
	}
}
