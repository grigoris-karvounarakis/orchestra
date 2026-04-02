package edu.upenn.cis.orchestra.optimization;

import java.util.HashMap;
import java.util.Map;

public class Morphism {
	private Map<RelOccPair,Integer> map;
	private boolean finished = false;

	public Morphism(int initialSize) {
		map = new HashMap<RelOccPair,Integer>(initialSize);
	}

	public Morphism() {
		map = new HashMap<RelOccPair,Integer>();
	}

	private Morphism createInverse() {
		Morphism inverse = new Morphism();

		for (Map.Entry<RelOccPair, Integer> me : map.entrySet()) {
			inverse.mapOccurrence(me.getKey().relation, me.getValue(), me.getKey().occ);
		}

		return inverse;
	}

	public void addMappingsFromMorphism(Morphism otherMorphism) {
		if (finished) {
			throw new IllegalStateException("Cannot add mappings to a finished morphism");
		}
		map.putAll(otherMorphism.map);
	}

	public void mapOccurrence(String relation, int origOcc, int newOcc) {
		if (finished) {
			throw new IllegalStateException("Cannot add mappings to a finished morphism");
		}
		map.put(new RelOccPair(relation, origOcc), newOcc);
	}

	/**
	 * Get the atom variable that this morphism creates from the
	 * old atom variable, or null if the atom variable is not mapped
	 * by the morphism
	 * 
	 * @param av			The variable to map
	 * @param rt			Information about the relations
	 * @return				The mapped variable, or <code>null</code> if it
	 * 						is not mapped
	 */
	public AtomVariable mapAtomVariable(AtomVariable av, RelationTypes<?,?> rt) throws VariableNotInMapping {
		Integer newOcc = map.get(new RelOccPair(av.relation, av.occurrence));
		if (newOcc == null) {
			throw new VariableNotInMapping(av, this);
		} else {
			AtomVariable mapped = new AtomVariable(av.relation, newOcc, av.position, rt);
			// We may as well do some simple coalescing
			if (mapped.equals(av)) {
				return av;
			} else {
				return mapped;
			}
		}
	}

	public String toString() {
		return map.toString();
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}

		Morphism morph = (Morphism) o;
		return map.equals(morph.map);
	}

	public void finish() {
		finished = true;
	}

	public boolean isFinished() {
		return finished;
	}

	public Morphism duplicate() {
		Morphism m = new Morphism(this.map.size());
		m.map.putAll(this.map);
		return m;
	}

	private Morphism composeWith(Morphism m) throws VariableNotInMapping {
		if (m == null) {
			return this;
		}
		Morphism comp = new Morphism();
		for (Map.Entry<RelOccPair, Integer> entry : this.map.entrySet()) {
			RelOccPair output = new RelOccPair(entry.getKey().relation, entry.getValue());
			Integer compOutput = m.map.get(output);
			if (compOutput == null) {
				throw new VariableNotInMapping(output.relation, output.occ, m);
			}
			comp.mapOccurrence(output.relation, entry.getKey().occ, compOutput);
		}
		return comp;
	}

	public static Morphism compose(Morphism m1, Morphism m2) throws VariableNotInMapping {
		if (m1 == null) {
			return m2;
		} else {
			return m1.composeWith(m2);
		}
	}
	
	public static Morphism createInverse(Morphism m) {
		if (m == null) {
			return null;
		} else {
			return m.createInverse();
		}
	}
}
