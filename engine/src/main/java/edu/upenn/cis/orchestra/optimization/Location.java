package edu.upenn.cis.orchestra.optimization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.optimization.RelationTypes.MaterializedView;
import edu.upenn.cis.orchestra.p2pqp.plan.CentralizedLoc;
import edu.upenn.cis.orchestra.p2pqp.plan.DistributedLoc;
import edu.upenn.cis.orchestra.p2pqp.plan.NamedLoc;
import edu.upenn.cis.orchestra.util.Pair;
import edu.upenn.cis.orchestra.util.SubsetIterator;

public class Location extends PhysicalProperties implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final Location CENTRALIZED = new Location(false, false);
	public static final Location FULLY_REPLICATED = new Location(true, false);
	private static final Location CENTRALIZED_WILL_BE_REGROUPED = new Location(false, true);

	// If both are null, location represents the query coordinator
	// If hashVars is not null, then location represents data striped across
	// the DHT
	// If namedNode is not null, then the location represents a single
	// node identified by a name
	private final Set<Variable> hashVars;
	private final String namedNode;

	// True if the tuples will be regrouped so grouping
	// can occur even if all tuples of a node 
	private final boolean willBeRegrouped;

	// True if a base table is replicated at each node, false otherwise
	private final boolean fullyReplicated;

	public Location(Collection<? extends Variable> hashVars) {
		this(hashVars, false);
	}

	private Location(Collection<? extends Variable> hashVars, boolean willBeRegrouped) {
		if (hashVars.isEmpty()) {
			this.hashVars = null;
			if (willBeRegrouped) {
				throw new IllegalArgumentException("Cannot have a central location that will regrouped or is fully replicated");
			}
		} else {
			this.hashVars = Collections.unmodifiableSet(new HashSet<Variable>(hashVars));
		}
		namedNode = null;
		this.willBeRegrouped = willBeRegrouped;
		this.fullyReplicated = false;
		if (willBeRegrouped && fullyReplicated) {
			throw new IllegalArgumentException("Should never have a location that will both be regrouped and be fully replicated ");
		}
	}


	private Location(boolean fullyReplicated, boolean willBeRegrouped) {
		hashVars = null;
		namedNode = null;
		this.fullyReplicated = fullyReplicated;
		if (fullyReplicated && willBeRegrouped) {
			throw new IllegalArgumentException("fullyReplicated and willBeRegrouped are incompatible");
		}
		this.willBeRegrouped = willBeRegrouped;
	}

	private Location(String namedNode, boolean willBeRegrouped) {
		this.hashVars = null;
		this.namedNode = namedNode;
		fullyReplicated = false;
		this.willBeRegrouped = willBeRegrouped;
	}

	public Location(String namedNode) {
		this(namedNode,false);
	}

	public boolean canShipTo(Expression e) {
		if (hashVars == null) {
			return true;
		}
		return e.head.containsAll(hashVars);
	}

	public boolean canShipTo(VariablePosition vp) {
		if (hashVars == null) {
			return true;
		}

		for (Variable v : hashVars) {
			if (vp.getPos(v) == null) {
				return false;
			}
		}

		return true;
	}

	public boolean isValidFor(Expression e) {
		if (hashVars == null) {
			return true;
		}
		
		for (Variable v : hashVars) {
			if (v instanceof AtomVariable) {
				AtomVariable av = (AtomVariable) v;
				Integer count = e.relAtoms.get(av.relation);
				if (count == null || count < av.occurrence) {
					return false;
				}
			} else if (v instanceof Aggregate) {
				if (e.groupBy == null) {
					return false;
				}
				if (! e.aggregates.contains(v)) {
					return false;
				}
			} else if (v instanceof Function) {
				if (! e.functions.contains(v)) {
					return false;
				}
			} else if (v instanceof EquivClass) {
				if (! e.equivClasses.contains(v)) {
					return false;
				}
			} else {
				throw new IllegalArgumentException("Don't know what to do with variable " + v);
			}
			
			if ((!(v instanceof EquivClass)) && e.findEquivClass.containsKey(v)) {
				return false;
			}
		}
		return true;
	}

	public Set<Variable> getHashVars() {
		if (hashVars == null) {
			throw new IllegalStateException("Location does not represent striped data");
		}
		return hashVars;
	}

	public String getLocationName() {
		if (namedNode == null) {
			throw new IllegalStateException("Location does not represent a named location");
		}
		return namedNode;
	}

	public boolean isFullyReplicated() {
		return fullyReplicated;
	}

	public boolean isCentralized() {
		return namedNode == null && hashVars == null && (! fullyReplicated);
	}

	public boolean isDistributed() {
		return hashVars != null;
	}

	public boolean isNamedLocation() {
		return namedNode != null;
	}

	public boolean willBeRegrouped() {
		return willBeRegrouped;
	}

	public Location updateWithEquivClasses(Map<? extends Variable, ? extends EquivClass> findEC) {
		if (! this.isDistributed()) {
			return this;
		}
		Set<Variable> newHashVars = new HashSet<Variable>(hashVars.size());
		boolean changed = false;
		try {
			for (Variable v : hashVars) {
				Variable vv = v.replaceVariable(findEC, true);
				if (vv == null) {
					newHashVars.add(v);
				} else {
					changed = true;
					newHashVars.add(vv);
				}
			}
		} catch (VariableRemoved vr) {
			throw new RuntimeException(vr);
		}

		if (changed) {
			return new Location(newHashVars, this.willBeRegrouped);
		} else {
			return this;
		}
	}

	public Location getWillNotBeRegrouped() {
		if (! willBeRegrouped) {
			throw new IllegalArgumentException("Current location will already not be regrouped");
		}

		if (this.isCentralized()) {
			return CENTRALIZED;
		} else if (this.isDistributed()) {
			return new Location(this.hashVars);
		} else if (this.isNamedLocation()) {
			return new Location(this.namedNode);
		} else {
			throw new RuntimeException("Need to add code to process this kind of location");
		}
	}

	public Location getWillBeRegrouped() {
		if (willBeRegrouped) {
			throw new IllegalArgumentException("Current location will already be regrouped");
		}

		if (this.isCentralized()) {
			return CENTRALIZED_WILL_BE_REGROUPED;
		} else if (this.isDistributed()) {
			return new Location(this.hashVars, true);
		} else if (this.isNamedLocation()) {
			return new Location(this.namedNode, true);
		} else {
			throw new RuntimeException("Need to add code to process this kind of location");
		}
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Location l = (Location) o;

		if ((willBeRegrouped ^ l.willBeRegrouped) || (fullyReplicated ^ l.fullyReplicated)) {
			return false;
		}

		if (namedNode != null) {
			return namedNode.equals(l.namedNode);
		} else if (hashVars != null) {
			return hashVars.equals(l.hashVars);
		} else {
			return (l.namedNode == null && l.hashVars == null);
		}
	}

	public boolean noShippingNeeded(Location to) {
		if (isFullyReplicated() || to.isFullyReplicated()) {
			return true;
		} else if (isCentralized()) {
			return to.isCentralized();
		} else if (isDistributed()) {
			if (! to.isDistributed()) {
				return false;
			}
			return (hashVars.equals(to.hashVars));
		} else if (isNamedLocation()) {
			return namedNode.equals(to.namedNode);
		} else {
			throw new RuntimeException("Need to implement");
		}
	}

	public int hashCode() {
		int hashCode;
		if (namedNode == null && hashVars == null) {
			if (fullyReplicated) {
				hashCode = 7;
			} else {
				hashCode = 0;
			}
		} else if (hashVars != null) {
			hashCode = hashVars.hashCode();
		} else {
			hashCode = namedNode.hashCode();
		}

		if (willBeRegrouped) {
			hashCode += 1021;
		}

		return hashCode;
	}


	public String toString() {
		String retval;
		if (namedNode != null) {
			retval = namedNode;
		} else if (hashVars != null) {
			retval = hashVars.toString();
		} else if (fullyReplicated) {
			retval = "FULLY_REPLICATED";
		} else {
			retval = "CENTRALIZED";
		}
		if (this.willBeRegrouped) {
			retval += "(WILL BE REGROUPED)";
		}
		return retval;
	}

	public edu.upenn.cis.orchestra.p2pqp.plan.Location getPlanLocation() {
		if (isCentralized()) {
			return CentralizedLoc.getInstance();
		} else if (isFullyReplicated()) {
			return DistributedLoc.getReplicatedInstance();
		} else if (isDistributed()) {
			return DistributedLoc.getInstance();
		} else {
			return new NamedLoc(namedNode);
		}
	}

	public Location applyMorphism(Morphism m, RelationTypes<?,?> rt) throws VariableNotInMapping {
		if (m == null || hashVars == null || hashVars.isEmpty()) {
			return this;
		}

		boolean changed = false;
		Set<Variable> morphed = new HashSet<Variable>(hashVars.size());
		for (Variable v : hashVars) {
			Variable vv = v.applyMorphism(m, rt);
			if (vv == null) {
				vv = v;
			} else {
				changed = true;
			}
			morphed.add(vv);
		}
		if (! changed) {
			return this;
		} else {
			return new Location(morphed, this.willBeRegrouped);
		}
	}

	public static class Factory implements PhysicalPropertiesFactory<Location> {
		private Map<ViewSummary,Set<Location>> findViewLocs;

		public Factory() {
			findViewLocs = Collections.emptyMap();
		}

		public Factory(RelationTypes<Location,?> rt) {
			findViewLocs = new HashMap<ViewSummary,Set<Location>>();
			for (String name : rt.getMaterializedViewNames()) {
				MaterializedView<Location,?> mv = rt.getMaterializedView(name);
				for (Map<String,Integer> relAtoms : reduceNumbers(mv.exp.relAtoms)) {
					for (boolean b : new boolean[] {true, false}) {
						ViewSummary vs = new ViewSummary(relAtoms, b);
						Set<Location> locs = findViewLocs.get(vs);
						if (locs == null) {
							locs = new HashSet<Location>();
							findViewLocs.put(vs, locs);
						}
						locs.add(mv.props);
					}
				}
			}
		}

		private static <T> List<Map<T,Integer>> reduceNumbers(Map<T,Integer> input) {
			List<Map<T,Integer>> result = new ArrayList<Map<T,Integer>>();
			if (input.isEmpty()) {
				return result;
			}
			Map.Entry<T,Integer> entry = input.entrySet().iterator().next();
			T val = entry.getKey();
			int count = entry.getValue();


			HashMap<T,Integer> recursiveInput = new HashMap<T,Integer>(input);
			recursiveInput.remove(val);
			List<Map<T,Integer>> recursive = reduceNumbers(recursiveInput);
			result.addAll(recursive);

			for (int i = 1; i <= count; ++i) {
				for (Map<T,Integer> recMap : recursive) {
					HashMap<T,Integer> newMap = new HashMap<T,Integer>(recMap);
					recMap.put(val, i);
					result.add(newMap);
				}
			}
			return result;
		}

		public Iterator<Location> getRelevantViewProperties(Expression e) {
			Set<Location> locs = findViewLocs.get(new ViewSummary(e));
			if (locs == null) {
				locs = Collections.emptySet();
			}
			return locs.iterator();
		}

		public Iterator<Location> enumerateAllProperties(Expression e,
				RelationTypes<? extends Location, ?> rt) {

			Set<Location> viewLocs = findViewLocs.get(new ViewSummary(e));

			int size = 1;
			if (viewLocs != null) {
				size += viewLocs.size();
			}
			size += e.equivClasses.size();
			if (e.groupBy != null) {
				size += (int) Math.pow(2, e.groupBy.size());
			}

			List<Pair<String,Integer>> atoms = new ArrayList<Pair<String,Integer>>();
			for (Map.Entry<String,Integer> me : e.relAtoms.entrySet()) {
				String rel = me.getKey();
				int numOcc = me.getValue();
				for (int i = 1; i <= numOcc; ++i) {
					atoms.add(new Pair<String,Integer>(rel,i));
				}
			}

			size += atoms.size();

			Set<Location> result = new HashSet<Location>(size);
			if (viewLocs != null) {
				for (Location loc : viewLocs) {
					result.add(loc.updateWithEquivClasses(e.findEquivClass));
				}
			}


			for (EquivClass ec : e.equivClasses) {
				result.add(new Location(Collections.singleton(ec)));
			}
			result.add(Location.CENTRALIZED);
			if (e.groupBy != null) {
				Iterator<Set<Variable>> subsets = new SubsetIterator<Variable>(e.groupBy, true);
				while (subsets.hasNext()) {
					Set<? extends Variable> subset = subsets.next();
					if (! subset.isEmpty()) {
						result.add(new Location(subset));
					}
				}
			}
			for (Pair<String,Integer> relOcc : atoms) {
				String rel = relOcc.getFirst();
				int occ = relOcc.getSecond();
				Location baseLoc = rt.getRelationProps(rel);
				Morphism m = new Morphism(1);
				m.mapOccurrence(rel, 1, occ);
				Location morphed = baseLoc.applyMorphism(m, rt).updateWithEquivClasses(e.findEquivClass);
				result.add(morphed);
			}
			
			
			
			return result.iterator();
		}
	}
}
