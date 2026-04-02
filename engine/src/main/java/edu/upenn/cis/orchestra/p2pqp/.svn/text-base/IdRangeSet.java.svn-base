package edu.upenn.cis.orchestra.p2pqp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class IdRangeSet implements Iterable<IdRange>, Comparable<IdRangeSet> {
	// remainingRanges is always sorted, and never span zero
	private List<IdRange> remainingRanges;
	private boolean full;

	public static IdRangeSet full() {
		return new IdRangeSet(true);
	}

	public static IdRangeSet empty() {
		return new IdRangeSet(false);
	}

	private IdRangeSet(boolean full) {
		remainingRanges = new ArrayList<IdRange>();
		this.full = full;
	}

	public IdRangeSet(IdRange remaining) {
		remainingRanges = new ArrayList<IdRange>();
		if (remaining.isFull()) {
			full = true;
		} else if (remaining.isEmpty()) {
			full = false;
		} else {
			remainingRanges.add(remaining);
			full = false;
		}
	}

	public boolean isEmpty() {
		return (! full) && remainingRanges.isEmpty();
	}

	private static final Comparator<Object> idComparator = new Comparator<Object>() {
		@Override
		public int compare(Object arg0, Object arg1) {
			Id id0, id1;
			if (arg0 instanceof Id) {
				id0 = (Id) arg0;
			} else {
				id0 = ((IdRange) arg0).getCCW();
			}
			if (arg1 instanceof Id) {
				id1 = (Id) arg1;
			} else {
				id1 = ((IdRange) arg1).getCCW();
			}
			return id0.compareTo(id1);
		}

	};

	public boolean contains(Id id) {
		if (full) {
			return true;
		} else if (remainingRanges.isEmpty()) {
			return false;
		}

		int pos = Collections.binarySearch(remainingRanges, id, idComparator);

		if (pos == -1) {
			pos = remainingRanges.size() - 1;
		} else if (pos < 0) {
			pos = -(pos + 2);
		}

		return remainingRanges.get(pos).contains(id);
	}

	public void remove(IdRange toRemove) {
		if (toRemove.isEmpty()) {
			return;
		} else if (toRemove.isFull()) {
			full = false;
			remainingRanges.clear();
			return;
		}
		if (full) {
			full = false;
			remainingRanges.add(new IdRange(toRemove.getCW(), toRemove.getCCW()));
			return;
		} else if (remainingRanges.isEmpty()) {
			return;
		}
		int pos = Collections.binarySearch(remainingRanges, toRemove);
		// We want to start with the first range that precedes toRemove.getCCW(),
		// and keep going until we're past the end of toRemove
		if (pos == -1) {
			pos = remainingRanges.size() - 1;
		} else if (pos < 0) {
			pos = -(pos + 2);
		}

		boolean checkedPrior = false;
		while (pos < remainingRanges.size()) {
			IdRange current = remainingRanges.get(pos);
			if (current.equals(toRemove)) {
				remainingRanges.remove(pos);
			} else if (current.contains(toRemove)) {
				if (current.getCCW().equals(toRemove.getCCW())) {
					remainingRanges.set(pos, new IdRange(toRemove.getCW(), current.getCW()));
				} else if (current.getCW().equals(toRemove.getCW())) {
					remainingRanges.set(pos, new IdRange(current.getCCW(), toRemove.getCCW()));
				} else {
					remainingRanges.set(pos, new IdRange(toRemove.getCW(), current.getCW()));
					remainingRanges.add(pos, new IdRange(current.getCCW(), toRemove.getCCW()));
				}
				break;
			} else if (toRemove.contains(current)) {
				remainingRanges.remove(pos);
			} else if (current.intersects(toRemove)) {
				IdRange newRange;

				if (current.contains(toRemove.getCCW())) {
					newRange = new IdRange(current.getCCW(), toRemove.getCCW());
				} else {
					newRange = new IdRange(toRemove.getCW(), current.getCW());
				}

				remainingRanges.set(pos, newRange);
				++pos;
			} else if (checkedPrior) {
				break;
			} else {
				++pos;
			}
			if (pos == remainingRanges.size()) {
				pos = 0;
			}
			checkedPrior = true;
		}

		if (remainingRanges.size() >= 2 && remainingRanges.get(remainingRanges.size()-1).compareTo(remainingRanges.get(0)) < 0) {
			// Split in last range means we need to shuffle things around to make them sorted again
			remainingRanges.add(0, remainingRanges.remove(remainingRanges.size() - 1));
		}
	}

	public void add(IdRangeSet toAdd) {
		if (toAdd.isFull()) {
			this.full = true;
			this.remainingRanges.clear();
		} else {
			for (IdRange range : toAdd) {
				add(range);
			}
		}
	}

	public void add(IdRange toAdd) {
		if (toAdd.isEmpty()) {
			return;
		} else if (toAdd.isFull()) {
			full = true;
			remainingRanges.clear();
			return;
		} else if (full) {
			return;
		}

		remove(toAdd);

		int pos = Collections.binarySearch(remainingRanges, toAdd);

		if (pos >= 0) {
			throw new IllegalStateException();
		} else if (pos < 0) {
			pos = -(pos + 1);
		}

		remainingRanges.add(pos, toAdd);

		int curr = 0;
		while (curr + 1 < remainingRanges.size()) {
			IdRange currRange = remainingRanges.get(curr);
			IdRange nextRange = remainingRanges.get(curr + 1);
			if (currRange.getCW().equals(nextRange.getCCW())) {
				remainingRanges.set(curr, new IdRange(currRange.getCCW(), nextRange.getCW()));
				remainingRanges.remove(curr + 1);
			} else {
				++curr;
			}
		}

		if (remainingRanges.size() >= 2) {
			IdRange first = remainingRanges.get(0);
			IdRange last = remainingRanges.get(remainingRanges.size() - 1);
			if (last.getCW().equals(first.getCCW())) {
				remainingRanges.remove(remainingRanges.size() - 1);
				remainingRanges.remove(0);
				remainingRanges.add(new IdRange(last.getCCW(), first.getCW()));
			}
		}

		if (remainingRanges.size() == 1 && remainingRanges.get(0).isFull()) {
			remainingRanges.clear();
			full = true;
		}

	}

	public String toString() {
		if (full) {
			return "[full]";
		} else {
			return remainingRanges.toString();
		}
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != IdRangeSet.class) {
			return false;
		}

		IdRangeSet set = (IdRangeSet) o;

		return full == set.full && remainingRanges.equals(set.remainingRanges);
	}

	public int hashCode() {
		if (full) {
			return Integer.MAX_VALUE;
		}
		return remainingRanges.hashCode();
	}

	public boolean isFull() {
		return full;
	}

	public Iterator<IdRange> iterator() {
		if (full) {
			return Collections.singletonList(IdRange.full()).iterator();
		} else {
			return Collections.unmodifiableList(remainingRanges).iterator();
		}
	}

	public void serialize(OutputBuffer buf) {
		buf.writeBoolean(full);
		if (full) {
			return;
		}
		buf.writeInt(remainingRanges.size());
		for (IdRange r : remainingRanges) {
			r.serialize(buf);
		}
	}

	private IdRangeSet(List<IdRange> remainingRanges) {
		this.remainingRanges = remainingRanges;
		this.full = false;
	}

	public static IdRangeSet deserialize(InputBuffer buf) {
		boolean full = buf.readBoolean();
		if (full) {
			return full();
		}
		int numRanges = buf.readInt();
		if (numRanges == 0) {
			return empty();
		}
		List<IdRange> ranges = new ArrayList<IdRange>(numRanges);
		for (int i = 0; i < numRanges; ++i) {
			ranges.add(IdRange.deserialize(buf));
		}
		return new IdRangeSet(ranges);
	}



	public IdRangeSet clone() {
		if (full) {
			return full();
		} else if (remainingRanges.isEmpty()) {
			return empty();
		} else {
			return new IdRangeSet(new ArrayList<IdRange>(remainingRanges));
		}
	}

	public Set<IdRange> getRanges() {
		if (this.full) {
			return Collections.singleton(IdRange.full());
		} else if (remainingRanges.isEmpty()) {
			return Collections.singleton(IdRange.empty());
		} else {
			return new HashSet<IdRange>(remainingRanges);
		}
	}

	static final double totalRange = Id.MAX.doubleValue();

	public double remainingFrac() {
		if (full) {
			return 1.0;
		}
		double remainingRange = 0.0;
		for (IdRange range : remainingRanges) {
			remainingRange += range.getSize();
		}

		return remainingRange / totalRange;
	}

	public IdRange[] toArray() {
		if (full) {
			return new IdRange[] { IdRange.full() };
		} else if (remainingRanges.isEmpty()) {
			return new IdRange[] { IdRange.empty() };
		} else {
			IdRange[] retval = new IdRange[remainingRanges.size()];
			return remainingRanges.toArray(retval);
		}
	}

	public double getSize() {
		if (full) {
			return Id.MAX.doubleValue();
		}
		double retval = 0.0;
		for (IdRange range : remainingRanges) {
			retval += range.getSize();
		}
		return retval;
	}

	public void intersect(IdRange range) {
		if (range.isFull() || this.isEmpty()) {
			return;
		} else if (range.isEmpty()) {
			this.remainingRanges.clear();
			this.full = false;
			return;
		} else if (this.isFull()) {
			this.full = false;
			remainingRanges.clear();
			remainingRanges.add(range);
			return;
		}
		int pos = Collections.binarySearch(remainingRanges, range.getCCW(), idComparator);

		if (pos == -1) {
			pos = remainingRanges.size() - 1;
		} else if (pos < 0) {
			pos = -(pos + 2);
		}
		final int start = pos;
		boolean first = true;
		List<IdRange> remaining = new ArrayList<IdRange>();
		for ( ; ; ) {
			IdRange intersect = remainingRanges.get(pos).intersect(range);
			if (intersect.isEmpty()) {
				if (! first) {
					break;
				}
			} else {
				remaining.add(intersect);
			}
			++pos;
			first = false;
			if (pos >= remainingRanges.size()) {
				pos = 0;
			}
			if (pos == start) {
				break;
			}
		}
		this.remainingRanges = remaining;
	}

	public boolean intersects(IdRange range) {
		if (this.isEmpty() || range.isEmpty()) {
			return false;
		} else if (this.isFull() || range.isFull()) {
			return true;
		}
		int pos = Collections.binarySearch(remainingRanges, range.getCCW(), idComparator);

		if (pos == -1) {
			pos = remainingRanges.size() - 1;
		} else if (pos < 0) {
			pos = -(pos + 2);
		}
		final int start = pos;
		boolean first = true;
		for ( ; ; ) {
			IdRange intersect = remainingRanges.get(pos).intersect(range);
			if (intersect.isEmpty()) {
				if (! first) {
					return false;
				}
			} else {
				return true;
			}
			++pos;
			first = false;
			if (pos >= remainingRanges.size()) {
				pos = 0;
			}
			if (pos == start) {
				return false;
			}
		}
	}
	
	public IdRangeSet createIntersection(IdRangeSet other) {
		if (other.isFull()) {
			return this.clone();
		} else if (other.isEmpty()) {
			return empty();
		}
		IdRangeSet retval = empty();
		for (IdRange range : this) {
			IdRangeSet remaining = other.clone();
			remaining.intersect(range);
			retval.add(remaining);
		}
		return retval;
	}

	public boolean contains(IdRange range) {
		if (this.isFull() || range.isEmpty()) {
			return true;
		} else if (range.isFull() || this.isEmpty()) {
			return false;
		}
		
		// Both are non-full and non-empty
		
		int pos = Collections.binarySearch(remainingRanges, range.getCCW(), idComparator);

		if (pos == -1) {
			pos = remainingRanges.size() - 1;
		} else if (pos < 0) {
			pos = -(pos + 2);
		}

		return remainingRanges.get(pos).contains(range);
	}

	public void remove(IdRangeSet ranges) {
		if (ranges.isFull()) {
			this.full = false;
			this.remainingRanges.clear();
		} else if (ranges.isEmpty()) {
			return;
		} else {
			for (IdRange range : ranges) {
				this.remove(range);
			}
		}
	}

	public int compareTo(IdRangeSet ranges) {
		if (this.isFull()) {
			if (ranges.isFull()) {
				return 0;
			} else {
				return -1;
			}
		} else if (ranges.isFull()) {
			return 1;
		} else if (this.isEmpty()) {
			if (ranges.isEmpty()) {
				return 0;
			} else {
				return 1;
			}
		} else if (ranges.isEmpty()) {
			return -1;
		} else {
			return this.remainingRanges.get(0).compareTo(ranges.remainingRanges.get(0));
		}
	}

	public void intersect(IdRangeSet ranges) {
		if (ranges.isFull()) {
			return;
		} else if (ranges.isEmpty()) {
			this.full = false;
			this.remainingRanges.clear();
			return;
		} else if (this.isEmpty()) {
			return;
		} else if (this.isFull()) {
			this.full = ranges.full;
			this.remainingRanges.clear();
			this.remainingRanges.addAll(ranges.remainingRanges);
			return;
		}
		
		IdRangeSet remaining = IdRangeSet.empty();
		for (IdRange range : ranges) {
			int pos = Collections.binarySearch(remainingRanges, range.getCCW(), idComparator);

			if (pos == -1) {
				pos = remainingRanges.size() - 1;
			} else if (pos < 0) {
				pos = -(pos + 2);
			}
			final int start = pos;
			boolean first = true;
			for ( ; ; ) {
				IdRange intersect = remainingRanges.get(pos).intersect(range);
				if (intersect.isEmpty()) {
					if (! first) {
						break;
					}
				} else {
					remaining.add(intersect);
				}
				++pos;
				first = false;
				if (pos >= remainingRanges.size()) {
					pos = 0;
				}
				if (pos == start) {
					break;
				}
			}
		}
		this.remainingRanges = remaining.remainingRanges;
		this.full = remaining.full;
	}
}
