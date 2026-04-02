package edu.upenn.cis.orchestra.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Ranges {
	private Set<Integer> data = new HashSet<Integer>();
	
	public void add(int val) {
		data.add(val);
	}
	
	static class Range {
		final int bot;
		final int top;
		
		Range(int bot, int top) {
			this.bot = bot;
			this.top = top;
		}
		
		public String toString() {
			if (bot == top) {
				return Integer.toString(bot);
			} else {
				return bot + "-" + top;
			}
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			
			Range r = (Range) o;
			return bot == r.bot && top == r.top;
		}
	}

	public List<Range> getRanges() {
		List<Integer> sorted = new ArrayList<Integer>(data);
		Collections.sort(sorted);

		List<Range> ranges = new ArrayList<Range>();
		Integer start = null;
		Integer last = null;
		for (Integer val : sorted) {
			if (start == null) {
				start = val;
				last = val;
			} else {
				if (val == last + 1) {
					last = val;
				} else {
					ranges.add(new Range(start, last));
					start = val;
					last = val;
				}
			}
		}
		
		if (start != null) {
			ranges.add(new Range(start,last));
		}
		
		return ranges;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		Iterator<Range> ranges = getRanges().iterator();
		while (ranges.hasNext()) {
			sb.append(ranges.next());
			if (ranges.hasNext()) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
}
