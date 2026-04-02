package edu.upenn.cis.orchestra.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TestRanges {
	@Test
	public void testConsecutiveRanges() {
		Ranges r = new Ranges();

		for (int i = 0; i < 100; i++) {
			r.add(i);
		}

		List<Ranges.Range> rs = r.getRanges();

		assertEquals("Incorrect range", Collections.singletonList(new Ranges.Range(0,99)), rs);
	}

	@Test
	public void testNonconsecutiveRanges() {
		Ranges r = new Ranges();
		for (int i = 0; i < 100; i++) {
			r.add(i);
		}

		for (int i = 200; i < 300; i++) {
			r.add(i);
		}
		
		List<Ranges.Range> expected = new ArrayList<Ranges.Range>();
		expected.add(new Ranges.Range(0,99));
		expected.add(new Ranges.Range(200,299));

		assertEquals("Incorrect ranges", expected, r.getRanges());
}
}
