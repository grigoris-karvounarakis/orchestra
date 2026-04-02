package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class TestIdRangeSet {

	Id one, two, three, four, five, six, seven;

	@Before
	public void setUp() {
		byte[] data = new byte[Id.idLengthBytes];
		data[0] = 1;
		one = Id.fromMSBBytes(data);
		data[0] = 2;
		two = Id.fromMSBBytes(data);
		data[0] = 3;
		three = Id.fromMSBBytes(data);
		data[0] = 4;
		four = Id.fromMSBBytes(data);
		data[0] = 5;
		five = Id.fromMSBBytes(data);
		data[0] = 6;
		six = Id.fromMSBBytes(data);
		data[0] = 7;
		seven = Id.fromMSBBytes(data);
	}

	@Test
	public void testRemoveFromFullRange() {
		IdRangeSet set = IdRangeSet.full();
		set.remove(new IdRange(four,four));
		assertTrue(set.isEmpty());

		set = IdRangeSet.full();
		set.remove(IdRange.empty());
		assertFalse(set.isEmpty());

		set = IdRangeSet.full();
		set.remove(new IdRange(one,four));
		assertFalse(set.isEmpty());
	}

	@Test
	public void testRemoveFullRange() {
		IdRangeSet set = IdRangeSet.full();
		set.remove(new IdRange(four,four));
		assertTrue(set.isEmpty());

		set = new IdRangeSet(new IdRange(one,three));
		set.remove(new IdRange(four,four));
		assertTrue(set.isEmpty());

	}

	@Test
	public void testRemovePartialRanges() {
		IdRangeSet set = IdRangeSet.full();
		set.remove(new IdRange(one,three));
		set.remove(new IdRange(three,one));
		assertTrue(set.isEmpty());
	}

	@Test
	public void testRemoveOverlappingRanges() {
		IdRangeSet set = IdRangeSet.full();
		set.remove(new IdRange(one,three));
		assertFalse(set.isEmpty());
		set.remove(new IdRange(one,two));
		assertFalse(set.isEmpty());
		set.remove(new IdRange(two, four));
		assertFalse(set.isEmpty());
		set.remove(new IdRange(three,one));
		assertTrue(set.isEmpty());
	}

	@Test
	public void testRemoveOverlappingRanges2() {		
		IdRangeSet set = IdRangeSet.full();
		set.remove(new IdRange(one,two));
		assertFalse(set.isEmpty());
		set.remove(new IdRange(three,four));
		assertFalse(set.isEmpty());
		set.remove(new IdRange(two,one));
		assertTrue(set.isEmpty());
	}

	@Test
	public void testAddCreateFull() {
		IdRangeSet set = new IdRangeSet(new IdRange(one,two));
		set.add(new IdRange(two,one));
		assertTrue(set.isFull());
		assertTrue(set.contains(two));
		assertTrue(set.contains(one));
		assertTrue(set.contains(Id.MAX));
	}

	@Test
	public void testRemoveAndAdd() {
		IdRangeSet set = new IdRangeSet(new IdRange(four,three));
		set.remove(new IdRange(one,two));
		set.add(new IdRange(one,two));
		assertEquals(new IdRangeSet(new IdRange(four,three)), set);
	}

	@Test
	public void testWrapping() {
		IdRangeSet set = IdRangeSet.empty();
		set.add(new IdRange(three,four));
		set.add(new IdRange(five, two));
		set.remove(new IdRange(one,two));

		IdRangeSet expected = IdRangeSet.empty();
		expected.add(new IdRange(three,four));
		expected.add(new IdRange(five,one));
		assertEquals(expected, set);
		
		assertTrue(set.contains(five));
		assertTrue(set.contains(Id.ZERO));
		assertTrue(set.contains(three));
		assertFalse(set.contains(four));
		assertFalse(set.contains(one));
	}

	@Test
	public void testRemainingFrac() {
		assertEquals(0.0, IdRangeSet.empty().remainingFrac());
		assertEquals(1.0, IdRangeSet.full().remainingFrac());

		Id oneQuarter = new Id(Id.MAX_BIGINT.divide(new BigInteger("4")));
		Id oneHalf = new Id(Id.MAX_BIGINT.divide(new BigInteger("2")));
		Id threeQuarters = new Id(Id.MAX_BIGINT.multiply(new BigInteger("3")).divide(new BigInteger("4")));

		IdRange secondQuarter = new IdRange(oneQuarter, oneHalf);
		IdRangeSet s = new IdRangeSet(secondQuarter);
		assertTrue(Math.abs(s.remainingFrac() - 0.25) < 0.0001);
		s = IdRangeSet.empty();
		s.add(secondQuarter);
		s.add(new IdRange(threeQuarters, Id.ZERO));
		assertTrue(Math.abs(s.remainingFrac() - 0.5) < 0.0001);		
	}

	@Test
	public void testComplexExample() {
		IdRange id1 = new IdRange(new Id("2AAA000000000000000000000000000000000000"), new Id("2C4E957B5E6BE1C146143D2C18D2C8A53F437FF6"));
		IdRange id2 = new IdRange(new Id("3296156E3AD38027F9471635BFFD81AAA015DC93"), new Id("34B2702FFB528B14C71DBB25A6B67BC23E77C2E3"));
		IdRange id3 = new IdRange(new Id("36D3239136250B6FAB5EE111E56364412650C5E5"), new Id("38F44593640FAA4B1DA1B674F1AB762B59A10C1E"));
		IdRange id4 = new IdRange(new Id("4195F12480C158EFEBDDF7048E4589227148441D"), new Id("45C87D30B1861DD5E6D08EE7EAD3BFD5F619FE7A"));
		IdRange id5 = new IdRange(new Id("4E030A37F303E115137E0AECA9372F42D5EF17D0"), new Id("5554800000000000000000000000000000000000"));		

		IdRangeSet remaining = IdRangeSet.empty();
		for (IdRange idr : Arrays.asList(id1, id2, id3, id4, id5)) {
			remaining.add(idr);
		}

		IdRangeSet expected = IdRangeSet.empty();
		for (IdRange idr : Arrays.asList(id2, id3, id4, id5)) {
			expected.add(idr);
		}
		
		IdRange toRemove = new IdRange(new Id("2A1C546A12662BA5F3E6A7E554F8521FF5416A82"), new Id("2C4E957B5E6BE1C146143D2C18D2C8A53F437FF6"));
		
		remaining.remove(toRemove);
		assertEquals(expected, remaining);
	}
	
	@Test
	public void testContains() {
		IdRangeSet r = IdRangeSet.empty();
		r.add(new IdRange(one, three));
		r.add(new IdRange(five, Id.ZERO));
		assertTrue(r.contains(one));
		assertTrue(r.contains(two));
	}
	
	@Test
	public void testIntersect() {
		IdRangeSet r = IdRangeSet.empty();
		r.add(new IdRange(Id.ZERO, one));
		r.add(new IdRange(two, three));
		r.add(new IdRange(four, five));
		
		IdRangeSet rr = r.clone();
		rr.intersect(new IdRange(two, four));
		IdRangeSet expected = IdRangeSet.empty();
		expected.add(new IdRange(two, three));
		assertEquals(expected, rr);
		
		rr = r.clone();
		rr.intersect(new IdRange(one, four));
		expected = IdRangeSet.empty();
		expected.add(new IdRange(two,three));
		assertEquals(expected, rr);
		
		rr = r.clone();
		rr.intersect(new IdRange(two, three));
		expected = IdRangeSet.empty();
		expected.add(new IdRange(two,three));
		assertEquals(expected, rr);

		rr = r.clone();
		rr.intersect(IdRange.empty());
		expected = IdRangeSet.empty();
		assertEquals(expected, rr);

		rr = r.clone();
		rr.intersect(IdRange.full());
		assertEquals(r, rr);
	}
	
	@Test
	public void testContainsRange() {
		assertTrue(IdRangeSet.empty().contains(IdRange.empty()));
		assertTrue(IdRangeSet.full().contains(IdRange.full()));
		assertTrue(IdRangeSet.full().contains(IdRange.empty()));
		
		IdRange onetwo = new IdRange(one, two);
		IdRange onethree = new IdRange(one, three);
		IdRange twofour = new IdRange(two, four);
		IdRangeSet ranges = IdRangeSet.empty();
		ranges.add(onethree);
		assertTrue(ranges.contains(onetwo));
		assertTrue(ranges.contains(onethree));
		assertFalse(ranges.contains(twofour));
		
		IdRange fourtwo = new IdRange(four, two);
		ranges = IdRangeSet.empty();
		ranges.add(fourtwo);
		assertTrue(ranges.contains(fourtwo));
		assertTrue(ranges.contains(onetwo));
		assertFalse(ranges.contains(onethree));
	}
	
	@Test
	public void testIntersects() {
		assertTrue(IdRangeSet.full().intersects(IdRange.full()));
		assertFalse(IdRangeSet.full().intersects(IdRange.empty()));
		assertFalse(IdRangeSet.empty().intersects(IdRange.full()));
		assertFalse(IdRangeSet.empty().intersects(IdRange.empty()));
		
		IdRange onethree = new IdRange(one, three);
		IdRange twofour = new IdRange(two, four);
		IdRangeSet ranges = IdRangeSet.empty();
		ranges.add(onethree);
		assertFalse(ranges.intersects(IdRange.empty()));
		assertTrue(ranges.intersects(IdRange.full()));
		assertTrue(ranges.intersects(onethree));
		assertTrue(ranges.intersects(twofour));
		IdRange fourtwo = new IdRange(four, two);
		assertTrue(ranges.intersects(fourtwo));
		assertFalse(ranges.intersects(new IdRange(three, four)));
		assertFalse(ranges.intersects(new IdRange(three, one)));
		
		ranges.add(new IdRange(four, Id.ZERO));
		assertTrue(ranges.intersects(new IdRange(three, one)));
		assertTrue(ranges.intersects(new IdRange(three, Id.ZERO)));
		assertTrue(ranges.intersects(new IdRange(four, one)));
		assertFalse(ranges.intersects(new IdRange(three, four)));
	}
	
	@Test
	public void testIntersectSet() {
		IdRangeSet ranges = IdRangeSet.empty();
		ranges.add(new IdRange(one, three));
		ranges.add(new IdRange(five, Id.ZERO));
		
		IdRangeSet filter = IdRangeSet.empty();
		filter.add(new IdRange(two, four));
		filter.add(new IdRange(six, seven));
		
		IdRangeSet result = IdRangeSet.empty();
		result.add(new IdRange(two, three));
		result.add(new IdRange(six, seven));
		ranges.intersect(filter);
		assertEquals(ranges, result);
	}
}
