package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class TestId {
	String string50 = "5000000000000000000000000000000000000000";
	String stringFF = "FF000000000000000000000000000000000000FE";
	byte[] bytes50 = {80, 0, 0, 0, 0, 0, 0, 0, 0 , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	byte[] bytesFF = {-1, 0, 0, 0, 0, 0, 0, 0, 0 , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -2};
	
	Id id0 = Id.fromMSBBytes(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
	Id id1 = Id.fromMSBBytes(new byte[] {(byte) 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
	Id id2 = Id.fromMSBBytes(new byte[] {(byte) 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
	Id idE = Id.fromMSBBytes(new byte[] {(byte) 0xE0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
	Id idF = Id.fromMSBBytes(new byte[] {(byte) 0xF0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

	@Test
	public void testCreation() {
		Id id = Id.fromMSBBytes(bytes50);
		Id id2 = Id.fromMSBBytes(bytes50);
		assertEquals(id, id2);
		assertEquals(string50, id.toString());
	}
	
	@Test
	public void testCreationLarge() {
		Id id = Id.fromMSBBytes(bytesFF);
		assertEquals(stringFF, id.toString());
	}
	
	@Test
	public void testCompare() {
		Id id50 = Id.fromMSBBytes(bytes50);
		Id idFF = Id.fromMSBBytes(bytesFF);
		
		assertFalse(id50.equals(idFF));
		
		assertTrue(id50.compareTo(idFF) < 0);
		assertTrue(idFF.compareTo(id50) > 0);
	}
	
	@Test
	public void testFromString() {
		Id id50 = Id.fromString(string50);
		Id idFF = Id.fromString(stringFF);
		assertEquals(Id.fromMSBBytes(bytes50), id50);
		assertEquals(Id.fromMSBBytes(bytesFF), idFF);
	}
	
	@Test
	public void testHalfway() {
		assertEquals(id1, id0.findHalfway(id2));
		assertEquals(idF, idE.findHalfway(id0));
	}
}
