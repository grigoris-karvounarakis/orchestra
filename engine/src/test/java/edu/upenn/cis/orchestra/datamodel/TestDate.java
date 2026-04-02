package edu.upenn.cis.orchestra.datamodel;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;


public class TestDate {
	
	Date d;
	
	@Before
	public void setUp() {
		d = new Date(1982, 2, 17);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testBadYear() {
		new Date(5,1,1);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testBadMonth() {
		new Date(2000,13,1);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testBadMonth2() {
		new Date(2000,0,1);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testBadDay() {
		new Date(2000,2,30);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testBadDay2() {
		new Date(2000,1,0);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testLeapYear1() {
		new Date(1900,2,29);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testLeapYear2() {
		new Date(2007,2,29);
	}

	public void testLeapYear4() {
		new Date(2000,2,29);
	}

	public void testLeapYear3() {
		new Date(2008,2,29);
	}

	@Test
	public void testCreation() {
		assertEquals("Incorrect year", (short) 1982, d.getYear());
		assertEquals("Incorrect month", (byte) 2, d.getMonth());
		assertEquals("Incorrect day", (byte) 17, d.getDay());
	}
	
	@Test
	public void testMonth() {
		assertEquals("Incorrect month name", "February", d.getMonthName());
	}
	
	@Test
	public void testStringification() {
		assertEquals("Incorrect stringification", "1982-02-17", d.toString());
	}
	
	@Test
	public void testDestringification() {
		assertEquals("Incorrect destringification", d, Date.fromString("1982-02-17"));
		assertEquals("Incorrect destringification", new Date(1998,11,1), Date.fromString("1998-11-01"));
	}
	
	@Test
	public void testSerialization() {
		byte[] data = d.getBytes();
		Date deser = Date.getValFromBytes(data);
		assertEquals("Incorrect serialization or deserialization", d, deser);
	}
	
	@Test
	public void testTomorrow() {
		Date dd = d.tomorrow();
		assertEquals("Wrong tomorrow", new Date(1982,2,18), dd);
		
		dd = new Date(1980,2,29).tomorrow();
		assertEquals("Wrong tomorrow", new Date(1980,3,1), dd);
		
		dd = new Date(1980,2,28).tomorrow();
		assertEquals("Wrong tomorrow", new Date(1980,2,29), dd);

		dd = new Date(1982,12,31).tomorrow();
		assertEquals("Wrong tomorrow", new Date(1983,1,1), dd);
	}
	
	@Test
	public void testYesterday() {
		Date dd = d.yesterday();
		assertEquals("Wrong yesterday", new Date(1982,2,16), dd);
		
		dd = new Date(1980,3,1).yesterday();
		assertEquals("Wrong yesterday", new Date(1980,2,29), dd);
		
		dd = new Date(1980,2,29).yesterday();
		assertEquals("Wrong yesterday", new Date(1980,2,28), dd);

		dd = new Date(1983,1,1).yesterday();
		assertEquals("Wrong yesterday", new Date(1982,12,31), dd);
		
	}
	
	@Test
	public void testDayOfYear() {
		assertEquals("Wrong day of year", 48, d.getDayOfYear());
		
		Date dd = new Date(1983,1,1);
		assertEquals("Wrong day of year", 1, dd.getDayOfYear());
	}
	
	@Test
	public void testToSQL() {
		java.sql.Date sql = d.getSQL();
		
		Calendar c = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		c.setTime(sql);
		assertEquals("Incorrect year", 1982, c.get(Calendar.YEAR));
		assertEquals("Incorrect month", Calendar.FEBRUARY, c.get(Calendar.MONTH));
		assertEquals("Incorrect day", 17, c.get(Calendar.DAY_OF_MONTH));
	}
	
	@Test
	public void testFromSQL() {
		java.sql.Date sql = java.sql.Date.valueOf("1982-02-17");
		Date fromSql = Date.fromSQL(sql);
		assertEquals("Incorrect date from SQL date", d, fromSql);
	}
	
	@Test
	public void testCompare1() {
		Date dd = new Date(2007, 1, 1);
		assertTrue("Should be less than", d.compareTo(dd) < 0);
		assertTrue("Should be greater than", dd.compareTo(d) > 0);
	}

	@Test
	public void testCompare2() {
		Date dd = new Date(1982, 3, 1);
		assertTrue("Should be less than", d.compareTo(dd) < 0);
		assertTrue("Should be greater than", dd.compareTo(d) > 0);
	}

	@Test
	public void testCompare3() {
		Date dd = new Date(1982, 2, 18);
		assertTrue("Should be less than", d.compareTo(dd) < 0);
		assertTrue("Should be greater than", dd.compareTo(d) > 0);
	}

	@Test
	public void testCompare4() {
		Date dd = new Date(1982, 2, 17);
		assertTrue("Should be equal", d.compareTo(dd) == 0);
		assertTrue("Should be equal", d.equals(dd));
	}
}
