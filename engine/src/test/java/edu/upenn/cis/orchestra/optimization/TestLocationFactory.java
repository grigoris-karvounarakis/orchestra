package edu.upenn.cis.orchestra.optimization;


import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.experlog.zql.ParseException;
import com.experlog.zql.ZQuery;
import com.experlog.zql.ZqlParser;

import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.optimization.Query.SyntaxError;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;

public class TestLocationFactory {
	HashMapRelationTypes<Location,QpSchema> rt;
	PhysicalPropertiesFactory<Location> ppf;
	static ZqlParser parser = new ZqlParser();

	@Before
	public void setUp() throws Exception {
		rt = new HashMapRelationTypes<Location,QpSchema>();
		
		QpSchema r = new QpSchema("R", 1);
		r.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.addCol("b", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.setPrimaryKey(Arrays.asList("a"));
		r.setHashCols(new int[] {0});
		r.markFinished();
		
		List<Histogram<?>> rHists = new ArrayList<Histogram<?>>(2);
		rHists.add(Histogram.createIntegerHistogram(Arrays.asList(1,6,11), new double[] {5.0, 2.0}, new double[] {3.0,1.0}));
		rHists.add(Histogram.createIntegerHistogram(Arrays.asList(1,6,11), new double[] {5.0, 2.0}, new double[] {3.0,1.0}));
		rt.addRelation(r, r.getOptimizerLocation(), rHists);

		QpSchema s = new QpSchema("S", 2);
		s.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		s.addCol("c", new StringType(true,true,true,20));
		s.setPrimaryKey(Arrays.asList("a","c"));
		s.addForeignKey("fk1", Arrays.asList("a"), r, Arrays.asList("a"));
		s.setHashCols(new int[] {0});
		s.markFinished();

		List<Histogram<?>> sHists = new ArrayList<Histogram<?>>(2);
		sHists.add(Histogram.createIntegerHistogram(Arrays.asList(1,6,11), new double[] {5.0, 3.0}, new double[] {2.0,1.0}));
		sHists.add(Histogram.createStringHistogram(3,
				Arrays.asList(
				Histogram.convertForHistogram(3, "Albert"),
				Histogram.getSuccessorForHistogram(3, "Zebediah")),
				new double[] {8.0}, new double[] { 6.0 }));
		rt.addRelation(s, s.getOptimizerLocation(), sHists);
		
		ppf = new Location.Factory(rt);
	}
	
	@Test
	public void testScanLoc() throws Exception {
		Query q = getQuery("SELECT a FROM R");
		
		Set<Location> result = new HashSet<Location>();
		Iterator<Location> locs = ppf.enumerateAllProperties(q.exp, rt);
		while (locs.hasNext()) {
			result.add(locs.next());
		}
		
		Set<Location> expected = new HashSet<Location>(Arrays.asList(rt.getRelationProps("R"), Location.CENTRALIZED));
		assertEquals("Incorrect locations for scan", expected, result);
	}
	
	@Test
	public void testJoinLoc() throws Exception {
		Query q = getQuery("SELECT R.a FROM R, S WHERE R.b = S.a");
		Set<Location> result = new HashSet<Location>();
		Iterator<Location> locs = ppf.enumerateAllProperties(q.exp, rt);
		while (locs.hasNext()) {
			result.add(locs.next());
		}
		
		Set<Location> expected = new HashSet<Location>(Arrays.asList(rt.getRelationProps("R"), rt.getRelationProps("S"), Location.CENTRALIZED));
		EquivClass ec = new EquivClass();
		ec.add(new AtomVariable("R", 1, 1, rt));
		ec.add(new AtomVariable("S", 1, 0, rt));
		ec.setFinished();
		expected.add(new Location(Collections.singleton(ec)));
		assertEquals("Incorrect locations for join", expected, result);
	}

	private Query getQuery(String SQL) throws ParseException, TypeError, SyntaxError {
		if (! SQL.endsWith(";")) {
			SQL = SQL + ";";
		}
		parser.initParser(new StringReader(SQL));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, rt);
		return q;
	}

}
