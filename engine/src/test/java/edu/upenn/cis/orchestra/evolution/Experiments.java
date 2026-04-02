package edu.upenn.cis.orchestra.evolution;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import junit.framework.Assert;
import junit.framework.TestCase;
import edu.upenn.cis.orchestra.workloadgenerator.Generator;
import edu.upenn.cis.orchestra.workloadgenerator.WorkloadGeneratorUtils;

public class Experiments extends TestCase {
	private static String ppart(boolean after, int i) {
		return (after ? "Q" : "P") + String.valueOf(i) + "_";
	}

	private static String printrel(boolean after, int i, int j, int k, List<String> a) {
		return ppart(after, i) + printsrel(j, k, a);
	}

	private static String printsrel(int i, int j, List<String> a) {
		StringBuilder sb = new StringBuilder(WorkloadGeneratorUtils.rpart(j) + "(");
		for (Iterator<String> itr = a.iterator(); itr.hasNext();) {
			sb.append(itr.next());
			if (itr.hasNext()) {
				sb.append(", ");
			}
		}
		sb.append(")");
		return sb.toString();
	}

	static List<String> selectAtoms(boolean after, int i, String key, List<String> x,
			String ch, List<List<List<String>>> schemas, List<Integer> peers) {
		Set<String> xSet = new HashSet<String>(x);
		List<String> atoms = new ArrayList<String>();
		if (null == peers.get(i)) {
			return atoms;
		}
		for (int k = 0; k < schemas.get(peers.get(i)).size(); k++) {
			List<String> rel = schemas.get(peers.get(i)).get(k);
			Set<String> relSet = new HashSet<String>(rel);
			relSet.retainAll(xSet);
			if (0 != relSet.size()) {
				List<String> atts = new ArrayList<String>(Arrays
						.asList(new String[] { key }));
				for (String att : rel) {
					if (xSet.contains(att)) {
						atts.add(att);
					} else {
						atts.add(ch);
					}
				}
				atoms.add(printrel(after, i, peers.get(i), k, atts));
			}
		}
		return atoms;
	}

	protected SignedRule getSignedRule(String head, List<String> body) {
		StringBuffer buf = new StringBuffer(head);
		buf.append(" :- ");
		for (int i = 0; i < body.size(); i++) {
			if (i > 0) {
				buf.append(", ");
			}
			buf.append(body.get(i));
		}
		return SignedRule.parse(buf.toString());
	}

	@SuppressWarnings("unchecked")
	protected Program getProgram(Generator generator, boolean after) {
		List<Integer> peers = generator.getPeers();
		List<List<List<String>>> schemas = generator.getLogicalSchemas();
		List<List<Object>> mappings = generator.getMappings();
		Vector<SignedRule> rules = new Vector<SignedRule>();
		for (int k = 0; k < mappings.size(); k++) {
			if (null == mappings.get(k)) { 
				continue;
			}
			int i = (Integer)mappings.get(k).get(0);
			int j = (Integer)mappings.get(k).get(1);
			// HACK to ensure acyclicity
			if (i > j) {
				int tmp = i;
				i = j;
				j = tmp;
			}

			List<String> x = (List<String>) mappings.get(k).get(2);
			List<String> source = selectAtoms(after, i, "KID", x, "_", schemas, peers);
			List<String> target = selectAtoms(after, j, "KID", x, "-", schemas, peers);
			for (String str : target) {
				rules.add(getSignedRule(str, source));
			}
		}
		Program prog = new Program(rules.toArray(new SignedRule[0]));
		return addSourceMappings(prog, after);
	}

	private Program addSourceMappings(Program program, boolean after) {
		// for those we identify as "sources", add a new mapping with
		// a special source relation
		Vector<Union> views = new Vector<Union>();
		Schema sources = program.getSources();
		HashMap<Integer,Integer> relations = sources.getArities();
		for (int k : relations.keySet()) {
			String key = Utils.TOKENIZER.getString(k);
			int arity = relations.get(k);
			String source = after ? "P" + key.substring(1) + "_src" : key + "_src";
			int s = Utils.TOKENIZER.getInteger(source);
			Atom head = new Atom(k, arity);
			Atom body = new Atom(s, arity);
			SignedRule rule = new SignedRule(new Rule(head, body), true);
			views.add(new Union(rule));
		}
		return program.addViews(views);
	}

	private void fillSources(Database db, Schema schema, int insertions) throws WrappedSQLException{
		for (int i = 0; ; i++) {
			String prefix = "P" + i + "_";
			String suffix = "_src";
			Schema matches = schema.match(prefix, suffix);
			HashMap<Integer,Integer> map = matches.getArities();
			if (map.size() == 0) {
				break;
			}
			int keyStart = i*insertions;
			for (int k : map.keySet()) {
				String key = Utils.TOKENIZER.getString(k);
				db.fillRandom(key, map.get(k), keyStart, insertions);
			}
		}
		db.commit();
	}
	
	int nextInRange(Random generator, int min, int max) {
		assert(min >= 0 && max >= min);
		int next = generator.nextInt();
		if (next < 0) {
			next = -next;
		}
		return (next % (max - min + 1)) + min;
	}

	private Union randomPathView(Random generator, String name) {
		int unions = nextInRange(generator, 1, UNIONS);
		SignedRule[] rules = new SignedRule[unions];
		int n = Utils.TOKENIZER.getInteger(name);
		for (int i = 0; i < unions; i++) {
			int joins = nextInRange(generator, 2, JOINS);
			Atom head = new Atom(n, 0, joins);
			Atom[] body = new Atom[joins];
			for (int j = 0; j < joins; j++) {
				int source = nextInRange(generator, 0, SOURCES.getSize()-1);
				int tok = SOURCES.getRelation(source);
				body[j] = new Atom(tok, j, j+1);
			}
			rules[i] = new SignedRule(new Rule(head, body), true);
		}
		return new Union(rules);
	}
	
	private static final int UNIONS = 3;
	private static final int JOINS = 5;
	//private static final int NUMTUPLES = 200000;
	private static final int NUMTUPLES = 200;
	private static final Schema SOURCES = Schema.parse("R(2), S(2)");
	//private static final int MAXVIEWS = 256;
	private static final int MAXVIEWS = 4;
	private static final int ITERATIONS = 5;

	private void testFlatPaths() throws Exception {
		TeePrintStream ts = new TeePrintStream(System.out, "output.txt");
		System.setOut(ts);
		Database db = new Database();
		fillSourceData(db);
		final int[] configurations = {
			Optimizer.Flags.create(Optimizer.Flags.GREEDY, Optimizer.Flags.LIMITED),
			Optimizer.Flags.create(Optimizer.Flags.GREEDY, Optimizer.Flags.LIMITED, Optimizer.Flags.MASHING),
			Optimizer.Flags.create(Optimizer.Flags.SEMIEXHAUSTIVE),
			Optimizer.Flags.create(Optimizer.Flags.SEMIEXHAUSTIVE, Optimizer.Flags.MASHING),
			Optimizer.Flags.create(Optimizer.Flags.SEMIEXHAUSTIVE, Optimizer.Flags.LIMITED, Optimizer.Flags.MASHING),
		};
		for (int flags : configurations) {
			doFlatPaths(db, flags);
		}
		cleanupSourceData(db);
		System.out.println("Done.");
		db.close();
		ts.close();
	}
	
	private void fillSourceData(Database db) throws WrappedSQLException {
		System.out.println("Creating source data (" + SOURCES.getSize() + " tables, " + NUMTUPLES + " tuples per table)...");
		db.createAll(SOURCES);
		for (int i = 0; i < SOURCES.getSize(); i++) {
			String s = Utils.TOKENIZER.getString(SOURCES.getRelation(i));
			db.fillChain(s, 0, NUMTUPLES);
		}
		db.commit();
	}
	
	private void cleanupSourceData(Database db) throws WrappedSQLException {
		System.out.println("Cleaning up...");
		db.dropAll(SOURCES);
		db.commit();
	}

	private void doFlatPaths(Database db, int flags) throws Exception {
		Random generator = new Random(0);
		Vector<Union> before = new Vector<Union>();
		Vector<Union> after = new Vector<Union>();
		System.out.println("Flat paths experiment, flags " + Optimizer.Flags.toString(flags) + ", # views = " + MAXVIEWS + "...");
		for (int i = 0; i < ITERATIONS; i++) {
			System.out.println("Iteration # " + i+1 + " of " + ITERATIONS + "...");
			for (int j = 0; j < MAXVIEWS; j++) {
				after.add(randomPathView(generator, "Q" + j));
			}
			Program p1 = new Program(before);
			Program p2 = new Program(after);
			System.out.println(p1.toString());
			System.out.println(p2.toString());
			System.out.println("Setting up...");
			db.createAll(p1.getDerived());
			db.createAll(p2.getDerived());
			Optimizer opt = new Optimizer(db);
			db.createViews(p1);
			db.createViews(p2);
			System.out.println("Optimizing and updating...");
			Statistic[] s3 = opt.optimizedUpdate(p1, p2, flags);
			outputStatsForExcel(flags, s3);
			db.dropAll(p1.getDerived());
			db.dropAll(p2.getDerived());
			db.commit();
			System.out.println("Done.");
			System.out.println();
		}
	}
	
	private void outputStatsForExcel(int flags, Statistic[] stats) {
		String title = "UNIONS=" + UNIONS + ", JOINS=" + JOINS + ", NUMTUPLES=" + NUMTUPLES + ", SOURCES=" + SOURCES + ", VIEWS=" + stats.length + ", FLAGS=" + Optimizer.Flags.toString(flags);
		System.out.println(title);		
		for (int i = 0; i < stats.length; i++) {
			System.out.println("Original Plan: " + stats[i].origPlan);
			System.out.println("Optimized Plan: " + stats[i].optPlan);
		}
		System.out.println();
		System.out.println(title);
		System.out.println("name\tprocessorTime (ms)\toptimizerTime (ms)\torigCost (timerons)\toptCost (timerons)\torigExecuteTime (ms)\toptExecuteTime (ms)");
		for (Statistic stat : stats) {
			System.out.println(Utils.TOKENIZER.getString(stat.origPlan.getName()) + "\t" +
				stat.processorTime + "\t" +
				stat.optimizerTime + "\t" +
				Math.round(stat.origCost) + "\t" +
				Math.round(stat.optCost) + "\t"  + 
				stat.origExecuteTime + "\t" +
				stat.optExecuteTime);
		}
	}

	private void runTest(String filename) throws Exception {
		InputStream input = OptimizerTest.class.getResourceAsStream(filename);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		Database db = new Database();
		int count = 1;
		while (Utils.skipBlanks(reader)) {
			long time = System.currentTimeMillis();
			HashMap<String, String> blocks = Utils.readBlocks(reader); 
			String test = blocks.get("TEST");
			int flags = Optimizer.Flags.parse(blocks.get("FLAGS"));
			int insertions = Integer.parseInt(blocks.get("INSERTIONS"));
			Assert.assertTrue(flags != 0);
			String cmdline = blocks.get("CMDLINE");
			String[] args = cmdline.split("\\s+");
			Map<String, Object> params = Generator.parseCommandLine(args);
			params.put("integers", new Integer(1));
			params.put("maxcycles", new Integer(0));
			System.out.println("Setting up experiment " + count++ + " \"" + test + "\" (" + Optimizer.Flags.toString(flags) + ")...");
			System.out.println(cmdline);
			Generator generator = new Generator(params, true);
			generator.generate();
			Program before = getProgram(generator, false);	
			System.out.println("Initial views:");
			System.out.println(before);
			Generator curGenerator = generator;
			Generator prevGenerator = null;
			for (int i = 0; i < (Integer) params.get("iterations"); i++) {
				prevGenerator = curGenerator;
				curGenerator = new Generator(params, true, prevGenerator
						.getPeers().size(), prevGenerator.getPeers().size()
						+ (Integer) params.get("addPeers"), prevGenerator);

				curGenerator.generate(i + 1);
				curGenerator.deletePeers((Integer) params.get("deletePeers"));
				curGenerator.addAndDeleteBypasses((Integer) params
						.get("addBypasses"), (Integer) params
						.get("deleteBypasses"));

			}
			Program after = getProgram(curGenerator, true);
			System.out.println();
			System.out.println("Changed views:");
			System.out.println(after);
			db.createAll(before.getSchema());
			db.createAll(after.getSchema());
			System.out.println("Filling sources...");
			fillSources(db, before.getSchema(), insertions);
			System.out.println("Creating initial views...");
			Statistic[] stats = db.createViews(before);
//			outputStatsForExcel("INITIAL", stats);
			System.out.println("Creating updated views...");
			stats = db.createViews(after);
//			outputStatsForExcel("UPDATED", stats);
			System.out.println("Optimizing and updating...");
			before = before.unfoldAll();
			after = after.unfoldAll();
			Optimizer opt = new Optimizer(db);
			stats = opt.optimizedUpdate(before, after, flags);
			outputStatsForExcel(flags, stats);
			System.out.println();
			System.out.println("Done (" + ((System.currentTimeMillis() - time)/1000) + " sec total).");
			System.out.println();
			System.out.println();
			db.dropAll(before.getSchema());
			db.dropAll(after.getSchema());
			db.commit();
		}
		db.close();
	}
	
	public void doExperiments() throws Exception {
		runTest("Experiments.txt");
//		runTest("RandomEdits.txt");
	}
}
