package edu.upenn.cis.orchestra.evolution;

import junit.framework.Assert;
import junit.framework.TestCase;


public class RuleTest extends TestCase {
	protected static String[] s_isomorphism = {
//		"R(x,y) :- S(x,z), T(z,y) ; z~'2'", "R(x,y) :- S(x,'2'), T('2',y)",
//		"R(x,y) :- S(x,'2'), T('2',y)", "R(x,y) :- S(x,z), T(z,y) ; z~'2'", 
		"T(x,y) :- R(x,y)", "T(x,y) :- R(x,y)",
		"T(x,y) :- R(x,y)", "Q(u,v) :- R(u,v)",
		"T(x,y) :- R(x,z),T(z,y), T(y,y)", "Q1(u,v) :- R(u,w), T(v,v), T(w,v)",
		"C(x,'2') :- R(x,y)", "C(u,'2') :- R(u,v)",
		"C(u,v) :- R(u,'4'),R('4',v)",   "C(x,y) :- R('4',y), R(x,'4')",
		"C('1','2') :- R('1','2'),R('2','1')", "C('1','2') :- R('2','1'), R('1','2')",
	};
	protected static String[] s_nonIsomorphism = {
		"Q(u,v) :- R(u,v)", "T(x,y) :- R(x,z),T(z,y), T(y,y)",
		"Q1(u,w) :- R(u,w), T(v,v), T(w,v), T(v,w)", "Q2(x,x) :- R(x,y), T(y,z), T(z,y), T(z,z)",
		"Q2(x,x) :- R(x,y), T(y,z), T(z,y), T(z,z)", "Q3() :- R(x,y), T(y,z), T(z,y), T(y,y)",
		"V1(x,y) :- R(x,y)", "V2(x) :- R(x,y)",
		"V1(x) :- R(x,y)", "V2(y) :- R(x,y)",
		"V1(x,y) :- R(x,y)", "V1(x,x) :- R(x,x)",
		"V(x,y) :- R(x,y), R(x,y), R(y,y)", "V(x,y) :- R(x,y), R(y,y), R(y,y)",
		"C(x,'2') :- R(x,y)", "C(u,'3') :- R(u,v)",
		"C(x,'2') :- R(x,y)", "C(u,'3') :- R(u,v)",
		"C(u,v) :- R(u,'4'),R('4',v)",   "C(x,y) :- R('4',x), R(y,'4')",
		"C(u,v) :- R(u,'4'),R('4',v)",   "C(x,y) :- R('4',x), R(y,'4')",
		"C('1','2') :- R('1','2'),R('1','2')", "C('1','2') :- R('1','2')"
	};
	
	protected static String[] s_homomorphism = {
		"R(x,y) :- R(x,y)", 				"R(u,v) :- R(u,v), R(v,v)",
		"R(x,y) :- R(x,y), R(y,y), R(y,y)", "R(u,v) :- R(u,v), R(v,v), R(u,u)",
		"R(x,y) :- R(x,y)", 				"R(x,y) :- R(x,y), S(y,z)",
		"R(x,y) :- R(x,y)", 				"R(x,x) :- R(x,x), R(x,x)",
		"R() :- R(x,y)",					"R() :- R(u,u), R(u,w)",
		"R() :- R('2',x)",					"R() :- R('2',u), R(v,w)",
		"R('2',y) :- R('2',y)", 			"R('2',y) :- R('2',y), S(y,'3')",
		"R(x,y) :- R(x,y)", 				"R('2',y) :- R('2',y), S(y,'3')",
	};
	
	protected static String[] s_nonHomomorphism = {
		"R(u,v) :- R(u,v), R(v,v)", 		"R(x,y) :- R(x,y)", 
		"R(u,v) :- R(u,v), R(v,v), R(u,u)", "R(x,y) :- R(x,y), R(y,y), R(y,y)",
		"R(x,y) :- R(x,y), S(y,z)", 		"R(x,y) :- R(x,y)",
		"R(x,x) :- R(x,x), R(x,x)", 		"R(x,y) :- R(x,y)",
		"R('2',y) :- R('2',y)", 			"R(x,y) :- R(x,y), S(y,z)",
	};
	
	protected static String[] s_unfoldings = {
		"R(x,y) :- V(x,z), S(z,y)", "V(u,v) :- U(u,v), W(v,v)", "R(x,y) :- S(z,y), U(x,z), W(z,z)",
		"R(x,y) :- V(x,y), S(x,y)", "V(x,x) :- T(x,y)", "R(x,x) :- T(x,z), S(x,x)",
		"R(x,y) :- V(x,y), S(x,y), V(u,v)", "V(x,x) :- T(x,y)", "R(x,x) :- T(x,z), S(x,x), V(u,v)",
		"R(x,y) :- S(x,y)", "V(x,x) :- T(x,y)", "R(x,y) :- S(x,y)",
		"R(x,y) :- V('2',x),S(x,y)", "V(u,v) :- S(u,v)", "R(x,y) :- S('2',x),S(x,y)",
		"R(x,y) :- V(x,y)", "V(u,'2') :- S(u,v)", "R(x,'2') :- S(x,z)",
		"R(x,y) :- V(x,x,y)", "V('2','3',u) :- S(u)", "R(x,y) :- false",
	};
	
	protected static String[] s_embeddings = {
		"V(x,y) :- T(x,y)", "R(u,v) :- T(u,v)",
		"V(x,y) :- T(x,y)", "R(u,u) :- T(u,u)",
	};

	protected static String[] s_nonEmbeddings = {
		"V(x,x) :- T(x,y)", "R(u,v) :- T(u,v)",
	};
	
	protected static String[] s_substitutions = {
		"R(x,'2') :- T(x,y)", "V(x,'2') :- T(x,y)",
		"V(x,y) :- T(x,y)", "R(u,u) :- T(u,'2')",
		"V(x,'2') :- T(x,y)", "R(u,u) :- T(u,v)",
		"V(0,1) :- R(0,2), R(2,3), R(3,4), R(4,5), R(5,1)", "Q(0,1) :- R(0,2), R(2,3), R(3,4), R(4,5), R(5,1)",
		"V(x,y) :- T(x,y)", "R(u,v) :- T(u,v)",
		"V(x,y) :- T(x,y)", "R(u,u) :- T(u,u)",
	};
	
	protected static String[] s_nonSubstitutions = {
		"V(x,'2') :- T(x,y)", "R(u,v) :- T(u,v)",
		"V(x,y) :- T(x,y,z)", "R(u,v) :- S(u,v,w), T(u,v,w)",
	};
	
	protected static String[] s_foldings = {
		"R(x) :- T(x,'2')", "V(x) :- T(x,'2')", "R(x) :- V(x)",
		"R(x,x) :- S(x,y)", "V(x,x) :- S(x,y)", "R(x,x) :- V(x,x)",
		"R(x,y) :- S(x,z), T(z,y)", "V(u,v) :- S(u,w), T(w,v)", "R(x,y) :- V(x,y)",
		"R(x,y) :- S(x,z), T(z,y)", "V(y,x) :- S(y,z), T(z,x)", "R(x,y) :- V(x,y)",
		"R(x,y) :- S(x,z), T(z,y)", "V(y,x) :- S(y,z), T(z,x)", "R(x,y) :- V(x,y)",		
	};

	protected static String[] s_nonFoldings = {
		"R(x,y) :- S(x,z), T(z,y), U(z,y)", "V(u,v) :- S(u,w), T(w,v)"
	};

	public void testNextMorphism() {
		String s1 = "R(x,y) :- S(x,a), S(a,b), S(b,c), S(c,d), S(d,e), S(e,f), S(f,y)";
		String s2 = "V(x,y) :- S(x,z), S(z,y)";
		Rule query = Rule.parse(s1);
		Rule view = Rule.parse(s2);
		RuleMorphism m = view.findSubstitution(query);
		int count = 0;
		boolean success = m != null;
		while (success) {
			count++;
			success = m.next();
		}
		Assert.assertEquals(count, 6);
	}
	
	public void testViewFolding() {
		for (int i = 0; i < s_foldings.length-2; i+=3) {
			Rule r1 = Rule.parse(s_foldings[i]);
			Rule r2 = Rule.parse(s_foldings[i+1]);
			Rule r3 = Rule.parse(s_foldings[i+2]);
			System.out.println("Folding " + r2 + " into " + r1 + "... ");
			Rule result = r1.foldView(r2);
			System.out.println(result);
			RuleMorphism m = result.findIsomorphism(r3); 
			Assert.assertNotNull("" + i, m);
		}
		for (int i = 0; i < s_nonFoldings.length-1; i+=2) {
			Rule r1 = Rule.parse(s_nonFoldings[i]);
			Rule r2 = Rule.parse(s_nonFoldings[i+1]);
			System.out.println("Folding " + r2 + " into " + r1 + "... ");
			Rule result = r1.foldView(r2);
			System.out.println(result);
			Assert.assertEquals("" + i, r1, result);
		}
		for (int i = 0; i < s_unfoldings.length-2; i+=3) {
			Rule r3 = Rule.parse(s_unfoldings[i]);
			Rule r2 = Rule.parse(s_unfoldings[i+1]);
			Rule r1 = Rule.parse(s_unfoldings[i+2]);
			System.out.println("Folding " + r2 + " into " + r1 + "... ");
			Rule result = r1.foldView(r2);
			System.out.println(result);
			Program p = new Program(r1,r2,r3);
			boolean same = p.testEquivalent(result, r3);
			if (!same) {
				Assert.assertTrue("" + i, same);
			}
		}
	}
	
	public void testSQL() {
		for (int i = 0; i < s_isomorphism.length; i++) {
			Rule r1 = Rule.parse(s_isomorphism[i]);
			System.out.println(r1);
			System.out.println(r1.toSQL());
		}
		Rule r = Rule.parse("R(x,y) :- S(x,z), T(z,y)");
		System.out.println(r);
		System.out.println(r.toSQL());
		r = Rule.parse("R(x,y) :- S(x,z), T(z,y) ; z~'2'");
		System.out.println(r);
		System.out.println(r.toSQL());
	}

	protected void findSubstitution(String s1, String s2, boolean expected) {
		Rule r1 = Rule.parse(s1);
		Rule r2 = Rule.parse(s2);
		System.out.println("Testing " + r1 + " and " + r2 + " for " + (expected ? "substitution" : "non-substitution"));
		RuleMorphism m = r1.findSubstitution(r2);
		System.out.println(m);
		Assert.assertEquals(expected, m != null);
		if (expected) {
			// Also test view folding here
			Rule folded = r2.foldView(r1);
			Assert.assertNotNull(folded);
			Assert.assertNotSame(r1, folded);
			System.out.println(folded);
		}
	}

	protected void test(String s1, String s2, boolean expected, boolean isomorphism) {
		Rule r1 = Rule.parse(s1);
		Rule r2 = Rule.parse(s2);
		String s = isomorphism ? "isomorphism" : "homomorphism";
		if (!expected) {
			s = "non-" + s;
		}
		System.out.println("Testing " + r1 + " and " + r2 + " for " + s);
		RuleMorphism m = isomorphism ? r1.findIsomorphism(r2) : r1.findHomomorphism(r2);
		System.out.println(m);
		Assert.assertEquals(expected, m != null);
	}
	
	public void testFindIsomorphism() {
		for (int i = 0; i+1 < s_isomorphism.length; i += 2) {
			test(s_isomorphism[i], s_isomorphism[i+1], true, true);
		}
		for (int i = 0; i+1 < s_nonIsomorphism.length; i += 2) {
			test(s_nonIsomorphism[i], s_nonIsomorphism[i+1], false, true);
		}
	}
	
	public void testFindHomomorphism() {
		for (int i = 0; i+1 < s_isomorphism.length; i += 2) {
			test(s_isomorphism[i], s_isomorphism[i+1], true, false);
			test(s_isomorphism[i+1], s_isomorphism[i], true, false);
		}
		for (int i = 0; i+1 < s_homomorphism.length; i += 2) {
			test(s_homomorphism[i], s_homomorphism[i+1], true, false);
		}
		for (int i = 0; i+1 < s_nonHomomorphism.length; i += 2) {
			test(s_nonHomomorphism[i], s_nonHomomorphism[i+1], false, false);
		}
	}
	
	public void testFindSubstitution() {
		for (int i = 0; i+1 < s_substitutions.length; i += 2) {
			findSubstitution(s_substitutions[i], s_substitutions[i+1], true);
		}
		for (int i = 0; i+1 < s_nonSubstitutions.length; i += 2) {
			findSubstitution(s_nonSubstitutions[i], s_nonSubstitutions[i+1], false);
		}		
	}
	
	public void testViewUnfolding() {
		for (int i = 0; i+2 < s_unfoldings.length; i += 3) {
			Rule r1 = Rule.parse(s_unfoldings[i]);
			Rule r2 = Rule.parse(s_unfoldings[i+1]);
			Rule r3 = Rule.parse(s_unfoldings[i+2]);
			System.out.println("Unfolding " + r2 + " in " + r1 + "... ");
			Rule result = r1.unfoldView(r2);
			System.out.println(result);
			RuleMorphism m = result.findIsomorphism(r3); 
			Assert.assertNotNull(m);
		}
	}
	
	public void testRegression() {
		// Q(0,1) :- S(0,2), S(4,1), V(2,4)
		Atom head = new Atom(Utils.TOKENIZER.getInteger("Q"), 0, 1);
		Atom b1 = new Atom(Utils.TOKENIZER.getInteger("S"), 0, 2);
		Atom b2 = new Atom(Utils.TOKENIZER.getInteger("S"), 4, 1);
		Atom b3 = new Atom(Utils.TOKENIZER.getInteger("V"), 2, 4);
		Rule r = new Rule(head, b1, b2, b3);
		Assert.assertEquals(r.m_varcount, 4);
	}
	
	public void testConstants() {
		Rule r = Rule.parse("Q(x,z,'3') :- R(x,'2'), R('2',z)");
		System.out.println(r.toSQL());
		r = Rule.parse("R(0,1) :- false");
		System.out.println(r.toSQL());
	}
	
	static final String BIGUNION = "Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)\n" +
		"Q4_R1(0,1,2,3,4,5,6,7,8,9,10,11,12) :- P0_R0_src(0,51,52,35,30,26,54,31,53,56,32,13,33,55), P0_R1_src(0,29,3,58,57,60,59,17,8,27,62,61,28), P0_R0_src(0,10,6,75,76,77,37,79,5,16,78,81,80,9), P0_R1_src(0,83,82,14,36,12,18,85,84,87,34,15,86), P0_R0_src(0,63,64,11,4,21,66,2,65,68,7,40,20,67), P0_R1_src(0,1,38,70,69,72,71,39,42,24,74,73,25), P0_R0_src(0,50,48,88,89,90,22,92,47,44,91,94,93,43), P0_R1_src(0,96,95,49,19,46,45,98,97,100,23,41,99)";

}







