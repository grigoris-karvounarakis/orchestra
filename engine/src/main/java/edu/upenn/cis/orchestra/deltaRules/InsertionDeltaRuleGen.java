package edu.upenn.cis.orchestra.deltaRules;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Atom.AtomType;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * Rule generator for insertion delta rules
 * 
 * @author zives, gkarvoun
 *
 */
public class InsertionDeltaRuleGen extends DeltaRuleGen {
	public InsertionDeltaRuleGen (OrchestraSystem sys)//DeltaRules dr)
	{
		super(sys);//dr);
	}

	public InsertionDeltaRuleGen (OrchestraSystem sys/*DeltaRules dr*/, boolean DRed)
	{
		super(sys, DRed);
	}

	@Override
	protected List<DatalogSequence> computeRules(boolean DRed, IDb db) {
		List<DatalogSequence> ret = new ArrayList<DatalogSequence>();

		ret.add(preInsertion(db));
		ret.add(createInsertionProgramSequence(db, Config.getOuterUnion()));

		ret.add(postInsertion(Config.getApplyBaseInsertions(), db));
		
		return ret;
	}
	
	@Override
	public long execute(DatalogEngine de) throws Exception {
//		dbCn.commit();+
//		_provenancePrep.activateNotLoggedInitDB2(dbCn, system);
				
//			dbCn.commit();
		//_provenancePrep.collectStatistics(dbCn, system);
		
		
		// Map to each field it's database datatype. This is necessary 
		// because DB2 needs to cast null values!!!
		try {	
			Calendar before;
			Calendar after;
			long time;
			long retTime;
			
			//if(!"yes".equals(System.getProperty("skipins"))){
			System.out.println("=====================================================");
			System.out.println("INSERTIONS");
			System.out.println("=====================================================");
			
			de.commitAndReset();
			
			if(de._sql instanceof SqlDb)
				((SqlDb)de._sql).activateRuleBasedOptimizer();
			
			List<DatalogSequence> insProg = getCode();

			before = Calendar.getInstance();
			de.evaluatePrograms(insProg.get(0));
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("INSERTION PREP TIME: " + time + " msec");		
			
			de.commitAndReset();
			
			before = Calendar.getInstance();
			de.evaluatePrograms(insProg.get(1));
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("INCREMENTAL INSERTION ALG TIME (INCL COMMIT): " + time + " msec");
	        System.out.println("EXP: TIME SPENT FOR COMMIT AND LOGGING DEACTIVATION: " + de.logTime() + " msec");
	        System.out.println("EXP: TIME SPENT FOR EMPTY CHECKING: " + de.emptyTime() + " msec");
			System.out.println("EXP: NET INSERTION TIME: " + (time - de.logTime()) + " msec");

			SqlEngine.insTimes.add(new Long(time - de.logTime()));
			retTime = time - de.logTime();
			
			de.commitAndReset();
			
			before = Calendar.getInstance();
			de.evaluatePrograms(insProg.get(2));
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("POST INSERTION TIME: " + time + " msec");
	
			de.commitAndReset();

			return retTime;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
//			return -1;
		}
	}

	protected DatalogSequence preInsertion(IDb db) {
		DatalogSequence ret;
		ret = new DatalogSequence(false, true);
        List<Rule> init = new ArrayList<Rule>();
        
        init.addAll(copyRelationList(getEdbs(), AtomType.NEW, AtomType.NONE, db));
//        init.addAll(copyRelationList(getEdbs(), AtomType.NEW, AtomType.INS));
        init.addAll(copyRelationList(getIdbs(), AtomType.NEW, AtomType.NONE, db));
        init.addAll(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.NONE, db));
        init.addAll(copyRelationList(getOuterJoinRelations(), AtomType.NEW, AtomType.NONE, db));
        init.addAll(copyRelationList(getOuterUnionRelations(), AtomType.NEW, AtomType.NONE, db));
        
        ret.add(new NonRecursiveDatalogProgram(init, true));
		return ret;
	}

	protected DatalogSequence postInsertion(boolean apply, IDb db) {
		DatalogSequence ret;
		ret = new DatalogSequence(false, true);
		List<RelationContext> emptyList = new ArrayList<RelationContext>();
		
		if(apply){
			ret.add(applyDeltasToBase(getMappingRelations(), getEdbs(), getIdbs(), db));
			ret.add(applyDeltasToBase(getOuterJoinRelations(), emptyList, emptyList, db));
			ret.add(applyDeltasToBase(getOuterUnionRelations(), emptyList, emptyList, db));
		}else{
			ret.add(cleanupRelations(AtomType.NEW, getMappingRelations(), getEdbs(), getIdbs(), db));
			ret.add(cleanupRelations(AtomType.NEW, getOuterJoinRelations(), emptyList, emptyList, db));
			ret.add(cleanupRelations(AtomType.NEW, getOuterUnionRelations(), emptyList, emptyList, db));
		}
		
        ret.add(cleanupRelations(AtomType.INS, getMappingRelations(), getEdbs(), getIdbs(), db));
        ret.add(cleanupRelations(AtomType.INS, getOuterJoinRelations(), emptyList, emptyList, db));
        ret.add(cleanupRelations(AtomType.INS, getOuterUnionRelations(), emptyList, emptyList, db));
		return ret;
	}
    
//	protected DatalogSequence createOUInsertionProgramSequence(IDb db) {
//			List<Rule> mainloop = new ArrayList<Rule>();
//	        DatalogSequence ret = new DatalogSequence(false, true);
//	
//	        ret.add(new NonRecursiveDatalogProgram(copyRelationList(getEdbs(), AtomType.NEW, AtomType.INS, db), true));
//	        
//	        // Map in data from pure-EDB relations, i.e. those not in the head
//	        // of any rule
//			//ret.add(_dr.provenanceEdbDeltaApplicationRules(true));
//	        
//	        //if (_dr == null)
//	        //	System.err.println("No delta rules");
//	        
//	        if (getMappingRules() == null)
//	        	System.err.println("No OU rules");
//	
//	        for (Rule r : getMappingRules()) {
//		        mainloop.addAll(insertionRules(r, true, db));
//	        }
//	
//	        for (Rule r : getMappingProjectionRules()) {
//		        mainloop.addAll(insertionRules(r, false, db));
//	        }
//	        
//	//        for (Rule r : _dr.getOuterUnionPopulationRules()) {
//	//	        mainloop.addAll(insertionRules(r));
//	//        }
//	        
//	        mainloop.addAll(idbDeltaApplicationRules(true, false));
//	        
//	        ret.add(new RecursiveDatalogProgram(mainloop, true));
//			
//	        return ret;
//		}

	/**
	 * Creates the datalog program sequence (including copies/deletions) to
	 * apply a set of insertions.
	 * 
	 */
    protected DatalogSequence createInsertionProgramSequence(IDb db, boolean outerUnion) {
        DatalogSequence ret = new DatalogSequence(false, true);

       	ret.add(new NonRecursiveDatalogProgram(copyRelationList(getEdbs(), AtomType.NEW, AtomType.INS, db), true));
       	List<Rule> l2p = new ArrayList<Rule>();
       	for(Rule r : getLocal2PeerRules()) {
       		l2p.addAll(insertionRules(r, outerUnion, false, db));
        }
       	ret.add(new NonRecursiveDatalogProgram(l2p));
       	
       	List<Datalog> progList = new ArrayList<Datalog>();

        List<Rule> defs = new ArrayList<Rule>();
        List<Rule> combinedMappings = new ArrayList<Rule>();
        List<Rule> idbIns = new ArrayList<Rule>();        

        for(Rule r : getSource2ProvRulesForIns()) {
       		defs.addAll(insertionRules(r, outerUnion, false, db));
        }

        boolean existCombined = false;
        for(Rule r : getOuterJoinRules()) {
        	existCombined = true;
        	combinedMappings.addAll(insertionRules(r, outerUnion, true, db));
        }
        for(Rule r : getOuterUnionRules()) {
        	existCombined = true;
        	combinedMappings.addAll(insertionRules(r, outerUnion, true, db));
        }
        
        for(Rule r : getProv2TargetRulesForIns()) {
        	idbIns.addAll(insertionRules(r, outerUnion, false, db));
        }

		List<Rule> newDefs = new ArrayList<Rule>();
		List<Rule> unfoldedIdbIns = unfoldProvDefs(defs, idbIns, newDefs, false);

		progList.add(new NonRecursiveDatalogProgram(newDefs, true));
		if(existCombined){
			progList.add(new NonRecursiveDatalogProgram(combinedMappings, true));
		}
		progList.add(new NonRecursiveDatalogProgram(unfoldedIdbIns, true));
    	
//        NonRecursiveDatalogProgram appl = new NonRecursiveDatalogProgram(idbDeltaApplicationRules(true, false), false);
//		Count4fixpoint because of initial copying of data from local2peer relations 
        NonRecursiveDatalogProgram appl = new NonRecursiveDatalogProgram(idbDeltaApplicationRules(true, false), true);
        progList.add(appl);

        NonRecursiveDatalogProgram applMap = new NonRecursiveDatalogProgram((copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.INS, db)));
        progList.add(applMap);
//        NonRecursiveDatalogProgram applOJ = new NonRecursiveDatalogProgram((copyRelationList(getOuterJoinRelations(), AtomType.NEW, AtomType.INS, db)));
//        progList.add(applOJ);
        
        DatalogSequence p = new DatalogSequence(true, progList, false);
        ret.add(p);

        List<Rule> mappingAppl = new ArrayList<Rule>();
//        mappingAppl.addAll(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.INS, db));
        mappingAppl.addAll(copyRelationList(getOuterJoinRelations(), AtomType.NEW, AtomType.INS, db));
        mappingAppl.addAll(copyRelationList(getOuterUnionRelations(), AtomType.NEW, AtomType.INS, db));
        
        List<Rule> trash = new ArrayList<Rule>();
        List<Rule> unfoldedMappingAppl = unfoldProvDefs(defs, mappingAppl, trash, false);
        
//        ret.add(new NonRecursiveDatalogProgram(mappingAppl, true));
        ret.add(new NonRecursiveDatalogProgram(unfoldedMappingAppl, true));
        
        return ret;
    }	

	//Note: some atoms are shared between different rules. If these atoms need to be modified 
	// independently in another process, we should deep copy all atoms.
	
	/**
	 * Generate all insertion delta rules for a given rule 
	 * @param r Rule for which to generate the delta rules
	 * @return Delta rules
	 */
	private static List<Rule> insertionRules (Rule r, boolean replaceValsWithNullVals, boolean allStrata, IDb db)
    //private static List<DatalogProgram> insertionRules (Rule r)
	{
		AtomType type = AtomType.INS;
		
		Atom head = new Atom (r.getHead(), type);
		if(allStrata)
			head.setAllStrata();
		
		List<Atom> body = new ArrayList<Atom> ();
		for (Atom atom : r.getBody()){
			Atom a = new Atom (atom, AtomType.NONE);
			if(allStrata)
				a.setAllStrata();
			body.add (a);
		}
		List<Rule> deltas = new ArrayList<Rule> ();
		//List<DatalogProgram> deltas = new ArrayList<DatalogProgram> ();
		int size = body.size();
//		if(body.get(size - 1).isNeg()){
//			size = size - 1;
//		}
		
		int lastInd = -1;
		for (int i = 0 ; i < size ; i++)
		{
			while(i < size && (body.get(i).isSkolem() || body.get(i).isNeg())){
				i++;
			}
			
			if (lastInd >= 0){
//				body.set(lastInd, new Atom(body.get(i-1), AtomType.NEW));
				body.get(lastInd).setType(AtomType.NEW);
//				body.set(lastInd, new Atom(body.get(lastInd), AtomType.NEW));
			}
//			body.set (i, new Atom(body.get(i), type));
			if(i < size){
				body.get(i).setType(type);

				lastInd = i;

				Rule deltaRule = new Rule(head, body, r.getParentMapping(), db);
//				if(replaceValsWithNullVals)

//				Greg: I think we should not do this now that we have real skolems
//				if(!Config.isWideProvenance())
//					deltaRule.setReplaceValsWithNullValues();
				
				deltas.add(deltaRule);
				//deltas.add(new SingleRuleDatalogProgram(new Rule(head, body)));
			}
		}
		return deltas;
	}
	
	
    /**
     * Takes the NEW table and uses it to update the OLD and original (NONE) tables.
     * These operations aren't counted in execution timing.
     * 
     * @param DRed
     * @return
     */
/*
	protected DatalogSequence OldapplyInsertsToBase(boolean DRed) {
    	List<Rule> rules;
		List<DatalogProgram> ret = new ArrayList<DatalogProgram>();
    	
    	ret.add(new NonRecursiveDatalogProgram(dropRelationList(getEdbs(),AtomType.NONE)));
    	rules = copyRelationList(getEdbs(), AtomType.NONE, AtomType.NEW);
    	rules.addAll(copyRelationList(getEdbs(), AtomType.OLD, AtomType.NONE));
    	ret.add(new NonRecursiveDatalogProgram(rules));
    	
    	ret.add(new NonRecursiveDatalogProgram(dropRelationList(getEdbs(),AtomType.NEW)));
    	rules = copyRelationList(getIdbs(), AtomType.NONE, AtomType.INS);
    	rules.addAll(copyRelationList(getIdbs(), AtomType.OLD, AtomType.NONE));
    	ret.add(new NonRecursiveDatalogProgram(rules));
    	ret.add(new NonRecursiveDatalogProgram(dropRelationList(getIdbs(),AtomType.NEW)));
    	
    	if(DRed){
        	rules = copyRelationList(getMappingRelations(), AtomType.NONE, AtomType.INS);
        	rules.addAll(copyRelationList(getMappingRelations(), AtomType.OLD, AtomType.NONE));
        	ret.add(new NonRecursiveDatalogProgram(rules));

        	ret.add(new NonRecursiveDatalogProgram(dropRelationList(getMappingRelations(),AtomType.NEW)));	
    	}else{
        	rules = copyRelationList(getMappingRelations(), AtomType.NONE, AtomType.INS);
    		//ret.addAll(relCopy(getMappingRelations(), AtomType.OLD, AtomType.NONE));
    		//ret.addAll(relCleanup(getMappingRelations(),AtomType.NEW));
    		rules.addAll(copyRelationList(getMappingRelations(), AtomType.NEW, AtomType.NONE));

    		ret.add(new NonRecursiveDatalogProgram(rules));
    	}
    	for (DatalogProgram p: ret)
    		p.omitFromCount();
    	
//		DatalogProgram p = new NonRecursiveDatalogProgram(ret,false);
//    	return p;
    	return new DatalogSequence(false, ret, false);
//    	return new DatalogSequence(false, ret);
	}
*/

}
