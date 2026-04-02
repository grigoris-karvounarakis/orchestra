package edu.upenn.cis.orchestra.deltaRules;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogProgram;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.SingleRuleDatalogProgram;
import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.TranslationState;
import edu.upenn.cis.orchestra.datamodel.TypedRelation;
import edu.upenn.cis.orchestra.datamodel.Atom.AtomType;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.exchange.CreateProvenanceStorage;
import edu.upenn.cis.orchestra.exchange.RuleQuery;
import edu.upenn.cis.orchestra.mappings.MappingsIOMgt;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.MappingsTranslationMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.OuterJoinUnfolder;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;

/**
 * Basic generator for delta rules, based on
 * knowledge of provenance relations.
 * 
 * @author zives, gkarvoun
 *
 */
public abstract class DeltaRuleGen {
	//protected DeltaRules _dr;
	//protected CreateProvenanceStorage _provenancePrep;
	protected OrchestraSystem _system;
//	protected DatalogSequence _preCode;
//	protected DatalogSequence _code;
//	protected DatalogSequence _postCode;

	List<DatalogSequence> _code;

	/**
	 * Base abstract class for delta rule generation
	 *  
	 * @param dr
	 */
	public DeltaRuleGen (OrchestraSystem sys) 
	{
		_system = sys;
		//_provenancePrep = _dr.getProvenancePrepInfo();

		createRules(false, sys.getMappingDb());
	}

	/**
	 * Base abstract class for delta rule generation
	 *  
	 * @param dr
	 */
	public DeltaRuleGen (OrchestraSystem sys, boolean DRed)
	{
		//_provenancePrep = _dr.getProvenancePrepInfo();
		_system = sys;
		createRules(DRed, sys.getMappingDb());
	}

	/**
	 * Create, cache, and return a set of rules
	 * 
	 * @param DRed Setting for whether to use DRed algorithm 
	 * @return
	 */
	public List<DatalogSequence> createRules(boolean DRed, IDb db) {

		setCode(computeRules(DRed, db));

		return getCode();
	}

	/**
	 * Execute the internally generated set of rules
	 * 
	 * @param de
	 * @return
	 */
	public abstract long execute(DatalogEngine de) throws Exception;

	protected abstract List<DatalogSequence> computeRules(boolean DRed, IDb db);

	public void cleanupPreparedStmts() {
		List<DatalogSequence> prog = getCode();

		if(prog != null){
			for(DatalogSequence ds : prog){
				for(Datalog d : ds.getSequence()){
					if(d instanceof DatalogProgram){
						DatalogProgram dp = (DatalogProgram)d;
						RuleQuery rq = dp.statements();
						if(rq != null)
							rq.cleanupPrepared();
					}else if(d instanceof SingleRuleDatalogProgram){
						SingleRuleDatalogProgram dp = (SingleRuleDatalogProgram)d;
						RuleQuery rq = dp.statements();
						if(rq != null)
							rq.cleanupPrepared();
					}
				}
			}
		}
	}

//	public List<Rule> getBaseInsRules ()
//	{
////	why not baseIns?
////	return _insRules;
//	return getSystem().getMappingEngine().getBaseInsertionRules();
//	}

	public BasicEngine getMappingEngine() {
		return getSystem().getMappingEngine();
	}

	public IDb getMappingDb() {
		return getSystem().getMappingDb();
	}

	public OrchestraSystem getSystem() {
		return _system;
	}

	public TranslationState getState() {
		return getMappingEngine().getState();
	}

	/**
	 * Get the EDB relations
	 * @return EDB relations
	 */
	public List<RelationContext> getEdbs() 
	{
		return getState().getEdbs(getSystem());
	}

	/**
	 * Get the IDB relations
	 * @return IDB relations
	 */
	public List<RelationContext> getIdbs() 
	{
		return getState().getIdbs(getSystem());
	}

	public List<RelationContext> getMappingRelations() {
//		return getState().getMappingRelations();
		return getState().getRealMappingRelations();
	}

	protected boolean isIdb(RelationContext rel){
		return (getIdbs().contains(rel));
	}

	public static List<Rule> getProv2TargetRules (TranslationState state, IDb db)
	{
		return subtractFakeRules(MappingsInversionMgt.splitMappingsHeads(state.getProv2TargetMappings(), db)); 
	}   

	/**
	 * "ForProvQ" rules are meant to be used for inverse traversal of the provenance 
	 * graph, so existential variables there need not be skolemized (and new existentials
	 * from the body of the mapping (pre-inversion) are dealt with, since we are just 
	 * interested in tuples already in the database (or some particular tuple, in the case 
	 * of the provenance viewer) instead of introducing new ones (as in the Ins/Del case) 
	 *      
	 * @return
	 */

	public static List<Rule> getSource2ProvRulesForProvQ (TranslationState state)
	{
		return subtractFakeRules(MappingsTranslationMgt.computeSource2ProvMappingsForProvQ(
				state.getSource2ProvRules()));
	}    

	public static List<Rule> getProv2TargetRulesForProvQ (TranslationState state, IDb db)
	{
		return subtractFakeRules(MappingsInversionMgt.splitMappingsHeads(
				MappingsTranslationMgt.computeProv2TargetMappingsForProvQ(
						state.getProv2TargetMappings()), 
						db));
	}  

	/**
	 * "ForIns" rules are joined with mapping heads to insert in provenance 
	 * relations those derivations that could be obtained by extending a chase 
	 * homomorphism to use existing tuples to satisfy the target (head) of the
	 * mapping, instead of producing new ones with labeled nulls 
	 * 
	 * @return
	 */

	public static List<Mapping> getProv2TargetMappingsForIns (TranslationState state)
	{
		return subtractFakeMappings(MappingsTranslationMgt.computeProv2TargetMappingsForIns(
				state.getProv2TargetMappings()));
	}

	public static List<Rule> getProv2TargetRulesForIns (TranslationState state, IDb db, OrchestraSystem system)
	{
		return subtractFakeRules(MappingsIOMgt.inOutTranslationR(system, 
				MappingsInversionMgt.splitMappingsHeads(getProv2TargetMappingsForIns(state), 
						db), true)); 
	}   

	public static List<Rule> getSource2ProvRulesForIns (TranslationState state)
	{
		return subtractFakeRules(MappingsTranslationMgt.computeSource2ProvMappingsForIns(
				state.getSource2ProvRules()));
	}

	/**
	 * Convenience methods, to avoid passing parameters for local methods in this class
	 * @return
	 */
	protected List<Rule> getSource2ProvRulesForProvQ ()
	{
		return subtractFakeRules(DeltaRuleGen.getSource2ProvRulesForProvQ(getState()));
	}    

	protected List<Rule> getProv2TargetRulesForProvQ ()
	{
		return subtractFakeRules(DeltaRuleGen.getProv2TargetRulesForProvQ(getState(), getMappingDb()));
	}  

	protected List<Rule> getProv2TargetRules ()
	{
		return subtractFakeRules(DeltaRuleGen.getProv2TargetRules(getState(), getMappingDb()));
	}   

	protected List<Mapping> getProv2TargetMappingsForIns ()
	{
		return subtractFakeMappings(DeltaRuleGen.getProv2TargetMappingsForIns(getState()));
	}

	protected List<Rule> getProv2TargetRulesForIns ()
	{
		return subtractFakeRules(DeltaRuleGen.getProv2TargetRulesForIns(getState(), getMappingDb(), getSystem()));
	}

	public static List<Rule> subtractFakeRules(List<Rule> allMappings)
	{
		if(Config.getSkipFakeMappings()){
			List<Rule> realMappings = new ArrayList<Rule>();
			for(Rule r : allMappings){
				if(!r.isFakeMapping()){
					realMappings.add(r);
				}
			}
			return realMappings;
		}else{
			return allMappings;
		}
	}

	public static List<Mapping> subtractFakeMappings(List<Mapping> allMappings)
	{
		if(Config.getSkipFakeMappings()){

			List<Mapping> realMappings = new ArrayList<Mapping>();
			for(Mapping r : allMappings){
				if(!r.isFakeMapping()){
					realMappings.add(r);
				}
			}
			return realMappings;
		}else{
			return allMappings;

		}
	}


	protected List<Rule> getSource2ProvRulesForIns ()
	{
		return subtractFakeRules(DeltaRuleGen.getSource2ProvRulesForIns(getState()));
	}

	protected List<Rule> getSource2ProvRules ()
	{
		return subtractFakeRules(getState().getSource2ProvRules());
	}

	protected List<Rule> getOuterJoinRules ()
	{
		List<Rule> ret = new ArrayList<Rule>();
		for(RelationContext relCtx : getOuterJoinRelations()){
			ProvenanceRelation prel = (ProvenanceRelation)relCtx.getRelation();
			ret.addAll(prel.outerJoinMappings(getMappingDb()));
		}
		return ret;
	}

	protected List<Rule> getOuterUnionRules ()
	{
		List<Rule> ret = new ArrayList<Rule>();
		for(RelationContext relCtx : getOuterUnionRelations()){
			ProvenanceRelation prel = (ProvenanceRelation)relCtx.getRelation();
			ret.addAll(prel.outerUnionMappings(getMappingDb()));
		}
		return ret;
	}

	protected List<RelationContext> getOuterJoinRelations ()
	{
		return getState().getOuterJoinRelations();
	}

	protected List<RelationContext> getOuterUnionRelations ()
	{
		return getState().getOuterUnionRelations();
	}

	protected List<Mapping> getProv2TargetMappings ()
	{
		return subtractFakeMappings(getState().getProv2TargetMappings());
	} 

	protected List<Rule> getSource2TargetRules ()
	{
		return subtractFakeRules(getState().getSource2TargetRules());
	} 

	protected List<Rule> getLocal2PeerRules ()
	{
		return getState().getLocal2PeerRules();
	} 

	public List<RelationContext> getOuterUnionMappingRels() {
		return getState().getMappingRelations();
	}

	public CreateProvenanceStorage getProvenancePrepInfo() {
		return getMappingEngine().getProvenancePrepInfo();
	}

	protected DatalogProgram edbDeltaApplicationRules(boolean ins){
		return edbDeltaApplicationRules(ins, getEdbs(), getSystem().getMappingDb());
	}

	protected List<Rule> idbDeltaApplicationRules(boolean ins, boolean DRed, 
			boolean skipold, boolean includeKeysOnly){
		return deltaApplicationRules(getIdbs(), ins, DRed, skipold, includeKeysOnly, getSystem().getMappingDb());
	}

	protected List<Rule> idbDeltaApplicationRules(boolean ins, boolean includeKeysOnly){
		return idbDeltaApplicationRules(ins, false, false, includeKeysOnly);
	}

	protected List<Rule> mappingDeltaApplicationRules(boolean ins, boolean DRed, 
			boolean skipold, boolean includeKeysOnly){
		return deltaApplicationRules(getMappingRelations(), ins, DRed, skipold, includeKeysOnly, getSystem().getMappingDb());
	}

	protected List<Rule> mappingDeltaApplicationRules(boolean ins, boolean includeKeysOnly){
		return mappingDeltaApplicationRules(ins, false, false, includeKeysOnly);
	}


	protected List<Rule> mappingDeltaApplicationRules(boolean ins, boolean DRed, 
			boolean skipold){
		return mappingDeltaApplicationRules(getMappingRelations(), ins, DRed, skipold, getSystem().getMappingDb());
	}

	protected DatalogSequence idbDelCopyAndCleanup() {
		return relCopyDeltoAllDel(getIdbs(), getSystem().getMappingDb());
	}

	protected DatalogSequence mappingDelCopyAndCleanup() {
		return relCopyDeltoAllDel(getMappingRelations(), getSystem().getMappingDb());
	}



	public List<DatalogSequence> getCode() {
		return _code;
	}

	public void setCode(List<DatalogSequence> code) {
		_code = new ArrayList<DatalogSequence>();

		_code.addAll(code);	}



	/**
	 * If DO_APPLY, "moves" NEW to NONE for all edbs, idbs and mapping rels
	 * else it just clears the NEW
	 * 
	 * These operations aren't counted in execution timing.
	 * 
	 * @return
	 */
	protected static DatalogProgram applyDeltasToBase(List<RelationContext> mappingRels,
			List<RelationContext> edbs, List<RelationContext> idbs, IDb db) {
		List<Rule> rules = new ArrayList<Rule>();
		DatalogProgram p;

		rules.addAll(moveRelationList(edbs/*getEdbs()*/, AtomType.NONE, AtomType.NEW, db));
		rules.addAll(moveRelationList(idbs/*getIdbs()*/, AtomType.NONE, AtomType.NEW, db));
		rules.addAll(moveRelationList(mappingRels/*getMappingRelations()*/, AtomType.NONE, AtomType.NEW, db));
		p = new NonRecursiveDatalogProgram(rules, true);


		p.omitFromCount();
		return p;
	}

	public static DatalogProgram cleanupEdbs(AtomType typ, List<RelationContext> edbs, IDb db) {
		List<Rule> ret = new ArrayList<Rule>();

		ret.addAll(clearRelationList(edbs/*getEdbs()*/, typ, db));

		DatalogProgram p = new NonRecursiveDatalogProgram(ret,false);
		return p;
	}
	/**
	 * @deprecated
	 */
	public static DatalogProgram cleanupMappings(AtomType typ, List<RelationContext> mappingRelations, IDb db) {
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();

		ret.addAll(clearRelationList(mappingRelations/*getMappingRelations()*/, typ, db));

		DatalogProgram p = new NonRecursiveDatalogProgram(ret,false);
		return p;
	}


	public static DatalogProgram cleanupRelations(AtomType typ, List<RelationContext> mappingRels,
			List<RelationContext> edbs, List<RelationContext> idbs, IDb db) {
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();

		ret.addAll(clearRelationList(idbs, typ, db));

		// I think I shouldn't delete edb dels ...
		if (typ != AtomType.DEL && typ != AtomType.RCH)
			ret.addAll(clearRelationList(edbs, typ, db));

		if (typ != AtomType.RCH)
			ret.addAll(clearRelationList(mappingRels, typ, db));

		DatalogProgram p = new NonRecursiveDatalogProgram(ret,false);
		return p;
	}

	public static List<Rule> clearRelationList(List<RelationContext> rels, AtomType type,
			IDb db){
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();

		for(RelationContext rel : rels){
			Rule r = relCleanup(rel, type, db);
			ret.add(r);//new SingleRuleDatalogProgram(r));
		}
		return ret;
	}

	public static List<Rule> copyRelationList(List<RelationContext> rels, AtomType headType, 
			AtomType bodyType, IDb db){
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();
		for(RelationContext rel : rels){
			Rule r = relCopy(rel, headType, bodyType, db);
			ret.add(r);//new SingleRuleDatalogProgram(r));
		}
		return ret;
	}

	protected static List<Rule> deltaApplicationRule(RelationContext relation, boolean pos, AtomType type, 
			boolean skipold, IDb db) {
		return deltaApplicationRule(relation, pos, AtomType.NEW, AtomType.NONE, type, skipold, false, true, db);
	}

	protected static List<Rule> deltaApplicationRule(RelationContext relation, boolean pos, AtomType type, 
			boolean skipold, boolean includeKeysOnly, IDb db) {
		return deltaApplicationRule(relation, pos, AtomType.NEW, AtomType.NONE, type, skipold, includeKeysOnly, true, db);
	}

	public static List<Rule> deltaApplicationRule(RelationContext relation, boolean pos,
			AtomType resType, AtomType relType, AtomType deltaType, 
			boolean skipold, boolean includeKeysOnly, boolean deleteFromHead, IDb db) {
		List<Rule> ret = new ArrayList<Rule>();
		List<AtomArgument> vars = new ArrayList<AtomArgument>();

		for(int i = 0; i < relation.getRelation().getFields().size(); i++){
			vars.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
		}
		Atom head = new Atom(relation, vars, resType);

		//ScMappingAtom bodyatom = new ScMappingAtom(head, pos ? AtomType.INS : AtomType.DEL);
		Atom bodyatom = new Atom(head, deltaType);

		if(Config.getStratified() && deltaType == AtomType.DEL)
			//    	if(Config.DO_STRATIFIED)
			bodyatom.setAllStrata(); // STRATIFIED/INS doesn't need allStrata, just last one

		List<Atom> body1 = new ArrayList<Atom>();
		Atom bodyat1 = head.deepCopy();
		bodyat1.setType(relType);
		body1.add(bodyat1);

		Rule r;
		if(pos){
			if (skipold == false) {
				ret.add(new Rule(head, body1, null, includeKeysOnly, db));
			}
			List<Atom> body2 = new ArrayList<Atom>();
			body2.add(bodyatom);
			r = new Rule(head, body2, null, includeKeysOnly, db);
		}else{
			bodyatom.negate();
			body1.add(bodyatom);
			r = new Rule(head, body1, null, includeKeysOnly, db);
			if(deleteFromHead)
				r.setDeleteFromHead();
		}

		ret.add(r);

		return ret;      
	}

	public static List<Rule> applyRelonRel(List<RelationContext> rels, boolean pos, AtomType resType,
			AtomType relType, AtomType deltaType, boolean skipold, boolean includeKeysOnly, boolean deleteFromHead, IDb db){

		List<Rule> vr = new ArrayList<Rule>();

		for(RelationContext r : rels){
			vr.addAll(deltaApplicationRule(r, pos, resType, relType, deltaType, skipold, includeKeysOnly, deleteFromHead, db));
		}
		return vr;
	}


	public static List<Rule> deltaApplicationRules(List<RelationContext> rels, boolean ins, boolean DRed, 
			boolean skipold, boolean includeKeysOnly, IDb db){
		List<Rule> vr = new ArrayList<Rule>();

		for(RelationContext idb : rels){
			List<Rule> v;
			if(ins){
				if(DRed){
					v = deltaApplicationRule(idb, ins, AtomType.INS, skipold, includeKeysOnly, db);
				}else{
					// Add INS TO NEW inside fixpoint - no need to copy OLD again
					// enforced by skipold = true
					v = deltaApplicationRule(idb, ins, AtomType.INS, true, includeKeysOnly, db);
				}
			}else{
				if(DRed || Config.getStratified()){
					v = deltaApplicationRule(idb, ins, AtomType.DEL, skipold, includeKeysOnly, db);
				}else{
					v = deltaApplicationRule(idb, ins, AtomType.ALLDEL, skipold, includeKeysOnly, db);
				}
			}
			for(Rule r : v) {
				vr.add(r);
				//vr.add(new SingleRuleDatalogProgram(r));
			}
		}
		//NonRecursiveDatalogProgram ret = new NonRecursiveDatalogProgram(vr);
		return vr;//ret;
	}

	protected static DatalogProgram edbDeltaApplicationRules(boolean ins, 
			List<RelationContext> edbs, IDb db){
		//List<DatalogProgram> vr = new ArrayList<DatalogProgram>();
		List<Rule> vr = new ArrayList<Rule>();

		for(RelationContext edb : edbs) { //getEdbs()){
			List<Rule> v;
			if(ins)
				v = deltaApplicationRule(edb, ins, AtomType.INS, false, db);
			else
				v = deltaApplicationRule(edb, ins, AtomType.DEL, false, db);

			for(Rule r : v){ 
				vr.add(r);
				//vr.add(new SingleRuleDatalogProgram(r));
			}
		}
		//seq.add(new NonRecursiveDatalogProgram(vr));
		//NonRecursiveDatalogProgram ret = new NonRecursiveDatalogProgram(vr);
		return new NonRecursiveDatalogProgram(vr, false);//seq;
		//return ret;
	}

	protected static List<Rule> experimentalUnfoldIdbs(List<Rule> rules, List<Mapping> origDefs, List<RelationContext> idbs){
		List<Rule> newRules = new ArrayList<Rule>();
		newRules.addAll(rules);
		List<Mapping> defs = new ArrayList<Mapping>();

		for(Mapping r : origDefs){
			Mapping cutnot = r.deepCopy();
			cutnot.getBody().remove(cutnot.getBody().size()-1);
			//			cutnot.renameExistentialVars();
			defs.add(cutnot);
		}

		for(int j = 0; j < newRules.size(); j++){
			Rule r = newRules.get(j);
			int i = 0;
			//for(i = 0; i < r.getBody().size() && !isIdb(r.getBody().get(i).getRelationContext()); i++);
			for(i = 0; i < r.getBody().size() && !idbs.contains(r.getBody().get(i).getRelationContext()); i++);
			if(i < r.getBody().size()){
				newRules.remove(j);
				//				newRules.addAll(r.substituteAtom(i, defs));
				j--;
			}
		}

		return newRules;
	}

	protected List<Rule> localDerivabilityHeuristicRules(){
		List<Rule> ret = new ArrayList<Rule>();
		
		for(Rule r : getLocal2PeerRules()){
			Rule newRule = r.deepCopy();
			newRule.getHead().setType(AtomType.RCH);
			newRule.getBody().get(0).setType(AtomType.NONE);
			
			Atom goalDir = newRule.getHead().deepCopy();
			goalDir.setType(AtomType.INV);
			newRule.getBody().add(goalDir);
			
			Atom notDel = newRule.getBody().get(0).deepCopy();
			notDel.setType(AtomType.DEL);
			notDel.negate();
			newRule.getBody().add(notDel);
			newRule.setOnlyKeyAndNulls();
			ret.add(newRule);

			Atom head2 = newRule.getHead().deepCopy();
			Atom body2 = newRule.getHead().deepCopy();
			head2.setType(AtomType.INV);
//			head2.negate();
			List<Atom> body = new ArrayList<Atom>();
			
			body.add(head2.deepCopy());
			body2.setType(AtomType.RCH);
			body2.negate();
			body.add(body2);
			
			Rule newRule2 = new Rule(head2, body, r.getParentMapping(), r.getDb());
			newRule2.setDeleteFromHead();
			newRule2.setOnlyKeyAndNulls();
			ret.add(newRule2);
		}
		return ret;
	}
	
	
	protected List<Rule> derivabilityRules() {
		List<Rule> dRules = derivabilityRules(getSource2ProvRulesForProvQ(), //getMappingRules(), 
				getProv2TargetRulesForProvQ(), // getMappingProjectionRules(),
				getProv2TargetMappings(), 
				getEdbs(), getIdbs(), getMappingEngine().getMappingDb());
//		return OuterJoinUnfolder.unfoldOuterJoins(dRules, getOuterJoinRelations(), 
//				AtomType.NEW, AtomType.INV, getMappingDb());
		return dRules;
	}

	protected List<Rule> derivabilityRules(List<Rule> source2provRules, 
			List<Rule> prov2targetRules, List<Mapping> prov2targetMappings, 
			List<RelationContext> edbs, List<RelationContext> idbs, IDb db){

		List<Rule> vr = new ArrayList<Rule>();
		List<Rule> mInvToEdbInv = new ArrayList<Rule>();
		List<Rule> idbInvToMInv = new ArrayList<Rule>();
		List<Rule> invertedMappingRules = new ArrayList<Rule>();

//		Substitute idbs with prov2target rules in body of source2prov rules -> result
//		invert result + mappingProjections ...
		boolean skipFakeMappings = true;

//		Only works correctly for acyclic mappings
		if(skipFakeMappings && Config.isAcyclicSchema()){
			List<Rule> realInvertedSource2provRules = new ArrayList<Rule>();
			List<Rule> realSource2provRules = new ArrayList<Rule>();
			List<Rule> realProv2TargetRules = new ArrayList<Rule>();
			List<Rule> l2pRules = new ArrayList<Rule>();

			for(Rule r : source2provRules) {
				if(!r.isFakeMapping()){
					realSource2provRules.add(r);
					if(r.getSkolemAtoms().size() == 0){
						List<Rule> inv = r.invertForReachabilityTest(true, true, edbs, false);
						for(Rule rr : inv){
							if(!rr.onlyKeyAndNulls())
								rr.setOnlyKeyAndNulls();
							realInvertedSource2provRules.add(rr);
						}
					}
				}
			}
			for(Rule r : getLocal2PeerRules()) {
				realProv2TargetRules.add(r);
				if(r.getSkolemAtoms().size() == 0){
					List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
					for(Rule rr : inv){
						if(!rr.onlyKeyAndNulls())
							rr.setOnlyKeyAndNulls();
						l2pRules.add(rr);
					}
				}
			}
			List<Rule> unfoldedl2pRules = unfoldIdbs(l2pRules, realInvertedSource2provRules, idbs);
			mInvToEdbInv.addAll(unfoldedl2pRules); // inv edbs in terms of mappings 
			mInvToEdbInv.addAll(l2pRules); // inv edbs in terms of idbs, which are the input of this program

			for(Rule r : prov2targetRules) {
//				I think the following is wrong after all ... switching back to rules
//				for(Mapping m : prov2targetMappings) {			
//				List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, edbs, true, db);
				if(!r.isFakeMapping()){
					realProv2TargetRules.add(r);
					List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
					for(Rule rr : inv){
						if(!rr.onlyKeyAndNulls())
							rr.setOnlyKeyAndNulls();

						idbInvToMInv.add(rr);
					}
				}
			}

//			Maybe I should invert first and unfold afterwards ...
//			Otherwise it seems to produce redundant rules
//			List<Rule> unfoldedMappingRules = unfoldIdbs(source2provRules, prov2targetRules, idbs);
			List<Rule> unfoldedMappingRules = unfoldIdbs(realSource2provRules, realProv2TargetRules, idbs);

			for(Rule r : unfoldedMappingRules){
				List<Rule> inv = r.invertForReachabilityTest(false, true, edbs);
				for(Rule rr : inv){
					if(!rr.onlyKeyAndNulls())
						rr.setOnlyKeyAndNulls();

					if(!invertedMappingRules.contains(rr))
						invertedMappingRules.add(rr);
				}
			}
		}else{ // Old code, treating local2Peer mappings as real mappings
			for(Rule r : source2provRules) {
				if(r.getSkolemAtoms().size() == 0){
					List<Rule> inv = r.invertForReachabilityTest(true, false, edbs);
					for(Rule rr : inv){
						if(!rr.onlyKeyAndNulls())
							rr.setOnlyKeyAndNulls();
						mInvToEdbInv.add(rr);
					}
				}
			}

			for(Rule r : prov2targetRules) {
//				I think the following is wrong after all ... switching back to rules
//				for(Mapping m : prov2targetMappings) {			
//				List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, edbs, true, db);
				List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
				for(Rule rr : inv){
					if(!rr.onlyKeyAndNulls())
						rr.setOnlyKeyAndNulls();

					idbInvToMInv.add(rr);
				}
			}

//			Maybe I should invert first and unfold afterwards ...
//			Otherwise it seems to produce redundant rules
			List<Rule> unfoldedMappingRules = unfoldIdbs(source2provRules, prov2targetRules, idbs);

			for(Rule r : unfoldedMappingRules){
				List<Rule> inv = r.invertForReachabilityTest(false, true, edbs);
				for(Rule rr : inv){
					if(!rr.onlyKeyAndNulls())
						rr.setOnlyKeyAndNulls();

					if(!invertedMappingRules.contains(rr))
						invertedMappingRules.add(rr);
				}
			}
		}

//		Replace Mapping relation atoms with Join relations
//		if(outerJoinRelations == null || outerJoinRelations.size() == 0){
		vr.addAll(mInvToEdbInv);
		vr.addAll(invertedMappingRules);
		vr.addAll(idbInvToMInv);
//		}else{ // Some ASRs exist, use them!
//		List<Rule> unusedDefs = new ArrayList<Rule>();
////		I think the first below is unnecessary, since there are no M_NEW in the bodies of these
//		List<Rule> mInvAndJoinRelsToEdbInv = unfoldProvDefs(inverseOuterJoinRules, mInvToEdbInv, unusedDefs, true);
//		List<Rule> invRulesWithMappingAndJoinRels = unfoldProvDefs(inverseOuterJoinRules, invertedMappingRules, unusedDefs, true);
//		List<Rule> idbInvAndJoinRelsToMInv = unfoldProvDefs(inverseOuterJoinRules, idbInvToMInv, unusedDefs, true);

////		The following unfolding needs to be done in the order of mappings in ASRs, otherwise it 
////		doesn't necessarily get rid of M atoms ...
//		List<Rule> allInvRules = new ArrayList<Rule>();
//		allInvRules.addAll(mInvAndJoinRelsToEdbInv);
//		allInvRules.addAll(invRulesWithMappingAndJoinRels);
//		allInvRules.addAll(idbInvAndJoinRelsToMInv);

//		Map<TypedRelation, List<Rule>> defsMap = Rule.list2map(allInvRules); 

//		for(RelationContext rel : outerJoinRelations){
//		ProvenanceRelation provRel = (ProvenanceRelation)rel.getRelation();

//		for(int i = provRel.getRels().size(); i > 0; i--){
//		ProvenanceRelation p = provRel.getRels().get(i-1);
//		TypedRelation key = new TypedRelation(p, AtomType.INV);
//		List<Rule> keyDefs = defsMap.get(key);

//		List<Rule> unfoldedDefs = unfoldProvDefs(keyDefs, allInvRules, unusedDefs, true);
//		for(Rule rr : unfoldedDefs){
//		rr.minimize();
//		}
//		defsMap = Rule.list2map(unfoldedDefs);
//		defsMap.remove(key);
//		allInvRules = Rule.map2list(defsMap);
//////	Change this to use updated rules
////		List<Rule> unfoldedInvMappingRules = unfoldProvDefs(keyDefs, invRulesWithMappingAndJoinRels, unusedDefs, true);
////		for(Rule rr : unfoldedInvMappingRules){
////		rr.minimize();
//////	if(!vr.contains(rr))
//////	vr.add(rr);
////		}

////		List<Rule> unfoldedMInvToEdbInv = unfoldProvDefs(keyDefs, mInvAndJoinRelsToEdbInv, unusedDefs, true);
//////	for(Rule rr : unfoldedMInvToEdbInv){
//////	if(!vr.contains(rr))
//////	vr.add(rr);
//////	}	
//		}
//		}
//		vr.addAll(allInvRules);
//		}


//		FOR NO-OU CASE, BUT CAUSES BLOWUP - WOULD ONLY WORK WITH SELECT DISTINCT BUT DON'T THINK
//		THAT IT WOULD BE FASTER ... (fewer rules, but even with distinct it would have to compute the big join ...)

//		for(ScMapping m : _engine.getProjBefInv()){
//		List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, getEdbs());
//		for(Rule rr : inv){
//		//vr.add(rr);
//		rr.setOnlyKeyAndNulls();
//		rr.setDistinct();
//		vr.add(rr);//new SingleRuleDatalogProgram(rr));
//		}
//		}

		return vr;
	}

	protected List<Rule> lineageRules() {
		return lineageRules(getSource2ProvRulesForProvQ(),
				getProv2TargetRulesForProvQ(), 
				getEdbs(), getIdbs(), getMappingEngine().getMappingDb());
	}

	protected static List<Rule> lineageRules(List<Rule> source2provRules, 
			List<Rule> prov2targetRules, 
			List<RelationContext> edbs, List<RelationContext> idbs, IDb db){

		List<Rule> vr = new ArrayList<Rule>();

//		Substitute idbs with prov2target rules in body of source2prov rules -> result
//		invert result + mappingProjections ...

		for(Rule r : source2provRules) {
			if(r.getSkolemAtoms().size() == 0){
				List<Rule> inv = r.invertForReachabilityTest(true, false, edbs);
				for(Rule rr : inv){
					if(!rr.onlyKeyAndNulls())
						rr.setOnlyKeyAndNulls();
					vr.add(rr);
				}
			}
		}
//		Maybe I should invert first and unfold afterwards ...
//		Otherwise it seems to produce redundant rules
		List<Rule> unfoldedMappingRules = unfoldIdbs(source2provRules, prov2targetRules, idbs);

		for(Rule r : unfoldedMappingRules){
			List<Rule> inv = r.invertForReachabilityTest(false, true, edbs);
			for(Rule rr : inv){
				if(!rr.onlyKeyAndNulls())
					rr.setOnlyKeyAndNulls();
				if(!vr.contains(rr))
					vr.add(rr);
			}
		}

		for(Rule r : prov2targetRules) {
//			for(Mapping m : prov2targetMappings) {			
//			List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, edbs, true, db);
			List<Rule> inv = r.invertForReachabilityTest(true, true, edbs);
			for(Rule rr : inv){
				if(!rr.onlyKeyAndNulls())
					rr.setOnlyKeyAndNulls();

				vr.add(rr);
			}
		}

//		FOR NO-OU CASE, BUT CAUSES BLOWUP - WOULD ONLY WORK WITH SELECT DISTINCT BUT DON'T THINK
//		THAT IT WOULD BE FASTER ... (fewer rules, but even with distinct it would have to compute the big join ...)

//		for(ScMapping m : _engine.getProjBefInv()){
//		List<Rule> inv = Rule.invertMappingForReachabilityTest(m, true, true, getEdbs());
//		for(Rule rr : inv){
//		//vr.add(rr);
//		rr.setOnlyKeyAndNulls();
//		rr.setDistinct();
//		vr.add(rr);//new SingleRuleDatalogProgram(rr));
//		}
//		}

		return vr;
	}


	public static List<Rule>/*NonRecursiveDatalogProgram*/ mappingDeltaApplicationRules(List<RelationContext> mappingRelations,
			boolean ins, boolean DRed, boolean skipold, IDb db){
		List<Rule> vr = new ArrayList<Rule>();
		//List<DatalogProgram> vr = new ArrayList<DatalogProgram>();
		// application of deletions on idbs
		// not necessary to have these as rules
		for(RelationContext idb : mappingRelations) {//getMappingRelations()){
			List<Rule> v;
			if(ins){
				v = deltaApplicationRule(idb, ins, AtomType.INS, skipold, false, db);
			}else{
				if(DRed){
					v = deltaApplicationRule(idb, ins, AtomType.DEL, skipold, false, db);
				}else{
					v = deltaApplicationRule(idb, ins, AtomType.ALLDEL, skipold, false, db);
				}
			}
			for(Rule r : v) {
				vr.add(r);
				//vr.add(new SingleRuleDatalogProgram(r));
			}
		}
		//NonRecursiveDatalogProgram ret = new NonRecursiveDatalogProgram(vr);
		return vr;//ret;
	}

	public static List<Rule> moveRelationList(List<RelationContext> rels, AtomType headType, 
			AtomType bodyType, IDb db){
		List<Rule> ret = new ArrayList<Rule>();
		//List<DatalogProgram> ret = new ArrayList<DatalogProgram>();
		for(RelationContext rel : rels){
			Rule r = relMove(rel, headType, bodyType, db);
			ret.add(r);//new SingleRuleDatalogProgram(r));
		}
		return ret;
	}

	public List<Rule> moveLtoP(AtomType headType, AtomType bodyType, IDb db){
		List<Rule> ret = new ArrayList<Rule>();

		for(Rule r : getLocal2PeerRules()){
			RelationContext rel1 = r.getHead().getRelationContext();
			RelationContext rel2 = r.getBody().get(0).getRelationContext();

			Rule m = relMove(rel1, headType, rel2, bodyType, db);
			ret.add(m);
		}
		return ret;
	}

	protected static DatalogProgram provenanceEdbDeltaApplicationRules(boolean ins, List<RelationContext> edbs,
			IDb db){
		List<Rule> vr = new ArrayList<Rule>();

		// Reminder: idb update rules only need to be evaluated once ... 
		// not necessary to have these as rules
		//DatalogSequence seq = new DatalogSequence(false);
		for(RelationContext edb : edbs) { //getEdbs()){
			List<Rule> v;
			if(ins)
				v = deltaApplicationRule(edb, ins, AtomType.INS, false, false, db);
			else
				v = deltaApplicationRule(edb, ins, AtomType.DEL, false, false, db);
			for(Rule r : v){ 
				vr.add(r);
			}
		}
		return new NonRecursiveDatalogProgram(vr, true);//seq;
	}

	protected static Rule relCleanup(RelationContext relation, AtomType type,
			IDb db) {
		List<AtomArgument> vars = new ArrayList<AtomArgument>();
		for(int i = 0; i < relation.getRelation().getFields().size(); i++){
			vars.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
		}
		Atom deleteHead = new Atom(relation, vars, type);
		deleteHead.negate();
		List<Atom> deleteBody = new ArrayList<Atom>();
		Rule del = new Rule(deleteHead, deleteBody, null, db);

		return del;      
	}

	protected static Rule relCopy(RelationContext relation, AtomType headType, AtomType bodyType, IDb db){
		return relCopy(relation, headType, relation, bodyType, false, db);
	}

	protected static Rule relCopy(RelationContext relation1, AtomType headType, RelationContext relation2, AtomType bodyType, IDb db){
		return relCopy(relation1, headType, relation2, bodyType, false, db);
	}

	protected static Rule relCopy(RelationContext relation1, AtomType headType, RelationContext relation2, AtomType bodyType, boolean delfix, IDb db) {
		List<AtomArgument> vars = new ArrayList<AtomArgument>();
		for(int i = 0; i < relation1.getRelation().getFields().size(); i++){
			vars.add(new AtomVariable(Mapping.getFreshAutogenVariableName()));
		}
		Atom head = new Atom(relation1, vars, headType);
		Atom bodyatom = new Atom(relation2, vars, bodyType);
		List<Atom> copyBody = new ArrayList<Atom>();
		copyBody.add(bodyatom);
		Rule copy = new Rule(head, copyBody, null, delfix, db);	

		return copy;      
	}

	public static DatalogSequence relCopyDeltoAllDel(List<RelationContext> rels, IDb db) {
		//List<Rule> ret = new ArrayList<Rule>();
		List<Datalog> ret = new ArrayList<Datalog>();

		List<Rule> copy = new ArrayList<Rule>();
		for(RelationContext rel : rels){
			copy.add(relCopy(rel, AtomType.ALLDEL, rel, AtomType.DEL, true, db));
		}
		ret.add(new NonRecursiveDatalogProgram(copy, false));

		List<Rule> clean = new ArrayList<Rule>();
		for(RelationContext rel : rels){
			clean.add(relCleanup(rel, AtomType.DEL, db));
		}
		ret.add(new NonRecursiveDatalogProgram(clean, false));

		//DatalogProgram p = new NonRecursiveDatalogProgram(ret,false);
		//return p;

		return new DatalogSequence(false, ret, false);
		//		return new DatalogSequence(false, ret);
	}

	protected static Rule relMove(RelationContext relation, AtomType headType, AtomType bodyType, IDb db){
		Rule newRule = relCopy(relation, headType, relation, bodyType, false, db);
		newRule.setClearNcopy();
		return newRule;
	}

	protected static Rule relMove(RelationContext relation1, AtomType headType, RelationContext relation2, AtomType bodyType, IDb db){
		Rule newRule = relCopy(relation1, headType, relation2, bodyType, false, db);
		newRule.setClearNcopy();
		return newRule;
	}

	/*
	 * 
	 */
	protected static List<Rule> unfoldIdbs(List<Rule> rules, List<Rule> origDefs, List<RelationContext> idbs){
//		protected static List<Rule> unfoldIdbs(List<Rule> rules, List<Mapping> origDefs, List<RelationContext> idbs){
		List<Rule> newRules = new ArrayList<Rule>();
		newRules.addAll(rules);
		List<Rule> defs = new ArrayList<Rule>();

		for(Rule r : origDefs){
			Rule cutnot = r.deepCopy();
			if(r.getBody().get(r.getBody().size() - 1).isNeg()){
				cutnot.getBody().remove(cutnot.getBody().size()-1);
				cutnot.renameExistentialVars();
			}
			defs.add(cutnot);
		}

		//		defs.addAll(local2PeerRules);

		for(int j = 0; j < newRules.size(); j++){
			Rule r = newRules.get(j);
			int i = 0;
			//for(i = 0; i < r.getBody().size() && !isIdb(r.getBody().get(i).getRelationContext()); i++);
			for(i = 0; i < r.getBody().size() && !idbs.contains(r.getBody().get(i).getRelationContext()); i++);
			if(i < r.getBody().size()){
				newRules.remove(j);
//				try{
				newRules.addAll(r.substituteAtom(i, defs, true));
//				}catch(UnsupportedDisjunctionException e){
////				I think this will never happen here anyway, 
////				because I have "cut" the negated clauses above
//				e.printStackTrace();
//				}
				j--;
			}
		}

		return newRules;
	}

	/**
	 * Go through defs and "substituteAtom" the ones whose head only appears in one rule and body 
	 * has a single atom. Those rules that are "unfolded" this way don't need to exist at all
	 * We can also unfold the M- in the inv rules later, where the M- appears positively, but I 
	 * think there it also only makes sense to unfold the same ones as here ...
	 *  
	 * @param defs
	 * @param provUpdRules
	 * @return
	 */
	public static List<Rule> unfoldProvDefs(List<Rule> defs, List<Rule> provUpdRules, List<Rule> unusedDefs,
			boolean unfoldPosMultiAtomDefs) {
		List<Rule> ret = new ArrayList<Rule>();

		if(defs == null){
			return provUpdRules;
		}else{
			unusedDefs.clear();
			unusedDefs.addAll(defs);
			for(Rule r : provUpdRules){
				ret.addAll(r.substituteSingleAtomDefs(defs, unusedDefs, unfoldPosMultiAtomDefs));
			}
		}
		return ret;
//		for(Rule def : defs)
//		unusedDefs.add(def);
//		return provUpdRules;
	}

}
