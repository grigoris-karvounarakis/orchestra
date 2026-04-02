package edu.upenn.cis.orchestra.datamodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen;
import edu.upenn.cis.orchestra.exchange.CreateProvenanceStorage;
import edu.upenn.cis.orchestra.exchange.exceptions.MappingNotFoundException;
import edu.upenn.cis.orchestra.mappings.MappingsIOMgt;
import edu.upenn.cis.orchestra.mappings.MappingsInversionMgt;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation.ProvRelType;

public class TranslationState implements Serializable {
//from BasicEngine
	
	public static final long serialVersionUID = 42;

	public List<RelationContext> _rels;
	/**
	 * List of the edb/idb relations in those rules
	 */
	public List<RelationContext> _edbs;
	public List<RelationContext> _idbs;
	public List<RelationContext> _rej;
	
	public CreateProvenanceStorage _provenancePrep;
	
	public List<RelationContext> _mappingRels;
	public List<RelationContext> _realMappingRels;
	
	public List<RelationContext> _outerJoinRels = new ArrayList<RelationContext>();
	public List<RelationContext> _outerUnionRels = new ArrayList<RelationContext>();

//	Original mappings
	public List<Mapping> _originalMappings;
	
//	OuterUnionMapping structures ... where are they used?
//	public List<OuterUnionMapping> _ouMappings;
	
//	In/out rules. For ins we need to conjoin with not R_r
//	For del it is not necessary
//	public List<Rule> _baseInsRules;


	/**
	 * List of input rules
	 */

	protected List<Rule> _source2TargetRules;
//	protected List<Rule> _source2provForProvQ;
//	protected List<Rule> _prov2targetForProvQ;
//	protected List<Rule> _mappingProjectionRules;
	protected List<Rule> _local2PeerMappings;
	protected List<Mapping> _prov2targetMappings;
//	protected List<Mapping> _prov2targetMappingsForIns;
//	protected List<Rule> _prov2targetRulesForIns;
//	protected List<Rule> _source2provRulesForIns;
	protected List<Rule> _source2provRules;
	
	public TranslationState(List<Mapping> mappings, List<RelationContext> rels) {
		_originalMappings = mappings;
				
		_rels = rels;
	}
	
	public void setMappings(List<Mapping> mappings){
		_originalMappings = mappings;
	}
	
	/**
	 * Get the EDB relations
	 * @return EDB relations
	 */
	public synchronized List<RelationContext> getEdbs(OrchestraSystem system){
		if(_edbs != null)
			return _edbs;
		return getEdbsIdbs(system, true);
	}

	/**
	 * Get the rejection relations
	 * @return rejection relations
	 */
	public synchronized List<RelationContext> getRej(OrchestraSystem system){
		if(_rej != null)
			return _rej;
		return getRejTables(system);
	}
	
	/**
	 * Get the IDB relations
	 * @return IDB relations
	 */
	public synchronized List<RelationContext> getIdbs(OrchestraSystem system){
		if(_idbs != null)
			return _idbs;
		return getEdbsIdbs(system, false);
	}
	
	public synchronized List<RelationContext> getEdbsIdbs(OrchestraSystem system, boolean returnEdbs) 
	{
//		It would be a lot cleaner if this was:
//		idbs: all real relations
//		edbs: all _L, _R
		List<Rule> prov2targetRules = MappingsIOMgt.inOutTranslationR(system, getProv2TargetRules(system.getMappingDb()), true);

		if(getSource2ProvRules() != null &
			prov2targetRules != null && 
			getLocal2PeerRules() != null && 
			_rels != null){
			
			_edbs = new ArrayList<RelationContext>();
			_idbs = new ArrayList<RelationContext>();
			
			List<Mapping> mr = new ArrayList<Mapping>();
			mr.addAll(getSource2ProvRules());
			mr.addAll(prov2targetRules);
			mr.addAll(getLocal2PeerRules());
			extractEdbsIdbs(mr, _rels, _edbs, _idbs);
			if(returnEdbs)
				return _edbs;
			else
				return _idbs;
		}
		return null;
	}

	public synchronized List<RelationContext> getRejTables(OrchestraSystem system){
		List<RelationContext> ret = new ArrayList<RelationContext>();
		for(Peer p : system.getPeers()){
			for(Schema s : p.getSchemas()){
				for(Relation r : s.getRelations()){
					try{
						Relation rr = s.getRelation(r.getLocalRejDbName());
						ret.add(new RelationContext(rr, s, p, false));
					}catch(Exception e){
					}
				}
			}
		}
		return ret;
	}
	
	/**
	 * Get the list of relations for storing mappings
	 */
	public synchronized List<RelationContext> getMappingRelations() {
		return _mappingRels;
	}

	public synchronized List<RelationContext> getRealMappingRelations() {
		if(Config.getSkipFakeMappings()){
			return _realMappingRels;
		}else{
			return _mappingRels;
		}
	}
	
	public synchronized List<RelationContext> getOuterJoinRelations() {
		return _outerJoinRels;
	}
	
	public synchronized List<RelationContext> getOuterUnionRelations() {
		return _outerUnionRels;
	}
	
	public synchronized List<RelationContext> getRelations() {
		return _rels;
	}
	
	public synchronized void setMappingRels(List<RelationContext> mappingRels){
		_mappingRels = mappingRels;
	}

	public synchronized void setRealMappingRels(List<RelationContext> realMappingRels){
		_realMappingRels = realMappingRels;
	}

	public synchronized void setOuterJoinRels(List<RelationContext> outerJoinRels){
		_outerJoinRels = outerJoinRels;
	}
	
	public synchronized void setOuterUnionRels(List<RelationContext> outerUnionRels){
		_outerUnionRels = outerUnionRels;
	}

	public synchronized List<Rule> getSource2TargetRules() {
		return DeltaRuleGen.subtractFakeRules(_source2TargetRules);
	}

	public synchronized void setSource2TargetRules(List<Rule> source2TargetRules) {
		_source2TargetRules = source2TargetRules;
	}
	
	public synchronized List<Mapping> getOriginalMappings() {
		return _originalMappings;
	}
	
	public synchronized void setOriginalMappings(List<Mapping> mappings) {
		_originalMappings = mappings;
	}	

	public synchronized List<Mapping> getProv2TargetMappings(){
		return DeltaRuleGen.subtractFakeMappings(_prov2targetMappings);
	}	
	
	public synchronized List<Rule> getProv2TargetRules(IDb db){
		return DeltaRuleGen.subtractFakeRules(MappingsInversionMgt.splitMappingsHeads(_prov2targetMappings, db));	
	}
	
	public synchronized void setProv2TargetMappings (List<Mapping> p2t) {
		_prov2targetMappings = p2t;
	}
	
	public synchronized void setProv2TargetRules(List<Rule> p2t) {
		_prov2targetMappings = new ArrayList<Mapping>();
		_prov2targetMappings.addAll(p2t);
	}

	public synchronized List<Rule> getSource2ProvRules(){
		return DeltaRuleGen.subtractFakeRules(_source2provRules);
	}
	
	public synchronized void setSource2ProvRules(List<Rule> rules){
		_source2provRules = rules;
	}
	
	public synchronized void setLocal2PeerRules(List<Rule> local){
		if(_local2PeerMappings == null){
			_local2PeerMappings = new ArrayList<Rule>();
		}
		_local2PeerMappings.addAll(local);
	}

	public synchronized List<Rule> getLocal2PeerRules(){
		return _local2PeerMappings;
	}
	
	/**
	 * From the set of rules extract the list of EDB and IDB relations
	 * Add the result to the _edb and _idb attributes 
	 * @param rules Rules from which to extract edb/idb relations
	 */
	protected synchronized void extractEdbsIdbs (List<Mapping> rules, List<RelationContext> rels,
			List<RelationContext> edbs, List<RelationContext> idbs)
	{
		// In this map a relation viewed in at least one rule head has value false, true otherwise
		Map<AbstractRelation, Boolean> edbsMap = new HashMap<AbstractRelation, Boolean> ();
		Map<AbstractRelation, RelationContext> contextMap = new HashMap<AbstractRelation, RelationContext> ();
//		edbs.clear();
//		idbs.clear();
		
		for (Mapping r : rules)
		{
			for(Atom ma : r.getMappingHead()){
				if(!ma.isSkolem() && !_mappingRels.contains(ma.getRelationContext())){
					edbsMap.put(ma.getRelation(), false);
					contextMap.put(ma.getRelation(), ma.getRelationContext());
				}
			}
			
			for (Atom atom : r.getBody()){
				if(!atom.isSkolem() && !_mappingRels.contains(atom.getRelationContext())){
					if (!edbsMap.containsKey(atom.getRelation()))
					{
						edbsMap.put(atom.getRelation(), true);
						contextMap.put(atom.getRelation(), atom.getRelationContext());
					}
				}
			}
		}
	
		for(RelationContext rc : rels){
			AbstractRelation r = rc.getRelation();
			if(!edbsMap.containsKey(r)){
				edbsMap.put(r, true);
				contextMap.put(r, rc);
			}
		}
	
		// Extract the list of relations for which the map says it's an EDB relation
		for (Map.Entry<AbstractRelation, Boolean> entry : edbsMap.entrySet())
			if (entry.getValue().booleanValue()){
				if(!entry.getKey().getName().endsWith(Relation.REJECT))
					edbs.add (contextMap.get(entry.getKey()));
			}else{
				idbs.add (contextMap.get(entry.getKey()));
			}
	}
	
//	protected void extractEdbsIdbs (List<ScMapping> rules, List<RelationContext> rels,
//			List<RelationContext> edbs, List<RelationContext> idbs)
//	{
//		// In this map a relation viewed in at least one rule head has value false, true otherwise
//		Map<TableSchema, Boolean> edbsMap = new HashMap<TableSchema, Boolean> ();
//		Map<TableSchema, RelationContext> contextMap = new HashMap<TableSchema, RelationContext> ();
////		edbs.clear();
////		idbs.clear();
//		
//		for (ScMapping r : rules)
//		{
//			for(ScMappingAtom ma : r.getMappingHead()){
//				edbsMap.put(ma.getRelation(), false);
//				contextMap.put(ma.getRelation(), ma.getRelationContext());
//				
//				for (ScMappingAtom atom : r.getBody())
//					if (!edbsMap.containsKey(atom.getRelation()))
//					{
//						edbsMap.put(atom.getRelation(), true);
//						contextMap.put(atom.getRelation(), atom.getRelationContext());
//					}
//			}
//		}
//	
//		for(RelationContext rc : rels){
//			TableSchema r = rc.getRelation();
//			if(!edbsMap.containsKey(r)){
//				edbsMap.put(r, true);
//				contextMap.put(r, rc);
//			}
//		}
//	
//		// Extract the list of relations for which the map says it's an EDB relation
//		for (Map.Entry<TableSchema, Boolean> entry : edbsMap.entrySet())
//			if (entry.getValue().booleanValue()){
//				edbs.add (contextMap.get(entry.getKey()));
//			}else{
//				idbs.add (contextMap.get(entry.getKey()));
//			}
//	}
	
	public synchronized List<RelationContext> provenanceTablesForRelation(RelationContext rel){
		List<RelationContext> ret = new ArrayList<RelationContext>();

		if(getSource2ProvRules() != null){
			for(Mapping r : getProv2TargetMappings()){
				boolean foundRelInHead = false;
				for(Atom h : r.getMappingHead()){
					if(h.getRelationContext().equals(rel)){
						foundRelInHead = true;
					}
				}
				if(foundRelInHead){
					for(Atom a : r.getBody()){
						if(!a.isNeg() && !a.isSkolem()){
							if(!ret.contains(a.getRelationContext())){
								ret.add(a.getRelationContext());
							}
						}
					}
				}
			}
			return ret;
		}else{
			System.out.println("Translation Rules not computed yet!");
			return null;
		}
	}
	
	public RelationContext getProvenanceRelationForMapping(String mappingId) throws MappingNotFoundException
	{
		List<RelationContext> provRels = getMappingRelations();

		for(int j = 0; j < provRels.size(); j++){
			ProvenanceRelation provRel = (ProvenanceRelation)provRels.get(j).getRelation();
			if(ProvRelType.SINGLE.equals(provRel.getType())){
				Mapping m = provRel.getMappings().get(0);
				if(mappingId.equals(m.getId())){
					return provRels.get(j);
				}
			}
		}
		throw new MappingNotFoundException(mappingId);
	}
	
}
