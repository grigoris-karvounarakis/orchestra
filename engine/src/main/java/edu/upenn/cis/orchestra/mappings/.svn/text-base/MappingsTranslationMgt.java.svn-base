package edu.upenn.cis.orchestra.mappings;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.AtomConst;
import edu.upenn.cis.orchestra.datamodel.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;

public class MappingsTranslationMgt {
	
	public static String edbBitName = "EDBBIT";

	public static List<Rule> computeSource2ProvMappingsForIns(List<Rule>source2provRules){
		List<Rule>source2provRulesForIns = new ArrayList<Rule>();

		if(source2provRules == null)
			return null;

		for(Rule s : source2provRules){
//			if(!Config.getOuterJoin()){
			if(Config.isWideProvenance() && s.getSkolemAtoms().size() != 0){
				Atom newHead = s.getHead().deepCopy();
				newHead.deskolemizeAllVars();

				List<Atom> newBody = s.copyBodyWithoutSkolems(); 
				newBody.addAll(s.getParentMapping().copyMappingHead());
				for(Atom a : newBody){
					a.deskolemizeAllVars();
				}
				Rule jr = new Rule(newHead, newBody, s.getParentMapping(), s.getDb());
				source2provRulesForIns.add(jr);
			}
//			}
			source2provRulesForIns.add(s.deepCopy());
		}
		return source2provRulesForIns;
	}

	public static List<Mapping> computeProv2TargetMappingsForIns(List<Mapping> prov2targetMappings){
		List<Mapping>prov2targetMappingsForIns = new ArrayList<Mapping>();

		if(prov2targetMappings == null)
			return null;

		int i = 0;
		for(Mapping t : prov2targetMappings){
//			if(!Config.getOuterJoin()){
			if(!Config.isWideProvenance() && t.getSkolemAtoms().size() != 0){
				List<Atom> newHead = t.copyMappingHead();
				for(Atom a : newHead){
					a.deskolemizeAllVars();
				}

				List<Atom> newBody = t.copyBodyWithoutSkolems();
				newBody.addAll(t.copyMappingHead());
				for(Atom a : newBody){
					a.deskolemizeAllVars();
				}

				Mapping proj2 = new Mapping("MH-PROJ"+i+"-B", "MH-PROJ"+i+"-A", true, 1, newHead, newBody);
				prov2targetMappingsForIns.add(proj2);
			}
//			}
			prov2targetMappingsForIns.add(t.deepCopy());
			i++;
		}

		return prov2targetMappingsForIns;
	}

	public static List<Rule> computeSource2ProvMappingsForProvQ(List<Rule>source2provRules){
		List<Rule>source2provRulesForProvQ = new ArrayList<Rule>();

		if(source2provRules == null)
			return null;

		for(Rule s : source2provRules){
			Atom newHead = s.getHead().deepCopy();
			newHead.deskolemizeAllVars();

			List<Atom> newBody = s.copyBodyWithoutSkolems(); 
			for(Atom a : newBody){
				a.deskolemizeAllVars();
			}
			Rule jr = new Rule(newHead, newBody, s.getParentMapping(), s.getDb());
			source2provRulesForProvQ.add(jr);
		}

		return source2provRulesForProvQ;
	}

	public static List<Mapping> computeProv2TargetMappingsForProvQ(List<Mapping> prov2targetMappings) {
		List<Mapping>prov2targetMappingsForProvQ = new ArrayList<Mapping>();

		if(prov2targetMappings == null)
			return null;

		int i = 0;
		for(Mapping t : prov2targetMappings){
			List<Atom> newHead = t.copyMappingHead();
			for(Atom a : newHead){
				a.deskolemizeAllVars();
				if(Config.getEdbbits()){
					for(int j = 0; j < a.getValues().size(); j++){
						if(a.getRelation().getField(j).getName().equals(edbBitName)){
							a.getValues().set(j, new AtomVariable(Mapping.getFreshAutogenVariableName()));
						}
					}
				}
			}

			List<Atom> newBody = t.copyBodyWithoutSkolems();
			for(Atom a : newBody){
				a.deskolemizeAllVars();
			}

			Mapping proj2 = new Mapping("MH-PROJ"+i+"-B", "MH-PROJ"+i+"-A", true, 1, newHead, newBody);
			proj2.setFakeMapping(t.isFakeMapping());
			prov2targetMappingsForProvQ.add(proj2);
			i++;
		}

		return prov2targetMappingsForProvQ;
	}

	/**
	 * Adds a Boolean attribute to all atoms.  For the head it is set by default to 0 indicating non-EDB.
	 * 
	 * @param mappings
	 */
	public static void addEdbBitsToMappings(List<Mapping> mappings){
		for(Mapping mapping : mappings){
			for(Atom a : mapping.getMappingHead()){
				AtomConst c = new AtomConst("0");
				c.setType(new IntType(false, true));
				a.getValues().add(c);
			}
			for(Atom a : mapping.getBody()){
				AtomVariable c = new AtomVariable(Mapping.getFreshAutogenVariableName());
				c.setType(new IntType(false, true));
				a.getValues().add(c);
			}
		}
	}

	/**
	 * Add edb bit to each peer relation
	 * @param _rels the peer relations
	 * @throws UnsupportedTypeException
	 */
	public static void addEdbBitToPeerRelations(List<RelationContext> _rels) throws UnsupportedTypeException {

		for(RelationContext rel : _rels){
			rel.getRelation().getFields().add(edbBitField());
		}
	}

	public static RelationField edbBitField() throws UnsupportedTypeException {
		IntType intType = new IntType(false, true);
		return new RelationField(edbBitName, 
				"bit indicating that this tuple is directly derived from an edb tuple",
				false, intType.getSQLTypeName());
	}


}
