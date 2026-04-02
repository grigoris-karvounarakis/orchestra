package edu.upenn.cis.orchestra.datamodel;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.springframework.beans.BeanUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.mappings.RuleEqualityAtom;
import edu.upenn.cis.orchestra.mappings.exceptions.CompositionException;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomValSkolemBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomValueBean;
import edu.upenn.cis.orchestra.util.Holder;
import edu.upenn.cis.orchestra.util.PositionedString;
import edu.upenn.cis.orchestra.util.XMLParseException;


/****************************************************************
 * Mapping definition : Mapping atom
 * @author Olivier Biton
 *****************************************************************
 */
public class Atom {


	/**
	 * Relation referenced by this atom
	 */
	private RelationContext _relationContext;
	/**
	 * List of values (variables, skolems...) 
	 */
	private List<AtomArgument> _values = new Vector<AtomArgument> ();	
	
	/**
	 * Atom type used for delta rules. When a rule is not a delta rule, type will be AtomType.NONE
	 */
	
	/**
	 * Is this atom negated
	 */
	private boolean _neg=false;
	
	/**
	 * This atom is part of a bidirectional mapping and should be deleted as part 
	 * of "backward" propagation of deletions
	 */
	private boolean _del=false;
	
	private List<AtomArgument> _skolemKeyValues = null;
	
	private boolean _allStrata;
	
	public enum AtomType 
				{
					NONE,  // ALL
//					OLD,
					NEW,   // 
					INS,   // ALL
					DEL,   // ALL
					RCH,   // Only IDBs
					INV,   // ALL
					ALLDEL, // Not used in STRATIFIED deletions
//					BCK, // Backup
					D // Results of update policy
				};
				
	/**
	 * Atom type used for delta rules. When a rule is not a delta rule, type will be AtomType.NONE
	 */			
	protected AtomType _type;

	
	//TODO: Check if peer/schema/relation coherent
	//TODO: Check if nb values = nb fields in relation
	
	/**
	 * Creates a new Mapping atom
	 * @param peer Peer containing the schema/relation referenced by this atom
	 * @param schema Schema containing the relation referenced by this atom
	 * @param relation Relation referenced by this atom
	 * @param values List of values (variables, skolems...) 
	 */
	public Atom (Peer peer, Schema schema, Relation relation, List<AtomArgument> values)
	{
		this (new RelationContext (relation, schema, peer, false), values);
	}
	
	/**
	 * Creates a new Mapping atom
	 * @param relationContext Relation referenced by this atom with its full context
	 * @param values List of values (variables, skolems...) 
	 */
	public Atom (RelationContext relationContext, List<AtomArgument> values, AtomType type)
	{
		_relationContext = relationContext;
		_values.addAll(values);
		_type = type;
		_allStrata = true;
		_neg = false;
		_skolemKeyValues = null;
		setValuesTypes();
	}	
	
	private void setValuesTypes ()
	{
		int realSize;
		realSize = getRelation().getFields().size();
		if(Config.getEdbbits())
			realSize -= 1;
		
		for (int i = 0 ; i < realSize ; i++)
		{
			getValues().get(i).setType(getRelation().getField(i).getType());
		}
	}
	
	public Atom (RelationContext relationContext, List<AtomArgument> values)
	{
		this(relationContext, values, AtomType.NONE);
	}	

	
	/**
	 * Creates a deep copy of a given atom. <BR>
	 * To benefit from polymorphism, use method <code>deepCopy (OrchestraSystem)</code>
	 * @param atom Atom to deep copy
	 * @param system Deep copy of the system containing the original mapping. 
	 * 				 Used to get find the peer/schema/relation referenced by the new mapping atom.
	 * @see Atom#deepCopy(OrchestraSystem) 
	 * @see Atom#deepCopy() 
	 */
	protected Atom (Atom atom, OrchestraSystem system)
	{
		Peer peer = system.getPeer(atom.getPeer().getId());
		Schema schema = peer.getSchema(atom.getSchema().getSchemaId());
		Relation relation = null;
		try{
			relation = schema.getRelation(atom.getRelation().getName());
		}catch(RelationNotFoundException e){
			e.printStackTrace();
		}
		_relationContext = new RelationContext (relation, schema, peer, false);
		if(atom.getSkolemKeyVals() == null)
			_skolemKeyValues = null;
		else
			_skolemKeyValues = new ArrayList<AtomArgument>();		
		
		for (AtomArgument val : atom.getValues()){
			AtomArgument vc = val.deepCopy();
			_values.add (vc);
			
			if(atom.getSkolemKeyVals() != null	&& atom.getSkolemKeyVals().contains(val))
				_skolemKeyValues.add (vc);
		}
		
		_type = AtomType.NONE;
		_neg = atom.isNeg();
		_del = atom.getDel();
		_allStrata = atom.allStrata();
		setValuesTypes();
		
	}
	
	/**
	 * Creates a deep copy of a givem atom but keeping a reference to the same relation. <BR>
	 * If the full system is being copied, call deepCopy (OrchestraSystem) to
	 * update the relations references.<BR>
	 * To benefit from polymorphism, use method <code>deepCopy ()</code>
	 * @param atom Atom to deep copy
	 * @see Atom#deepCopy(OrchestraSystem)
	 * @see Atom#deepCopy()
	 */
	public Atom (Atom atom)
	{
//		this(atom, atom.getValues(), AtomType.NONE);
		this(atom, atom.getValues(), atom.getType());
	}

	//TODO Exception if peer ref or rel unknown

	
	/**
	 * Creates a new mapping Atom as a copy of <code>atom</code>, with a new set of values 
	 * @param atom Atom from which basic properties (everything but values) must be copied
	 * @param newValues New values
	 */
	public Atom (Atom atom, List<AtomArgument> newValues, AtomType type)
	{
		Peer peer = atom.getPeer();
		Schema schema = atom.getSchema();
		Relation relation = atom.getRelation();
		boolean mapping = atom.isMapping();
		_relationContext = new RelationContext (relation, schema, peer, mapping);
		_skolemKeyValues = new ArrayList<AtomArgument>();
		if(atom.getSkolemKeyVals() == null)
			_skolemKeyValues = null;
		else
			_skolemKeyValues = new ArrayList<AtomArgument>();		
		
		for (AtomArgument val : newValues){
			AtomArgument vc = val.deepCopy();
			_values.add (vc);
			
			if(atom.getSkolemKeyVals() != null	&& atom.getSkolemKeyVals().contains(val))
				_skolemKeyValues.add (vc);
		}
		
		_type = type;
		_neg = atom.isNeg();
		_del = atom.getDel();
		_allStrata = atom.allStrata();
		
		setValuesTypes();
		
	}
	
	public Atom (Atom atom, List<AtomArgument> newValues)
	{
//		this(atom, newValues, AtomType.NONE);
		this(atom, newValues, atom.getType());
	}
	
	public Atom (Atom atom, AtomType type)
	{
		this(atom, atom.getValues(), type);
	}
	
	
	/**
	 * Creates a new mapping atom from its bean description 
	 * @param bean Bean describing the atom
	 * @param system System created from the system beam containing the mapping bean.
	 * 				 Used to get find the peer/schema/relation referenced by the new mapping atom.
	 * @throws InvalidBeanException If a bean property (value type for example) is incorrect
	 */
	public Atom (ScMappingAtomBean bean, OrchestraSystem system)
				throws InvalidBeanException
	{		
		Peer peer = system.getPeer(bean.getPeerId());
		Schema schema = peer.getSchema(bean.getSchemaId());
		Relation relation = null;
		try{
			relation = schema.getRelation(bean.getRelationId());
		}catch(RelationNotFoundException e){
			e.printStackTrace();
		}

		_relationContext = new RelationContext (relation, schema, peer, false);
		
		for (ScMappingAtomValueBean value : bean.getValues())		
			_values.add (atomValueFromValueBean(value));
		_type = AtomType.NONE;
		_skolemKeyValues = null;
		_neg = false;
		_allStrata = true;
		
		setValuesTypes();
	}	


	public static String typeToSuffix(AtomType type){
		if(type == AtomType.NONE){
			return "";
		}else{
			return "_" + type.toString();
		}
	}
	
	public static AtomType getSuffix(String name) {
		int index = name.lastIndexOf("_");
		if (index != -1) {
			String suffix = name.substring(index+1);
			for (AtomType a : AtomType.values()) {
				if (suffix.equals(a.toString())) {
					return a;
				}
			}
		}
		return AtomType.NONE;
	}
	
	public static String getPrefix(String name) {
		AtomType a = getSuffix(name);
		if (a == AtomType.NONE) {
			return name;
		} else {
			return name.substring(0, name.length() - a.toString().length() - 1);
		}
	}

	public static String typeToString(AtomType type){
		if(type == AtomType.NONE){
			return "";
		}else{
			return type.toString();
		}
	}	
	
	public synchronized AtomType getType() 
	{
		return _type;
	}

	public synchronized void setType(AtomType type) 
	{
		_type = type;
	}
	
	public static AtomArgument atomValueFromValueBean (ScMappingAtomValueBean value)
		throws InvalidBeanException
	{
		if (value.getType().equals("C"))
			return (new AtomConst(value));
		else
			if (value.getType().equals("V"))
				return (new AtomVariable(value));
			else
				if (value.getType().equals("S") && value instanceof ScMappingAtomValSkolemBean)
					return (new AtomSkolem((ScMappingAtomValSkolemBean) value));
				else
					throw new InvalidBeanException (value, "Invalid mapping value type (or invalid bean object): " + value.getType());
		
	}
	
	/**
	 * Get the peer containing the schema/relation referenced by the mapping atom
	 * @return Peer referenced
	 */	
	public Peer getPeer() {
		return _relationContext.getPeer();
	}
	
	/**
	 * Get the schema containing the relation referenced by the mapping atom
	 * @return Schema referenced
	 */	
	public Schema getSchema() {
		return _relationContext.getSchema();
	}


	/**
	 * Check whether the relation for this atom is a mapping relation
	 * @return true, if relation is mapping relation, false otherwise
	 */	
	public boolean isMapping ()
	{
		return _relationContext.isMapping();
	}


	/**
	 * Get the relation referenced by the mapping atom
	 * @return Relation referenced
	 */	
	public Relation getRelation ()
	{
		return _relationContext.getRelation();		
	}
	
	/**
	 * Get the relation with it's full context (relation's schema and peer)
	 * @return
	 */
	public RelationContext getRelationContext ()
	{
		return _relationContext;
	}
	
	/** 
	 * Get the list of values for this mapping atom
	 * @return List of atom values
	 */
	public List<AtomArgument> getValues ()
	{
		return _values;
	}
	
	/** 
	 * Get the list of values for this mapping atom
	 * @return List of atom values
	 */
	public List<AtomVariable> getVariables ()
	{
		List<AtomVariable> l = new ArrayList<AtomVariable>();
		for(AtomArgument v : _values){
			if(v instanceof AtomVariable)
			l.add((AtomVariable)v);
		}
		return l;
	}
	
	public synchronized void setAllStrata ()
	{
		_allStrata = true;
	}

	public synchronized boolean allStrata ()
	{
		return _allStrata;
	}
	
	public synchronized void setNeg (boolean neg)
	{
		_neg = neg;
	}
	
	public void negate ()
	{
		setNeg(true);
	}
	
	public synchronized boolean isNeg ()
	{
		return _neg;
	}
	
	public synchronized void setDel (boolean del)
	{
		_del = del;
	}

	public synchronized boolean getDel ()
	{
		return _del;
	}
	
	public synchronized void setSkolemKeyVals (List<AtomArgument> vals)
	{
		_skolemKeyValues = vals;
	}

	public synchronized boolean isSkolem ()
	{
		return _skolemKeyValues != null;
	}
	
	public synchronized List<AtomArgument> getSkolemKeyVals(){
		return _skolemKeyValues;
	}

	public synchronized void deskolemizeAllVars(){
		for(AtomVariable var : getVariables()){
			if(var.isSkolem()){
				var.setSkolemDef(null);
			}
		}
	}
	
	/**
	 * Get a deep copy of the current atom when the whole system is being deep copied
	 * @param system Deep copy of the system containing the mapping.
	 * 				 Used to get find the peer/schema/relation referenced by the new mapping atom.
	 * @return Deep copy
	 * @see Atom#ScMappingAtom(Atom, OrchestraSystem)
	 */
	public synchronized Atom deepCopy (OrchestraSystem system)
	{
		return new Atom(this, system);
	}

	/**
	 * Get a deep copy of the current atom, but keeps references to the same relation.<BR>
	 * If the whole system is being deep copied, then call deepCopy(OrchestraSystem) to update
	 * the references to the relation (to use the new one)
	 * @return Deep copy
	 * @see Atom#ScMappingAtom(Atom)
	 * @see Atom#deepCopy(OrchestraSystem)
	 */
	public synchronized Atom deepCopy ()
	{
		return new Atom(this);
	}	

	/**
	 * Creates a bean representation of the atom.
	 * @param atomPosition Atom rank in the list of atoms
	 * @return Bean representing the atom
	 */
	protected synchronized ScMappingAtomBean toBean (int atomPosition)
	{
		ScMappingAtomBean bean = new ScMappingAtomBean ();
		BeanUtils.copyProperties(this, bean, new String[] {"relation", "values"});

		bean.setPeerId(getPeer().getId());
		bean.setSchemaId(getSchema().getSchemaId());
		bean.setRelationId(getRelation().getName());
		bean.setAtomPosition(atomPosition);
		

		List<ScMappingAtomValueBean> values = new ArrayList<ScMappingAtomValueBean> ();
		for (AtomArgument val : getValues())
			values.add(val.toBean());
		bean.setValues(values);
		
		return bean;
	}


   /**
    * Returns a description of the mapping, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Mapping description
    */
	
   /**
    * Returns a description of the mapping, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Mapping description Peer.Schema.RelName(variables)
    */
	@Override
	public synchronized String toString() {
		// TODO Auto-generated method stub
		
		StringBuffer buff = new StringBuffer ();
		buff.append(_relationContext.toString() + "(");
		boolean first = true;
		for (AtomArgument value : getValues())
		{
			buff.append((first?"":",") + value.toString());
			first = false;
		}
		buff.append (")");
		String strRes = buff.toString();
		
		if (getType() != AtomType.NONE)
			strRes = getType().toString() + "_" + strRes;
		if (isNeg())
			strRes = "not(" + strRes + ")";
		return strRes;
	}
		
	/*
	 * 	 @return RelName(variables) - not used anywhere
	 */	
	public synchronized String toString2() {
		// TODO Auto-generated method stub
		StringBuffer buff = new StringBuffer ();
		buff.append(getRelation().getName());
		if (getType() != AtomType.NONE)
			buff.append("_" + getType().toString());
		buff.append("(");
		boolean first = true;
		for (AtomArgument value : getValues())
		{
			buff.append ((first?"":",") + value.toString());
			first = false;
		}

		buff.append(")");
		String strRes = buff.toString();
		if (isNeg())
			strRes = "not(" + strRes + ")";
		
		return strRes;
	}
		
/*
 *  @return Schema.RelName only
 */
	public synchronized String toString3() {
		// TODO Auto-generated method stub
		String strRes = getRelation().getFullQualifiedDbId();
		if (getType() != AtomType.NONE)
			strRes = strRes + "_" + getType().toString();
		return strRes;
	}
	
/*
 * 	 @return RelName only
 */	
	public synchronized String toString4() {
		// TODO Auto-generated method stub
		String strRes = getRelation().getName();
		if (getType() != AtomType.NONE)
			strRes = strRes + "_" + getType().toString();
		return strRes;
	}

	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen atom will be returned, if false this atom will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public synchronized Atom fresh(Map<String, String> freshenVars, boolean asNewObject) {
		Atom atom;

		List<AtomArgument> newValues = new ArrayList<AtomArgument> ();
		for (AtomArgument val : getValues())
			newValues.add(val.fresh(freshenVars, asNewObject));
		
		if (asNewObject){
			atom = new Atom(this, newValues);

			atom.setType(getType());
			atom.setNeg(isNeg());
			atom.setDel(getDel());
			if(allStrata())
				atom.setAllStrata();
			return atom;
		}else{
			_values = newValues;

			return this;
		}
	}	

	
	/**
	 * Substitute all occurences of <code>valInit</code> with <code>valRepl</code>
	 * in the atom values
	 * @param oldVal Value to be replaced
	 * @param newVal New value
	 */	
	public synchronized void substitute (AtomArgument oldVal, 
								AtomArgument newVal)
	{
		for (int i = 0 ; i < _values.size() ; i++)
		{
			AtomArgument value = _values.get(i);
			// If the atom value is to be replaced ...
			if (value.equals(oldVal) && oldVal.equals(value))
			{
				_values.remove(i);
				_values.add(i, newVal);
			}
			// Else replace any occurence of oldVal inside the atom sub-atoms if any
			else
				value.substitute(oldVal, newVal);
		}
	}
	
	protected synchronized void renameVariables (String extension)
	{
		for (AtomArgument val : getValues())
			val.renameVariable(extension);
	}

	public synchronized Map<String, AtomArgument> varHomomorphism(Atom other){
		HashMap<String, AtomArgument> varmap = new HashMap<String, AtomArgument> ();
		if(getRelationContext().equals(other.getRelationContext())){
			for(int i = 0; i < getValues().size(); i++){
				AtomArgument v = getValues().get(i);
				if(v instanceof AtomVariable){
					AtomVariable var = (AtomVariable)v;
					varmap.put(var.getName(), other.getValues().get(i));
				}else if(v instanceof AtomConst){
					AtomConst c = (AtomConst)v;
					AtomArgument v2 = other.getValues().get(i);
					if(v2 instanceof AtomConst){
						AtomConst c2 =(AtomConst)v2;
						if(!c2.equals(c)){
							return null;
						}
					}else{
						return null;
					}
				}else{
					return null;
				}
			}
			return varmap;
		}else{
			return null;
		}
	}
	
	public synchronized List<RuleEqualityAtom> varHomomorphismEq(Atom other) {
		List<RuleEqualityAtom> ret = new ArrayList<RuleEqualityAtom>();
		try{
		if(getRelationContext().equals(other.getRelationContext())){
			for(int i = 0; i < getValues().size(); i++){
				AtomArgument v = getValues().get(i);
				if(v instanceof AtomVariable){
					AtomVariable var = (AtomVariable)v;
					ret.add(new RuleEqualityAtom(var, other.getValues().get(i)));
				}else if(v instanceof AtomConst){
					AtomConst c = (AtomConst)v;
					AtomArgument v2 = other.getValues().get(i);
					if(v2 instanceof AtomConst){
						AtomConst c2 =(AtomConst)v2;
						if(!c2.equals(c)){
							return null;
						}
					}else{ // not quite a homomorphism, but necessary for unfolding of idbs
						ret.add(new RuleEqualityAtom(v, v2));
					}
				}else{
					return null;
				}
			}
			return ret;
		}else{
			return null;
		}
		}catch(CompositionException e){
			e.printStackTrace();
			return null;
		}
	}

	
	public synchronized void substituteVars(Map<String, AtomArgument> varmap){
    	
    	for(int i = 0; i < getValues().size(); i++){
    		AtomArgument v = getValues().get(i);
    	
    		if(v instanceof AtomVariable){
    			AtomVariable var = (AtomVariable)v;
    			if(varmap.containsKey(var.getName())){
    				getValues().remove(i);
    				getValues().add(i, varmap.get(var.getName()));
    			}
    		}else if (v instanceof AtomConst){ 
    			
    		} else { // Should do smth else for skolems 
    			throw new RuntimeException("Cannot substitute non-variable: " + v.toString());
    			//System.out.println(1/0);
    		}
    	}
    }
    
    public synchronized boolean samePattern(Atom other){
    	if((!getRelationContext().equals(other.getRelationContext())) || (getValues().size() != other.getValues().size()))
    		return false;	
    	
    	for(int i = 0; i < getValues().size(); i++) {
    		AtomArgument v = getValues().get(i);
    		AtomArgument ov = other.getValues().get(i);
    		
    		if(((v instanceof AtomVariable) && !(ov instanceof AtomVariable)) ||
    		   ((v instanceof AtomConst) && !(ov instanceof AtomConst))) {
    			return false;
    		} else if ((v instanceof AtomConst) && (ov instanceof AtomConst)) {
    			AtomConst vc = (AtomConst)v;
    			AtomConst ovc = (AtomConst)ov;
    			
    			if(!vc.equals(ovc))
    				return false;
    		} // else Skolem?
    	}
    	
    	return true;
    }
    
    public synchronized void renameExistentialVars(Mapping r){
    	for(AtomArgument val : getValues()){
    		if(val instanceof AtomVariable){
    			if(!r.isDistinguished((AtomVariable) val)){
    				((AtomVariable) val).setExistential(true);
    				((AtomVariable) val).setName(Mapping.getFreshAutogenVariableName());
    			}
    		}else if(val instanceof AtomConst){
    			// do nothing?
   			}else{
    			//System.out.println(1/0);
        		throw new RuntimeException("Cannot rename non-variable: " + val.toString());
    		}
    	}
    }
    
    public synchronized void serialize(Document doc, Element atom) {
    	StringBuffer buf = new StringBuffer(_relationContext.toString());
    	buf.append("(");
    	boolean first = true;
    	for (AtomArgument value : _values) {
    		if (first) {
    			first = false;
    		} else {
    			buf.append(", ");
    		}
    		buf.append(value.toString());
    	}
    	buf.append(")");
    	atom.setTextContent(buf.toString());
    }
    
    public static Atom deserialize(OrchestraSystem catalog, Element atom, Holder<Integer> counter) throws XMLParseException {
    	PositionedString str = new PositionedString(atom.getTextContent());
    	try {
    		UntypedAtom u = UntypedAtom.parse(str, counter);
    		return u.getTyped(catalog);
    	} catch (ParseException e) {
    		throw new XMLParseException(e);
    	} catch (RelationNotFoundException e) {
    		throw new XMLParseException(e);
    	} catch (Exception e) {
    		e.printStackTrace();
    		throw new XMLParseException("Error deserializing" + str.getString() + ": " + e.getMessage());
    	}
    }

//    public int hashCode() {
//    	int ret = this.toString().hashCode();
//    	return ret;
//    }
    
    public boolean equals(Object oth){
    	Atom other;
    	if(oth instanceof Atom)
    		other = (Atom)oth;
    	else
    		return false;

    	if(!this._relationContext.equals(other._relationContext))
    		return false;
    	List<AtomArgument> thisVals = getValues();
    	List<AtomArgument> otherVals = other.getValues();

    	if(thisVals.size() != otherVals.size())
    		return false;

    	for(int i = 0; i < thisVals.size(); i++){
    		if(!thisVals.get(i).equals(otherVals.get(i)))
    			return false;
    	}
    	
    	return true;
    }
    
    public boolean hasExistentialVariables(Mapping r){
    	for(AtomVariable v : getVariables()){
        	boolean found = false;
    		for(AtomVariable var : r.getAllBodyVariablesInPosAtoms()){
       			if(var.equals(v))
        			found = true;
        	}
    		if(!found)
    			return true;
    	}
    	return false;
    }
    
    public void replaceRelationContext(RelationContext newCx) {
    	_relationContext = newCx;
    }
}
