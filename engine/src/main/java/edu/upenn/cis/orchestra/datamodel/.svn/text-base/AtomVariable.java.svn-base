package edu.upenn.cis.orchestra.datamodel;




import java.util.Map;

import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomValueBean;

/****************************************************************
 * Mapping definition : Mapping atom values // variable
 * Note: Vars do not have to be the same objects in the head and 
 * body, the <code>equals</code> method will be used 

 * @author Olivier Biton
 *****************************************************************
 */
public class AtomVariable extends AtomArgument {

	/** Variable name **/
	private String _name;
	/** Static variable used to freshen vars **/
	private static long _nextFreshId=1;
	
	private Atom _skolemDef = null;
	
	private boolean _existential = false;
	
	/**
	 * Creates a new atom value of type variable
	 * @param name Variable name
	 */
	public AtomVariable (String name)
	{
		if (name.equals("_")){
			_name = Mapping.getFreshAutogenVariableName();
			_existential = true;
		}else
			_name = name;
		_skolemDef = null;
	}

	/**
	 * Creates a deep copy of an atom value of type variable. <BR>
	 * To benefit from polymorphism, use <code>deepCopy()</code>
	 * @param var Variable to copy
	 * @see AtomVariable#deepCopy()
	 */	
	protected AtomVariable (AtomVariable var)
	{
		super (var);
		_name = var.getName();
		_skolemDef = var.skolemDef();
		_existential = var.isExistential();
	}
	
	/**
	 * Creates a new atom value of type variable from its bean
	 * representation
	 * @param bean Bean to create from
	 */
	public AtomVariable (ScMappingAtomValueBean bean)
	{
		_name = bean.getId();
		_skolemDef = null;
	}
	
	public synchronized String getName() {
		return _name;
	}

	public synchronized void setName (String name)
	{
		_name = name;
	}
	
	
	
	/**
	 * True if the other atom value is also a variable and has the same name
	 * @return True if variables are equivalent
	 */
	public synchronized boolean equals(AtomArgument atomVal) {
		if (atomVal instanceof AtomVariable)
			return // super.equals(atomVal) && 
			       _name.equals(((AtomVariable) atomVal).getName());
		else
			return false;
	}


	public int hashCode() {
		return _name.hashCode();
	}
	
	/**
	 * Get string representation
	 * @return String representation 
	 */
	public synchronized String toString ()
	{
		return getName();
	}

	/**
	 * Get a deep copy
	 * @return Deep copy
	 */
	public synchronized AtomArgument deepCopy() {
		return new AtomVariable(this);
	}

	/**
	 * Create a bean representation of this object
	 * @return Bean representation
	 */
	public synchronized ScMappingAtomValueBean toBean() {
		ScMappingAtomValueBean bean = new ScMappingAtomValueBean();
		//BeanUtils.copyProperties(this, bean);
		bean.setId(getName());
		bean.setType("V");
		return bean;
	}	
	
	
	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition. <BR>
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen variable will be returned, if false this variable will be
	 * 					freshen and then returned 
	 * @return Freshen variable
	 */
	public synchronized AtomArgument fresh (Map<String,String> freshenVars, boolean asNewObject)
	{
		if (freshenVars.containsKey(getName()))
		{
			if (asNewObject)
				return new AtomVariable (freshenVars.get(getName()));
			else
			{
				setName(freshenVars.get(getName()));
				return this;
			}
		}
		else
		{
			String newVarName = getName() + (_nextFreshId++);
			setExistential(true);
			freshenVars.put (getName(), newVarName);
			if (asNewObject)
			{
				AtomVariable var = new AtomVariable (newVarName);
				return var;
			}
			else
			{
				_name = newVarName;
				return this;
			}
		}
		
	}

	/**
	 * Check if this value could be replaced by another value during composition process
	 * Basically a variable can be substituted with anything: variable, constant, skolem... 
	 * @param val Value that would be substituted to this value
	 * @return True is the substitution is "valid" (compatible)
	 */
	public boolean couldBeSubstitutedWith (AtomArgument val)
	{
		/*		 
		boolean res = false;
		res = (val instanceof ScMappingAtomValVariable);
		return res;
		*/
		return true;
	}		

	/**
	 * If the atom value contains sub atoms values (eg Skolem), substitute
	 * all occurences of <code>oldVal</code> with <code>newVal</code>
	 * @param oldVal Value to be replaced
	 * @param newVal New value
	 */
	public void substitute (AtomArgument oldVal, AtomArgument newVal)
	{
		
	}
	
	public synchronized void renameVariable (String extension)
	{
		_name = _name + extension;
	}
	
	/*
	 * If this variable is existentially quantified in the head of a mapping,
	 * this field points to the "Skolem Atom" we are creating for it
	 */
	public synchronized boolean isSkolem(){
		return (_skolemDef != null);
	}

	
	
	public synchronized Atom skolemDef(){
		return _skolemDef;
	}

	public synchronized void setSkolemDef(Atom skolemDef){
		_skolemDef = skolemDef;
	}

	public synchronized boolean isExistential(){
		return _existential;
	}
	
	public synchronized void setExistential(boolean ex){
		_existential = ex;
	}

}
