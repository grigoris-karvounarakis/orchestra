package edu.upenn.cis.orchestra.datamodel;

import java.util.Map;

import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomValueBean;


/****************************************************************
 * Mapping definition : Mapping atom value

 * @author Olivier Biton
 *****************************************************************
 */
public abstract class AtomArgument 
{
	
	private Type _type=null;

	protected AtomArgument ()
	{
	}
	
	protected AtomArgument (AtomArgument atomVal)
	{
		_type = atomVal._type;
	}
	
	/**
	 * Get a String description of the atom value, conforms to the 
     * flat file format defined in <code>RepositoryDAO</code>
	 * @return Description
	 */
	public abstract String toString ();
	
	public boolean equals (Object obj)
	{
		if (obj instanceof AtomArgument)
			return equals ((AtomArgument) obj);
		else
			return false;
	}	
	
	/**
	 * True if the atom value has the same type, and is equivalent
	 * It will check for attributes equality but won't check on inheritance. Thus if the
	 * parameter extends the class it will be considered equals when the known attributes are 
	 * equal.
	 * It you want to avoid that when calling this method, call it on both elements (x.equals(y) && y.equals(x))
	 * @param atomVal Atom value to compare with
	 * @return True if equivalent
	 */
	public boolean equals (AtomArgument atomVal)
	{
		if (getType()!=null && atomVal.getType()!=null)
			//TODO: Stronger check? Pb is if we check for type equality it requires 
			// exact same type (same capacity...)
			return (getType().getClass().equals(atomVal.getType().getClass()));
		else
			return (getType()==null || atomVal.getType() == null);
		
	}
	
	public int hashCode() {
		return getType().getClass().hashCode();
	}
	
	/**
	 * Get a deep copy of the atom value
	 * @return Deep copy
	 */
	public abstract AtomArgument deepCopy (); 

	/**
	 * Get a bean representation of the atom value
	 * @return Bean representation
	 */
	public abstract ScMappingAtomValueBean toBean ();
	
	
	//TODO: Would be better to map actual Variable objects to reduce memory use...
	//TODO: Would be better to have an external object to get new indices for variables renaming. Using a static attribute will keep getting new indices even for different mappings composition calls...
	
	
	/**
	 * Get a fresh version of this atom value, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true the a new freshen variable will be returned, if false this variable will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public abstract AtomArgument fresh (Map<String,String> freshenVars, boolean asNewObject);
	
	
	/**
	 * Check if this value could be replaced by another value during composition process
	 * @param val Value that would be substituted to this value
	 * @return True is the substitution is "valid" (compatible)
	 */
	public abstract boolean couldBeSubstitutedWith (AtomArgument val);

	
	/**
	 * If the atom value contains sub atoms values (eg Skolem), substitute
	 * all occurences of <code>oldVal</code> with <code>newVal</code>
	 * @param oldVal Value to be replaced
	 * @param newVal New value
	 */
	public abstract void substitute (AtomArgument oldVal, AtomArgument newVal);
	
	
	public abstract void renameVariable (String extension);

	
	public void setType (Type type){
		_type = type;
	}
	
	public Type getType ()
	{
		return _type;
	}
	
	
}


