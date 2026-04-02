package edu.upenn.cis.orchestra.mappings;

import java.util.Map;


import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.mappings.exceptions.CompositionException;

/**
 * 
 * @author gkarvoun
 *
 */
public class RuleEqualityAtom
{
	private AtomArgument _val1, _val2;
	
	public RuleEqualityAtom (AtomArgument val1, AtomArgument val2)
				throws CompositionException
	{
		if (val1.couldBeSubstitutedWith(val2))
		{
			_val1 = val1;
			_val2 = val2;
		} else 
		{
			if (val2.couldBeSubstitutedWith(val1))
			{
				_val1 = val2;
				_val2 = val1;
			}
			else
				throw new CompositionException ("Incompatible values: " + val1.toString() + " // " + val2.toString());
		}
		
	}
	
	public AtomArgument getVal1 ()
	{
		return _val1;
	}
	
	public AtomArgument getVal2 ()
	{
		return _val2;
	}
	
	
	public AtomArgument[] getValues ()
	{
		return new AtomArgument[]{getVal1(),getVal2()};
	}
	
	/**
	 * Get a fresh version of this equality atom, that is a version whose variables 
	 * are renamed so that they are not mixed up with original variables composition
	 * @param freshenVars old/new names for already freshen variables
	 * @param asNewObject If true then a new freshen eq atom will be returned, if false this atom will be
	 * 					freshen and then returned 
	 * @return Freshen atom
	 */
	public RuleEqualityAtom fresh (Map<String,String> freshenVars, boolean asNewObject)
	{
		RuleEqualityAtom res=null;
		AtomArgument val1 =  getVal1().fresh(freshenVars, asNewObject);
		AtomArgument val2 =  getVal2().fresh(freshenVars, asNewObject);
		
		if (asNewObject)
		{
			try
			{
				res = new RuleEqualityAtom(val1, val2);
			} catch (CompositionException ex)
			{
				// Cannot happen during a copy!
				assert false : "CompositionException should not be raised while freshening vars!";
			}
		}
		else
		{
			_val1 = val1;
			_val2 = val2;
			res = this;
		}
		return res;
	}
	
	@Override
	public String toString ()
	{
		StringBuffer buffer = new StringBuffer ();
		buffer.append(getVal1().toString());
		buffer.append ("=");
		buffer.append(getVal2().toString());
		return buffer.toString();
	}
}
