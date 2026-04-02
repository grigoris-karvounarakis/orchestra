package edu.upenn.cis.orchestra.repository.dao.model.beans;

import java.io.StringReader;
import java.util.List;


import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.FlatRepository;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.ParseException;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomValueBean;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.FlatRepository.StupidJavaDoesntHaveOutParameters;

public class ScMappingAtomTextBean extends ScMappingAtomBean {

	//TODO: Should be dealt with properly, does not have to be statuc...
	//private static int _nextVarInd = 0;
	private static FlatRepository.StupidJavaDoesntHaveOutParameters _nextVarInd = null;
	
	public ScMappingAtomTextBean() {
		super ();
	}

	public ScMappingAtomTextBean(Object beanSameProperties) {
		super(beanSameProperties);
	}
	
	public void setStrValues (String values)
	{
		if (values != null)
		{		
			StringReader reader = new StringReader (values);
			FlatRepository repos = new FlatRepository (reader);
			if (_nextVarInd==null)
				_nextVarInd = new StupidJavaDoesntHaveOutParameters();
			List<ScMappingAtomValueBean> vals=null;
			try
			{
				vals = repos.mappingAtomValuesList(_nextVarInd);
			} catch (ParseException ex)
			{
				//TODO
				assert(false);
			}
			setValues(vals);
		}
		
	}
	
	public String getStrValues ()
	{
		StringBuffer buff = new StringBuffer ();
		if (getValues() != null)
		{
			try
			{	
				boolean firstVal = true;
				for (ScMappingAtomValueBean value : getValues())
				{
					buff.append ((firstVal?"":",") + Atom.atomValueFromValueBean(value).toString());
					firstVal = false;
				}			
			}
			catch (InvalidBeanException ex)
			{
				//TODO
				assert(false);
			}
		}
		return buff.toString();
	}

}
