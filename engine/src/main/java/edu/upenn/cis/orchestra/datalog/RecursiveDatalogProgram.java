package edu.upenn.cis.orchestra.datalog;

import java.util.List;

import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * 
 * @author gkarvoun
 *
 */
public class RecursiveDatalogProgram extends DatalogProgram {
	public RecursiveDatalogProgram(List<Rule> r, boolean c4f){
		super(r, c4f);
	}
	
	public RecursiveDatalogProgram(List<Rule> r){
		super(r);
	}

	public String toString ()
	{		
		return "Recursive Datalog Program { \n" + super.toString() + "} END Recursive Datalog Program\n";
	}


}
