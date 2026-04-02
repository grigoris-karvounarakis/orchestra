package edu.upenn.cis.orchestra.exchange;

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.RelationField;

/**
 * Class used to define the mapping from a field in the head of a rule
 * to its source columns.
 * 
 * @author zives
 *
 */
public class RuleFieldMapping {
	public boolean isIndex;
	public RelationField outputField;
	public Mapping rule;
//	public List<String> srcColumns;
	public List<RelationField> srcColumns;
	public List<RelationField> trgColumns;
	public AtomArgument srcArg;

//	public RuleFieldMapping(RelationField f, List<String> src, List<String> trg, 
	public RuleFieldMapping(RelationField f, List<RelationField> src, List<RelationField> trg,
			AtomArgument arg, boolean inx, Mapping r) {
		outputField = f;
		srcColumns = src;
		trgColumns = trg;
		srcArg = arg;
		isIndex = inx;
		rule = r;
	}

	@Override
	public String toString() {
		return new String(outputField.getName() + " :- " + srcColumns.toString());
	}
}

