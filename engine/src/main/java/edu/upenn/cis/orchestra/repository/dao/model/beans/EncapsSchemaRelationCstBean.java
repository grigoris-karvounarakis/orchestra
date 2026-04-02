package edu.upenn.cis.orchestra.repository.dao.model.beans;

import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScConstraintBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScRelationBean;
import edu.upenn.cis.orchestra.repository.model.beans.SchemaBean;

public class EncapsSchemaRelationCstBean extends EncapsSchemaRelationBean {
	private ScConstraintBean _constraint;
	
	public EncapsSchemaRelationCstBean ()
	{}
	
	public EncapsSchemaRelationCstBean (PeerBean peer,			
										SchemaBean sch, 
										ScRelationBean rel,
										ScConstraintBean cst)
	{
		super (peer, sch, rel);
		_constraint = cst;
	}

	public ScConstraintBean getConstraint() {
		return _constraint;
	}

	public void setConstraint(ScConstraintBean constraint) {
		this._constraint = constraint;
	}
	
	
	
}
