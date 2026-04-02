package edu.upenn.cis.orchestra.repository.dao.model.beans;

import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScFieldBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScRelationBean;
import edu.upenn.cis.orchestra.repository.model.beans.SchemaBean;

public class EncapsSchemaRelationFieldBean extends EncapsSchemaRelationBean 
{
	private ScFieldBean _field;

	public EncapsSchemaRelationFieldBean ()
	{
		
	}
	
	public EncapsSchemaRelationFieldBean (PeerBean peer, SchemaBean sch, ScRelationBean rel, ScFieldBean fld)
	{
		super (peer, sch, rel);
		_field = fld;
	}
	
	public ScFieldBean getField() {
		return _field;
	}

	public void setField(ScFieldBean field) {
		this._field = field;
	}
	
	
}
