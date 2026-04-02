package edu.upenn.cis.orchestra.repository.dao.model.beans;

import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScConstraintBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScRelationBean;
import edu.upenn.cis.orchestra.repository.model.beans.SchemaBean;

public class EncapsSchemaRelationCstFieldBean 
		extends EncapsSchemaRelationCstBean
{
	private String _field;
	private int _position;
	
	public EncapsSchemaRelationCstFieldBean ()
	{
		
	}
	
	public EncapsSchemaRelationCstFieldBean (PeerBean peer,
												SchemaBean sch,
												ScRelationBean rel,
												ScConstraintBean cst,
												String field,
												int pos)
	{
		super (peer, sch, rel, cst);
		_field = field;
		_position = pos;
	}

	public String getField() {
		return _field;
	}

	public void setField(String field) {
		this._field = field;
	}

	public int getPosition() {
		return _position;
	}

	public void setPosition(int pos) {
		this._position = pos;
	}
	
	

}
