package edu.upenn.cis.orchestra.repository.dao.model.beans;

import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScForeignKeyBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScRelationBean;
import edu.upenn.cis.orchestra.repository.model.beans.SchemaBean;

public class EncapsSchemaRelationFkBean 
				extends EncapsSchemaRelationCstBean 
{
	private String _refRelation;
	private String _refField;
	private int _position;
	
	public EncapsSchemaRelationFkBean ()
	{
		super ();
	}
	
	public EncapsSchemaRelationFkBean (PeerBean peer,
										SchemaBean sch,
										ScRelationBean rel,
										ScForeignKeyBean fk,
										int fieldPos
									  )
	{
		super (peer, sch, rel, fk);
		_refRelation = fk.getRefRelation();
		_refField = fk.getRefFields().get(fieldPos-1);
		_position = fieldPos;
	}

	public int getPosition() {
		return _position;
	}

	public void setPosition(int position) {
		this._position = position;
	}

	public String getRefField() {
		return _refField;
	}

	public void setRefField(String field) {
		_refField = field;
	}

	public String getRefRelation() {
		return _refRelation;
	}

	public void setRefRelation(String relation) {
		_refRelation = relation;
	}
	
	

}
