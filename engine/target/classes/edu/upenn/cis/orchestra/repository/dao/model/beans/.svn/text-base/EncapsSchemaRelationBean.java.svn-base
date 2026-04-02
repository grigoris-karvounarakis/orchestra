package edu.upenn.cis.orchestra.repository.dao.model.beans;

import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScRelationBean;
import edu.upenn.cis.orchestra.repository.model.beans.SchemaBean;



/**
 * Bean created to deal with relations inserts and updates.
 * We need an object to encapsulate a relation and its schema because
 * IBATIS takes only one parameter for its insert statements (parameterMap 
 * would be less efficient). 
 * @author Olivier BITON
 *
 */
public class EncapsSchemaRelationBean {
	private PeerBean _peer;
	private SchemaBean _schema;
	private ScRelationBean _relation;
	
	public EncapsSchemaRelationBean ()
	{
	}
	
	public EncapsSchemaRelationBean (PeerBean peer, SchemaBean schBean, ScRelationBean relBean)
	{
		_peer = peer;
		_schema = schBean;
		_relation = relBean;
	}
	
	public PeerBean getPeer ()
	{
		return _peer;
	}
	
	public void setPeer (PeerBean peer)
	{
		_peer = peer;
	}
	
	public ScRelationBean getRelation() {
		return _relation;
	}
	public void setRelation(ScRelationBean relation) {
		this._relation = relation;
	}
	public SchemaBean getSchema() {
		return _schema;
	}
	public void setSchema(SchemaBean schema) {
		this._schema = schema;
	}
	
}
