package edu.upenn.cis.orchestra.datamodel;



/**
 * The peer/schema/relation... object hierarchy has been designed to avoid circular dependencies.
 * Thus a relation does not have a reference to its schema, nor has a schema to its peer...
 * But in some cases we need to have this information (we want to avoid a lookup for the relation
 * in the OrchestraSystem). That's what this class will be used for, typically for Mappings atoms.
 * Note: this class is just a container, it will not check that the schema contains the relation or that
 * the peer contains the schema
 * 
 * @author Olivier Biton
 *
 */
public class RelationContext 
{
	/** The relation itself */
	private Relation _relation;
	
	/** The relation's schema */	
	private Schema _schema;
	
	/** The schema's peer */
	private Peer _peer;
	
	private boolean _mapping;
	
	public RelationContext (Relation relation, Schema schema, Peer peer, boolean mapping)
	{
		this._peer = peer;
		this._schema = schema;
		this._relation = relation;		
		this._mapping = mapping;
	}

	public RelationContext (Peer peer, Schema schema, Relation relation)
	{
		this._peer = peer;
		this._schema = schema;
		this._relation = relation;		
		this._mapping = false;
	}

	public boolean isMapping () {
		return _mapping;
	}
		
	public Peer getPeer() {
		return _peer;
	}

	public Relation getRelation() {
		return _relation;
	}

	public Schema getSchema() {
		return _schema;
	}
	
	
	public String toString ()
	{
		if(_peer != null && _schema != null)
			return _peer.getId() + "." + _schema.getSchemaId() + "." + _relation.getName();
		else
			return _relation.getName();
	}
	
	@Override
	public boolean equals (Object relContext)
	{
		if (relContext instanceof RelationContext)
		{
			return equals((RelationContext) relContext);
		}
		else
			return false;
	}		
	
	/*
	public boolean equals (RelationContext relContext)
	{
		return (getRelation() == relContext.getRelation()
				&& getSchema() == relContext.getSchema()
				&& getPeer() == relContext.getPeer());	
	}*/
	
	public boolean equals (RelationContext relContext)
	{
		return ((getRelation() == relContext.getRelation() || getRelation().getName().equals(relContext.getRelation().getName()))
				&& (getSchema() == relContext.getSchema() || getSchema().getSchemaId().equals(relContext.getSchema().getSchemaId()))
				&& (getPeer() == relContext.getPeer() || getPeer().getId().equals(relContext.getPeer().getId())));	
	}
	
	public int hashCode() {
		return getRelation().hashCode() ^ getSchema().hashCode();
	}
}
