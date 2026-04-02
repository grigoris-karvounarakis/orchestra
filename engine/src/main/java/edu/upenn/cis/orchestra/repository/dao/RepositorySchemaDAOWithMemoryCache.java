package edu.upenn.cis.orchestra.repository.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.repository.dao.exceptions.UnknownPeerException;
import edu.upenn.cis.orchestra.repository.model.beans.OrchestraSystemBean;
import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingBean;
import edu.upenn.cis.orchestra.repository.model.beans.SchemaBean;



/**
 * Abstract implementation of the repository DAO, to extend for any DAO 
 * implementation that uses an in-memory cache of the repository objects.
 *  
 * @author Olivier Biton
 *
 */
public class RepositorySchemaDAOWithMemoryCache implements RepositorySchemaDAO 
{
	protected OrchestraSystem _inMemoryRepos=null;
	
	private Log _log = LogFactory.getLog(getClass());
	
	protected void setSystem (OrchestraSystem system)
	{
		_inMemoryRepos = system;
	}
	

	private void checkPeerExists (String peerId)
				throws UnknownPeerException
	{
		if (_inMemoryRepos.getPeer(peerId)==null)
			throw new UnknownPeerException (peerId);
	}
	
	public void addMapping(Peer peer, Mapping mapping)
			throws UnknownPeerException, DuplicateMappingIdException
	{
		try
		{
			addMapping(peer.toBean(), mapping.toBean());
		} catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not occur when calling addMapping with peer.toBean() and mapping.toBean()!", ex);
			assert (false) : "InvalidBeanException should not occur when calling addMapping with peer.toBean() and mapping.toBean()!";
		}
	}

	public void addMapping(PeerBean peer, ScMappingBean mapping)
			throws InvalidBeanException, UnknownPeerException, DuplicateMappingIdException
	{
		checkPeerExists (peer.getId());
		
		Mapping mapp = new Mapping(mapping, _inMemoryRepos);
		_inMemoryRepos.getPeer(peer.getId()).addMapping(mapp);		
	}

	public void addPeer(Peer peer) 
		throws DuplicatePeerIdException
	{
		try
		{
			addPeer (peer.toBean());
		} catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not occur when calling addPeer with peer.toBean()!", ex);
			assert(false) : "InvalidBeanException should not occur when calling addPeer with peer.toBean()!";
		}
	}

	public void addPeer(PeerBean peer)
				throws DuplicatePeerIdException, InvalidBeanException
	{
		Peer p = new Peer (peer);
		_inMemoryRepos.addPeer(p);
		
	}

	public void addSchema(Peer peer, Schema sc) 
				throws UnknownPeerException, DuplicateSchemaIdException
	{
		try
		{
			addSchema(peer.toBean(), sc.toBean());
		} catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not occur when calling addSchema with Peer.toBean() and Schema.toBean()!", ex);
			assert(false) : "InvalidBeanException should not occur when calling addSchema with Peer.toBean() and Schema.toBean()!";
		}
	}

	public void addSchema(PeerBean peer, SchemaBean sc) 
				throws UnknownPeerException, DuplicateSchemaIdException, InvalidBeanException {
		
		addSchema(peer.getId(), sc);
	}
	
	public void addSchema (String peerId, SchemaBean schBean)
			throws UnknownPeerException, DuplicateSchemaIdException, InvalidBeanException
	{
		checkPeerExists(peerId);
		
		Schema schema = new Schema(schBean);
		_inMemoryRepos.getPeer(peerId).addSchema(schema);
	}



	public List<SchemaBean> getAllSchemasDoNotLoadRelationsBean(PeerBean peer) 
					throws UnknownPeerException
	{
		return (getAllSchemasDoNotLoadRelationsBean(peer.getId()));
	}


	public List<SchemaBean> getAllSchemasDoNotLoadRelationsBean(String peerId)
					throws UnknownPeerException
	{
		checkPeerExists(peerId);
		
		List<Schema> schemas = getAllSchemasDoNotLoadRelations(peerId);
		List<SchemaBean> res = new ArrayList<SchemaBean> ();
		for (Schema sc : schemas)
			res.add (sc.toBean());
		return res;
	}
	
	// No need to use lazy load from flat file... Schemas will have relation
	public List<Schema> getAllSchemasDoNotLoadRelations(Peer peer)
				throws UnknownPeerException
	{
		
		return getAllSchemasDoNotLoadRelations(peer.getId());
	}	
	

	

	public List<Schema> getAllSchemasDoNotLoadRelations (String peerId)
				throws UnknownPeerException
	{
		checkPeerExists(peerId);
		
		Peer p = _inMemoryRepos.getPeer(peerId).deepCopy();
		return new ArrayList<Schema> (p.getSchemas());
	}

	public PeerBean getPeerDetailBean(String peerId) 
	{
		Peer peer = getPeerDetailNoMapping(peerId);
		if (peer != null)
			return peer.toBean();
		else
			return null;
	}

	// No need to use lazy loads for flat file, will contain mappings
	public Peer getPeerDetailNoMapping(String peerId) {
		Peer peer = _inMemoryRepos.getPeer(peerId);
		if (peer != null)
			return peer.deepCopy();
		else
			return null;
	}

	// No need to use lazy loads for flat file, will contain schemas
	public Peer getPeerDoNotLoadSchemas(String peerId) {
		return getPeerDetailNoMapping(peerId);
	}

	public PeerBean getPeerDoNotLoadSchemasBean(String peerId) {
		return getPeerDetailBean(peerId);
	}

	public Schema getSchemaDetail(Peer peer, String schemaId) 
				throws UnknownPeerException
	{
		return getSchemaDetail(peer.getId(), schemaId);
	}

	public Schema getSchemaDetail(String peerId, String schemaId) 
				throws UnknownPeerException
	{
		checkPeerExists(peerId);
		
		Schema schema = _inMemoryRepos.getPeer(peerId).getSchema(schemaId);
		if (schema != null)
			return schema.deepCopy();
		else
			return null;
	}

	public SchemaBean getSchemaDetailBean(String peerId, String schemaId) 
				throws UnknownPeerException
	{
		Schema schema = getSchemaDetail (peerId, schemaId);
		if (schema != null)
			return schema.toBean();
		else
			return null;
	}

	public OrchestraSystem loadAllPeers() {
		return _inMemoryRepos;
	}

	public OrchestraSystemBean loadAllPeersBean() {
		return _inMemoryRepos.toBean();
	}

	// No need for lazy loads from flat file
	public OrchestraSystem loadPeerAndDependencies(String peerId) {
		return loadAllPeers();
	}

	public OrchestraSystemBean loadPeerAndDependenciesBean(String peerId)
	{
		return loadAllPeersBean();
	}
 
	public void removeMapping (String peerId, String mappingId)
					throws UnknownPeerException
	{
		checkPeerExists(peerId);
		
		_inMemoryRepos.getPeer(peerId).removeMapping(mappingId);
	}	
	
	public void removePeer (String peerId)
	{
		_inMemoryRepos.removePeer(peerId);
	}

	public void removeSchema (String peerId, String schemaId)
				throws UnknownPeerException
	{
		checkPeerExists(peerId);
		
		_inMemoryRepos.getPeer(peerId).removeSchema(schemaId);
	}
}
