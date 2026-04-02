package edu.upenn.cis.orchestra.repository.dao.ibatis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;
import edu.upenn.cis.orchestra.repository.dao.exceptions.UnknownPeerException;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsPeerIdMappingId;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsPeerIdSchemaId;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsPeerMapping;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsPeerMappingAtom;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsPeerSchemaBean;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsPeerSchemaIdBean;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsSchemaRelationBean;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsSchemaRelationCstBean;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsSchemaRelationCstFieldBean;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsSchemaRelationFieldBean;
import edu.upenn.cis.orchestra.repository.dao.model.beans.EncapsSchemaRelationFkBean;
import edu.upenn.cis.orchestra.repository.dao.model.beans.ScMappingAtomTextBean;
import edu.upenn.cis.orchestra.repository.model.beans.OrchestraSystemBean;
import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScConstraintBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScFieldBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScForeignKeyBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingAtomBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScRelationBean;
import edu.upenn.cis.orchestra.repository.model.beans.SchemaBean;

/**
 * DAO layer implemented using IBATIS as an ORM.
 * Transactions are managed using Spring declarative transactions.
 * 
 * @author Olivier Biton
 *
 */
public class IbatisRepositorySchemaDAO
		extends SqlMapClientDaoSupport
		implements RepositorySchemaDAO { 

	private Log _log = LogFactory.getLog(getClass());
	
	private void checkPeerExists (String peerId) 
			throws UnknownPeerException
	{
		PeerBean peer = getPeerDoNotLoadSchemasBean(peerId);
		if (peer == null)
			throw new UnknownPeerException (peerId);
	}
	

	
	public PeerBean getPeerDoNotLoadSchemasBean (String peerId)
	{
		PeerBean peerB = (PeerBean) getSqlMapClientTemplate().queryForObject("getPeerDoNotLoadSchemas", peerId);
		return peerB;
	}
	
	public Peer getPeerDoNotLoadSchemas (String peerId)
	{
		PeerBean bean = getPeerDoNotLoadSchemasBean (peerId);
		if (bean != null)
			try
			{
				return (new Peer(bean));
			} catch (InvalidBeanException ex)
			{
				_log.fatal("InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peerId + ")!", ex);
				assert(false) : "InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peerId + ")!";
				return null;
			}
		else
			return null;
	}
	
	@SuppressWarnings(value={"unchecked"})
	public List<SchemaBean> getAllSchemasDoNotLoadRelationsBean(PeerBean peerBean)
				throws UnknownPeerException
	{
		checkPeerExists (peerBean.getId());
		
		List<SchemaBean> sch = (List<SchemaBean>) getSqlMapClientTemplate().queryForList("getAllSchemasDoNotLoadRelations", peerBean);
		return sch;
	}

	
	public List<Schema> getAllSchemasDoNotLoadRelations(Peer peer) 
				throws UnknownPeerException
	{
		List<Schema> res = new ArrayList<Schema> ();
		List<SchemaBean> sch = getAllSchemasDoNotLoadRelationsBean (peer.toBean());
		try
		{
			for (SchemaBean bean : sch)
				res.add (new Schema (bean));
		} catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peer.getId() + ")!", ex);
			assert(false) : "InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peer.getId() + ")!";
			return null;
		}
		return res;
	}	
	
	public SchemaBean getSchemaDetailBean (String peerId, String schemaId)
					throws UnknownPeerException
	{
		checkPeerExists (peerId);
		
		EncapsPeerSchemaIdBean encaps = new EncapsPeerSchemaIdBean (peerId, schemaId);
		SchemaBean bean = (SchemaBean) getSqlMapClientTemplate().queryForObject("getSchemaDetail", encaps);
		return bean;
	}

	
	public Schema getSchemaDetail (String peerId, String schemaId)
					throws UnknownPeerException
	{
		SchemaBean bean =  getSchemaDetailBean(peerId, schemaId);
		
		if (bean!=null)
			try
			{
				return new Schema(bean);
			} catch (InvalidBeanException ex)
			{
				_log.fatal("InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peerId + ", schema " + schemaId + ")!", ex);
				assert(false) : "InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peerId + ", schema " + schemaId + ")!";
				return null;
			}
		else
			return null;
	}
	
	

	public Schema getSchemaDetail(Peer peer, String schemaId) 
					throws UnknownPeerException	
	{
		return getSchemaDetail(peer.getId(), schemaId);
	}
	
	
	public PeerBean getPeerDetailBean (String peerId)
	{
		PeerBean bean = (PeerBean) getSqlMapClientTemplate().queryForObject("getPeerDetail", peerId);
		return bean;
	}
	
	public Peer getPeerDetailNoMapping (String peerId)
	{
		PeerBean bean = getPeerDetailBean(peerId);
		if (bean != null)
		{
			try
			{
				return new Peer(bean);
			} catch (InvalidBeanException ex)
			{
				_log.fatal("InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peerId + ")!", ex);
				assert(false) : "InvalidBeanException should not be raised when loading from the RDBMS, database constraints are supposed to enforce these constraints (peer " + peerId + ")!";
				return null;
			}
		}
		else
			return null;
	}	
	
	
	
	
	@SuppressWarnings(value={"unused"})
	public void addPeer (PeerBean peerBean)
					throws DuplicatePeerIdException, InvalidBeanException 
	{
		// Check the bean
		Peer pCheck = new Peer (peerBean);
		
		// Check the bean existance
		PeerBean beanExist = getPeerDoNotLoadSchemasBean(peerBean.getId());
		if (beanExist!=null)
			throw new DuplicatePeerIdException (peerBean.getId());
		else
		{			
			getSqlMapClientTemplate().insert("insertPeer", peerBean);
			try
			{
				for (Schema schema : pCheck.getSchemas())
					addSchema(pCheck, schema);
				for (ScMappingBean mapp : peerBean.getMappings())
					addMapping(peerBean, mapp);
			} catch (DuplicateSchemaIdException ex)
			{
				_log.fatal("DuplicateSchemaIdException should not happen, peer has just been inserted with no schema (peer " + peerBean.getId() + ")!", ex);
				assert(false) : "DuplicateSchemaIdException should not happen, peer has just been inserted with no schema (peer " + peerBean.getId() + ")!";
			}
			catch (UnknownPeerException ex)
			{
				_log.fatal("UnknownPeerException should not happen, peer has just been inserted (check Spring transaction config) (peer " + peerBean.getId() + ")!", ex);
				assert(false) : "UnknownPeerException should not happen, peer has just been inserted (check Spring transaction config) (peer " + peerBean.getId() + ")!";
			}
			catch (DuplicateMappingIdException ex)
			{
				_log.fatal("DuplicateMappingIdException should not happen, peer has just been inserted with no schema (peer " + peerBean.getId() + "!", ex);
				assert(false) : "DuplicateMappingIdException should not happen, peer has just been inserted with no schema (peer " + peerBean.getId() + "!";
			}
			
		}
	}
	
	public void addPeer (Peer peer)
				throws DuplicatePeerIdException 
	{
		try
		{
			addPeer (peer.toBean());
		} catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not be raised when calling addPeer with Peer.toBean()! (peer " + peer.getId() + ")", ex);
			assert(false) : "InvalidBeanException should not be raised when calling addPeer with Peer.toBean()! (peer " + peer.getId() + ")";
		} 
	}
	

	
	public void addSchema (String peerId, SchemaBean schBean)
				throws UnknownPeerException, DuplicateSchemaIdException, InvalidBeanException
	{
		// Create fake peer bean. Not very clean but avoids changing all the IBATIS config files
		PeerBean pBean = new PeerBean ();
		pBean.setId(peerId);
		pBean.setDescription(peerId);
		pBean.setAddress(peerId);
		addSchema(pBean, schBean);
		
	}

	
	@SuppressWarnings(value={"unused"})
	public void addSchema (PeerBean peerBean, SchemaBean schBean)
			throws UnknownPeerException, DuplicateSchemaIdException, InvalidBeanException
	{
		
		// Check beans properties
		Peer pCtrl = new Peer (peerBean);
		Schema sc = new Schema (schBean);
		
		checkPeerExists(peerBean.getId());
		
		// Check that schema does not already exist
		List<SchemaBean> existSchemas = getAllSchemasDoNotLoadRelationsBean(peerBean);
		for (SchemaBean bean : existSchemas)
			if (bean.getSchemaId().equals(schBean.getSchemaId()))
				throw new DuplicateSchemaIdException (peerBean.getId(), schBean.getSchemaId());
		
		EncapsPeerSchemaBean bean = new EncapsPeerSchemaBean (peerBean, schBean);
		
		getSqlMapClientTemplate().insert("insertSchema", bean);
		
		// Save the schema relations
		for (ScRelationBean relBean : schBean.getRelations())
		{
			EncapsSchemaRelationBean encaps = 
						new EncapsSchemaRelationBean (peerBean, schBean, relBean);
			getSqlMapClientTemplate().insert("insertRelation", encaps);
			
			// Save the relation's fields
			for (ScFieldBean fldBean : relBean.getFields())
			{
				EncapsSchemaRelationFieldBean encapsFld = 
						new EncapsSchemaRelationFieldBean (peerBean, schBean, relBean, fldBean);
				getSqlMapClientTemplate().insert("insertField", encapsFld);
			}
			
			// Save the relation's direct constraints
			for (ScConstraintBean cstBean : relBean.getDirectConstraints())
				addConstraint(peerBean, schBean, relBean, cstBean);
			
		}
		
		//We can now save the foreign keys (all tables have been added to the 
		//repository)
		for (ScRelationBean relBean : schBean.getRelations())
		{
			for (ScForeignKeyBean fkBean : relBean.getForeignKeys())
			{
				addConstraint(peerBean, schBean, relBean, fkBean);
				for (int pos=0 ; pos < fkBean.getFields().size() ; pos++)
				{
					EncapsSchemaRelationFkBean encapsFk = 
							new EncapsSchemaRelationFkBean (peerBean, schBean, relBean, fkBean, pos+1);
					getSqlMapClientTemplate().insert("insertForeignKeyRefFld", encapsFk);
				}
			}
		}
		
	}
	
	
	public void addSchema (Peer peer, Schema sc)
					throws UnknownPeerException, DuplicateSchemaIdException
	{
		
		
		try
		{
			addSchema(peer.toBean(), sc.toBean());
		} catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not be raised when calling addSchema with Peer.toBean() and Schema.toBean() (Peer " + peer.getId() + ", schema " + sc.getSchemaId() + ")", ex);
			assert (false) : "InvalidBeanException should not be raised when calling addSchema with Peer.toBean() and Schema.toBean() (Peer " + peer.getId() + ", schema " + sc.getSchemaId() + ")";
		}
	}

	
	private void addConstraint (PeerBean peerBean, SchemaBean schBean, ScRelationBean relBean, ScConstraintBean cstBean)
	{
		EncapsSchemaRelationCstBean encapsCst = 
		    new EncapsSchemaRelationCstBean (peerBean, schBean, relBean, cstBean);
		getSqlMapClientTemplate().insert("insertDirectConstraint", encapsCst);
		
		// Save the constraint's fields
		int pos = 1;
		for (String fld : cstBean.getFields())
		{
			EncapsSchemaRelationCstFieldBean encapsCstFld = 
					new EncapsSchemaRelationCstFieldBean (peerBean, schBean, relBean, cstBean, fld, pos);
			getSqlMapClientTemplate().insert("insertDirectConstraintField", encapsCstFld);
			pos++;
		}		
	}
	
	
	public void addMapping (PeerBean peerBean, ScMappingBean mappBean)
					throws UnknownPeerException, DuplicateMappingIdException
	{
		// Check if the peer id exists
		checkPeerExists(peerBean.getId());
		
		// Check that this mapping id doesn't exist already
		EncapsPeerIdMappingId encaps = new EncapsPeerIdMappingId (peerBean.getId(), mappBean.getId());
		if (((String) getSqlMapClientTemplate().queryForObject("countMappingExistence", encaps)).equals("1"))
			throw new DuplicateMappingIdException (peerBean.getId(), mappBean.getId());
			
		
		
		EncapsPeerMapping encapsMapp = new EncapsPeerMapping (peerBean, mappBean);
		
		getSqlMapClientTemplate().insert("insertMapping", encapsMapp);
		
		// Save the head mapping atoms
		addMappingAtom (peerBean, mappBean, mappBean.getHead(), true);
		// Save the body mapping atoms
		addMappingAtom (peerBean, mappBean, mappBean.getBody(), false);		
	}
	
	public void addMapping (Peer peer, Mapping mapping)
					throws UnknownPeerException, DuplicateMappingIdException
	{
		addMapping(peer.toBean(), mapping.toBean());
		
	}

	private void addMappingAtom (PeerBean peerBean, ScMappingBean mappBean, List<ScMappingAtomBean> atoms, boolean isHead)
	{
		for (ScMappingAtomBean atomBean : atoms)
		{
			ScMappingAtomTextBean txtBean = new ScMappingAtomTextBean (atomBean);
			EncapsPeerMappingAtom encapsAtomFld = new EncapsPeerMappingAtom (peerBean, mappBean, txtBean, isHead);
			getSqlMapClientTemplate().insert("insertMappingAtom", encapsAtomFld);
			
		}		
	}
	
	
	@SuppressWarnings(value={"unchecked"})
	public OrchestraSystemBean loadAllPeersBean ()
	{
		Map<String, PeerBean> peers = new HashMap<String, PeerBean> ();

		List<String> ids = (List<String>) getSqlMapClientTemplate().queryForList("getAllPeerIds", null);

		Iterator<String> itPeerId = ids.iterator();
		while (itPeerId.hasNext())
		{
			String peerId = itPeerId.next();  
			if (!peers.containsKey(peerId))
				try
				{
					loadPeerAndDependenciesList(peerId, peers);
				} catch (UnknownPeerException ex)
				{
					_log.warn("UnknownPeerException has been raised in loadAllPeersBean, ids have been loaded from the database thus the peer has been deleted meanwhile!", ex);					
				}
		}
		
		OrchestraSystemBean system = new OrchestraSystemBean ();
		system.setPeers(new ArrayList<PeerBean> (peers.values()));
		return system;
	}
	
	public OrchestraSystem loadAllPeers ()
	{
		OrchestraSystem res = null;
		try
		{
			OrchestraSystemBean systemB = loadAllPeersBean();
			res = new OrchestraSystem(systemB);
		} catch (DuplicatePeerIdException ex)
		{
			_log.fatal("DuplicatePeerIdException should not happen when loading from RDBMS, constraints should enforce unicity!", ex);
			assert (false) : "DuplicatePeerIdException should not happen when loading from RDBMS, constraints should enforce unicity!";
		}
		catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not be raised when creating an OrchestraSystem from a bean loaded from RDBMS (constraints should enforce the same properties)", ex);
			assert (false) : "InvalidBeanException should not be raised when creating an OrchestraSystem from a bean loaded from RDBMS (constraints should enforce the same properties)";
			return null;
		}		
		return res;
	}

	
	private void loadPeerAndDependenciesList (String peerId, Map<String, PeerBean> peers)	
					throws UnknownPeerException
	{
		PeerBean mainPeer = getPeerDetailBean(peerId);
		if (mainPeer==null)
			throw new UnknownPeerException (peerId);
		
		peers.put(mainPeer.getId(), mainPeer);
		
		for (String peerRefId : mainPeer.getPeersIdsDependencies())
			if (!peers.containsKey(peerRefId))
			{				
				PeerBean refPeer = getPeerDetailBean(peerRefId);
				// If the peer does not exist ...
				if (refPeer==null)
				{
					_log.fatal("A peer referenced by a mapping does not exist, RDBMS constraints should enforce that (peer " + peerId + " -> peer " + peerRefId +")!");
					assert false : "A peer referenced by a mapping does not exist, RDBMS constraints should enforce that (peer " + peerId + " -> peer " + peerRefId +")!";
				}
				else
					peers.put(refPeer.getId(), refPeer);
			}
		
	}
	
	
	public OrchestraSystemBean loadPeerAndDependenciesBean (String peerId)
				throws UnknownPeerException
	{
		Map<String,PeerBean> peers = new HashMap<String,PeerBean> ();

		loadPeerAndDependenciesList(peerId, peers);
				
		List<PeerBean> peersRes = new ArrayList<PeerBean> (peers.values());		
		OrchestraSystemBean res = new OrchestraSystemBean ();
		res.setPeers(peersRes);
		
		return res;
	}
	
	public OrchestraSystem loadPeerAndDependencies (String peerId)
				throws UnknownPeerException
	{
		OrchestraSystem res = null;
		try
		{
			OrchestraSystemBean systemBean = loadPeerAndDependenciesBean(peerId);
			res = new OrchestraSystem(systemBean);
		} catch (DuplicatePeerIdException ex)
		{
			_log.fatal("DuplicatePeerIdException should not be raised when loading a new system from the database, should be enforced by constraints!", ex);
			assert false : "DuplicatePeerIdException should not be raised when loading a new system from the database, should be enforced by constraints!"; 
		}
		 catch (InvalidBeanException ex)
		{
			_log.fatal("InvalidBeanException should not be raised when loading a new system from the database, should be enforced by constraints!", ex);
			assert false : "InvalidBeanException should not be raised when loading a new system from the database, should be enforced by constraints!"; 
		}		
		return res;

	}
	
	
	public void removeMapping (String peerId, String mappingId)
			throws UnknownPeerException
	{
		checkPeerExists(peerId);
		
		EncapsPeerIdMappingId encaps = new EncapsPeerIdMappingId (peerId, mappingId); 
		getSqlMapClientTemplate().delete("deleteMappingAtom", encaps);
		getSqlMapClientTemplate().delete("deleteMapping", encaps);
	}
	
	public void removePeer (String peerId)
	{
		getSqlMapClientTemplate().delete("deleteMappingsAtomsForPeer", peerId);
		getSqlMapClientTemplate().delete("deleteMappingsForPeer", peerId);
		getSqlMapClientTemplate().delete("deleteForeignKeysForPeer", peerId);
		getSqlMapClientTemplate().delete("deleteDirectConstraintForPeer", peerId);		
		getSqlMapClientTemplate().delete("deleteConstraintForPeer", peerId);
		getSqlMapClientTemplate().delete("deleteFieldsForPeer", peerId);
		getSqlMapClientTemplate().delete("deleteRelationsForPeer", peerId);
		getSqlMapClientTemplate().delete("deleteSchemasForPeer", peerId);
		getSqlMapClientTemplate().delete("deletePeer", peerId);		
	}
	
	public void removeSchema (String peerId, String schemaId)
					throws UnknownPeerException
	{
		checkPeerExists(peerId);
		
		EncapsPeerIdSchemaId encaps = new EncapsPeerIdSchemaId ();
		encaps.setPeerId(peerId);
		encaps.setSchemaId(schemaId);
		
		getSqlMapClientTemplate().delete("deleteForeignKeysForSchema", encaps);
		getSqlMapClientTemplate().delete("deleteDirectConstraintForSchema", encaps);		
		getSqlMapClientTemplate().delete("deleteConstraintForSchema", encaps);
		getSqlMapClientTemplate().delete("deleteFieldsForSchema", encaps);
		getSqlMapClientTemplate().delete("deleteRelationsForSchema", encaps);
		getSqlMapClientTemplate().delete("deleteSchema", encaps);
	}
	
	
	//TODO: Add a caching feature? To load data in-memory when loaded?? Pb if mutiple repos objects //
	
}
