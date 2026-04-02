/**
 * This package contains the DAO interfaces/classes developed to manage the 
 * persistence of Orchestra Repository data
 */
package edu.upenn.cis.orchestra.repository.dao;

import java.util.List;

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
 * Interface describing the methods to be supported by a DAO layer for the OrchestraRepository.
 * 
 * @author Olivier Biton
 *
 */
public interface RepositorySchemaDAO {

	
	/**
	 * Lazy load of a peer properties, get only simple properties
	 * and do not load schemas
	 * @param peerId Id of the peer to be loaded 
	 * @return Bean representing the peer properties, null if does not exist
	 */
	public PeerBean getPeerDoNotLoadSchemasBean (String peerId);	
	
	/**
	 * Lazy load of a peer properties, get only simple properties
	 * and do not load schemas
	 * @param peerId Id of the peer to be loaded 
	 * @return Peer object, null if it does not exist
	 */
	public Peer getPeerDoNotLoadSchemas (String peerId);	
	
	// TODO: Exception if peer does not exist ...
	
	/**
	 * Lazy load of a peers' schemas: load only the schemas' basic 
	 * properties but do not load relations 
	 * @param peer Peer bean for which the schemas are to be loaded
	 * @return Schemas list (beans), empty if no schema
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 */
	public List<SchemaBean> getAllSchemasDoNotLoadRelationsBean(PeerBean peer)
				throws UnknownPeerException;
	/**
	 * Lazy load of a peers' schemas: load only the schemas' basic 
	 * properties but do not load relations 
	 * @param peer Peer for which the schemas are to be loaded
	 * @return Schemas list, empty if no schema
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 */
	public List<Schema> getAllSchemasDoNotLoadRelations (Peer peer)
				throws UnknownPeerException;
	;
	
	
	/**
	 * Load a schema properties (no lazy load). <BR>
	 * WARNING: The schema won't be added to <code>peer</code>. 
	 * @param peer Peer for which the schema must be loaded.
	 * @param schemaId Schema to be loaded
	 * @return Schema object with full relations properties, null if it does not exist
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 */
	public Schema getSchemaDetail (Peer peer, String schemaId)
				throws UnknownPeerException;
	;
	/**
	 * Load a schema properties (no lazy load)
	 * @param peerId Peer id for which the schema must be loaded.
	 * @param schemaId Schema to be loaded
	 * @return Schema object with full relations properties, null if it does not exist
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 */
	public Schema getSchemaDetail (String peerId, String schemaId)
				throws UnknownPeerException;
	;	
	/**
	 * Load a schema properties (no lazy load)
	 * @param peerId Peer id for which the schema must be loaded.
	 * @param schemaId Schema to be loaded
	 * @return Schema bean with full relations properties, null if it does not exist
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 */
	public SchemaBean getSchemaDetailBean (String peerId, String schemaId)
				throws UnknownPeerException;
	;	
	
	
	/**
	 * Load a peer properties
	 * @param peerId Id of the peer to be loaded
	 * @return Bean describing the peer (includes mappings), null if does not exist
	 */
	public PeerBean getPeerDetailBean (String peerId);
	
	/**
	 * Load a peer properties. <BR>
	 * WARNING: Mappings are not loaded (because other peers are 
	 * not known. To get peers and mappings, request an OrchestraSystem
	 * object
	 * @param peerId Id of the peer to be loaded
	 * @return The peer object (without mappings)
	 * @see RepositorySchemaDAO#loadAllPeers()
	 * @see RepositorySchemaDAO#loadPeerAndDependencies(String)
	 */
	public Peer getPeerDetailNoMapping (String peerId);

	/**
	 * Load all peers known by Orchestra. <BR>
	 * Will also load the peers' mappings
	 * @return Bean describing the whole system
	 */
	public OrchestraSystemBean loadAllPeersBean ();
	/**
	 * Load all peers known by Orchestra
	 * Will also load the peers' mappings
	 * @return Object describing the whole system
	 */
	public OrchestraSystem loadAllPeers ();
	
	
	/**
	 * Load a peer and all its dependencies, that is a peer and all the 
	 * other peers references by its mappings.
	 * @param peerId Id of the peer to be loaded
	 * @return Bean describing the subset of peers in the system that are used from peerId's mappings
	 * @throws UnknownPeerException If the peer is unknown
	 */
	public OrchestraSystemBean loadPeerAndDependenciesBean (String peerId)
					throws UnknownPeerException;
	
	/**
	 * Load a peer and all its dependencies, that is a peer and all the 
	 * other peers references by its mappings.
	 * @param peerId Id of the peer to be loaded
	 * @return Object describing the subset of peers in the system that are used from peerId's mappings
	 * @throws UnknownPeerException If the peer is unknown
	 */
	public OrchestraSystem loadPeerAndDependencies (String peerId)
					throws UnknownPeerException;
	
	
	/**
	 * Add a peer to the system
	 * @param peer Peer to add
	 * @throws DuplicatePeerIdException Raised if a peer already exists that has the same id.
	 */
	public void addPeer (PeerBean peer)
				throws DuplicatePeerIdException, InvalidBeanException; 
	/**
	 * Add a peer to the system
	 * @param peer Peer to add
	 * @throws DuplicatePeerIdException Raised if a peer already exists that has the same id.
	 */	
	public void addPeer (Peer peer)
				throws DuplicatePeerIdException; 
	
	/**
	 * Add a schema to a given peer
	 * @param peerId Id of Peer to which the schema must be added
	 * @param sc New schema
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 * @throws DuplicateSchemaIdException If the schema id is already used for this peer
	 * @throws InvalidBeanException If the bean properties aren't correct
	 */
	public void addSchema (String peerId, SchemaBean sc)
				throws UnknownPeerException, DuplicateSchemaIdException, InvalidBeanException;
	
	/**
	 * Add a schema to a given peer. <BR>
	 * WARNING: Will be added in the repository storage, but the peer object
	 * won't be modified to include the schema
	 * @param peer Peer to which the schema must be added
	 * @param sc New schema
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 * @throws DuplicateSchemaIdException If the schema id is already used for this peer
	 * @throws InvalidBeanException If the bean properties aren't correct
	 */
	public void addSchema (PeerBean peer, SchemaBean sc)
				throws UnknownPeerException, DuplicateSchemaIdException, InvalidBeanException;
	/**
	 * Add a schema to a given peer. <BR>
	 * WARNING: Will be added in the repository storage, but the peer object
	 * won't be modified to include the schema
	 * @param peer Peer to which the schema must be added
	 * @param sc New schema
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 * @throws DuplicateSchemaIdException If the schema id is already used for this peer
	 */
	public void addSchema (Peer peer, Schema sc)
				throws UnknownPeerException, DuplicateSchemaIdException;
	
	/**
	 * Add a mapping to a given peer
	 * @param peer Bean description of the peer to which the mapping should be added
	 *         This object won't be modified, only the repository is updated!
	 * @param mapping bean description of the mapping to be added
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 * @throws DuplicateMappingIdException If this mapping id already exists in the peer
	 */
	public void addMapping (PeerBean peer, ScMappingBean mapping)
				throws InvalidBeanException, UnknownPeerException, DuplicateMappingIdException;
	
	/**
	 * Add a mapping to a given peer
	 * @param peer Bean description of the peer to which the mapping should be added
	 *         This object won't be modified, only the repository is updated!
	 * @param mapping bean description of the mapping to be added
	 * @throws UnknownPeerException If the peer is unknown in the repository
	 * @throws DuplicateMappingIdException If this mapping id already exists in the peer
	 */	
	public void addMapping (Peer peer, Mapping mapping)
				throws UnknownPeerException, DuplicateMappingIdException;

	
	/**
	 * Removes the mapping with id <code>mappingId</code> in the peer with id <code>peerId</code>. <BR>
	 * No exception will be raised if the mapping does not exist 
	 * @param peerId Id of the peer containing the mapping to remove
	 * @param mappingId Id of the mapping to remove
	 */
	public void removeMapping (String peerId, String mappingId)
				throws UnknownPeerException;


	/**
	 * Removes the peer from the repository. <BR>
	 * No exception will be raised if the mapping does not exist
	 * @param peerId If of the peer to remove
	 */
	public void removePeer (String peerId);
	
	
	/**
	 * Removes a schema from the repository. <BR>
	 * No exception is raised if the schema does not exist in the peer
	 * @param peerId Id of the peer containing the schema
	 * @param schemaId Id of the schema to remove
	 * @throws UnknownPeerException 
	 */
	public void removeSchema (String peerId, String schemaId)
				throws UnknownPeerException;
	
	
	//TODO: Deal with Updates?? What about dependencies?
	
}
