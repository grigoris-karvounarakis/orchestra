/**
 * This package contains all classes used to describe the Orchestra Repository objects, 
 * and conversions to/from beans.
 */
package edu.upenn.cis.orchestra.datamodel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.springframework.beans.BeanUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogProgram;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.DatalogViewUnfolder;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.RecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datamodel.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.mappings.exceptions.RecursionException;
import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.NotPred;
import edu.upenn.cis.orchestra.predicate.OrPred;
import edu.upenn.cis.orchestra.proql.MatchPatterns;
import edu.upenn.cis.orchestra.proql.Pattern;
import edu.upenn.cis.orchestra.proql.QueryParser;
import edu.upenn.cis.orchestra.proql.SchemaGraph;
import edu.upenn.cis.orchestra.proql.SchemaSubgraph;
import edu.upenn.cis.orchestra.provenance.OuterJoinUnfolder;
import edu.upenn.cis.orchestra.provenance.ProvenanceNode;
import edu.upenn.cis.orchestra.reconciliation.ClientCentricDb;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.HashTableStore;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.SqlUpdateStore;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BerkeleyDBStoreClient;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BerkeleyDBStoreServer;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.ParseException;
import edu.upenn.cis.orchestra.repository.model.beans.OrchestraSystemBean;
import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/****************************************************************
 * Class used to group multiple peers (global view of the system)
 * @author Olivier Biton
 *****************************************************************
 */
public class OrchestraSystem {

	// Subclasses that must be loaded before we can deserialize anything
	@SuppressWarnings("unused")
	private static Class<?>[] classesToLoad = {IntPeerID.class,
		StringPeerID.class, AndPred.class,
		OrPred.class, NotPred.class,
		ComparePredicate.class};

	/** Set of peers composing the system */
	private Map<String,Peer> _peers;


	/** Local objects: mapping and reconciliation engines */
	protected BasicEngine _mappingEngine;
	protected UpdateStore.Factory _usf;
	protected StateStore.Factory _ssf;
	protected Map<String,Db> _recDbs;
	protected SchemaIDBinding _mapStore;
	protected Map<String,Map<String,TrustConditions>> _tcs;
	//protected Map<String,Map<String,edu.upenn.cis.orchestra.datamodel.Schema>> _schemas
	//								= new Hashtable<String,Map<String,edu.upenn.cis.orchestra.datamodel.Schema>>();

	//protected Schema _schema;
	protected Map<Peer,Schema> _schemas;
	protected boolean _recMode;
	protected String _name = "";

	private BerkeleyDBStoreServer _storeServer = null;
	private Environment _env = null;
	private static final String storeName = "updateStore";

	/**
	 * Creates an empty Orchestra system
	 */
	public OrchestraSystem ()
	{
		super ();
		_peers = new Hashtable<String, Peer>();
		_recDbs = new Hashtable<String,Db>();
		_schemas = new HashMap<Peer,Schema>();
		_tcs = new Hashtable<String,Map<String,TrustConditions>>();
	}
	public OrchestraSystem (SchemaIDBinding sch)
	{
		this();
		_mapStore = sch;
	}

	/**
	 * Deep copy of a given OrchestraSystem (use method 
	 * deepCopy to benefit from polymorphism)
	 * @param system System to copy
	 * @see OrchestraSystem#deepCopy()
	 */
	protected OrchestraSystem (OrchestraSystem system)
	{
		this(system._mapStore);

		// First: Create deep copy of peers without their 
		// mappings (would'nt be possible to load mappings 
		// if the referenced peer does not exist yet)
		Map<Peer,Peer> oldNewPeers = new HashMap<Peer, Peer> (); 
		for (Peer p : system.getPeers())
		{
			Peer np = p.deepCopy();
			oldNewPeers.put (p,np);
			try
			{
				addPeer(np);
			} catch (DuplicatePeerIdException ex)
			{
				System.out.println ("Peer id conflict should not happen in a deep copy!");
				ex.printStackTrace();
			}
		}
		// Second: Complete the deep copy by copying the peers
		for (Map.Entry<Peer, Peer> entry : oldNewPeers.entrySet())
			entry.getValue().deepCopyMappings(entry.getKey(), this);

		// Third: shallow-copy the stuff more recently added to OrchestraSystem
		// TODO: deep-copy?
		_mappingEngine = system._mappingEngine;
		//		_mappingDb = system._mappingDb;
		_usf = system._usf;
		_ssf = system._ssf;
		for (String peer : system._tcs.keySet()) {
			Map<String,TrustConditions> tcForSchemas = new HashMap<String,TrustConditions>();
			_tcs.put(peer, tcForSchemas);
			for (String schema : system._tcs.get(peer).keySet()) {
				tcForSchemas.put(schema, system._tcs.get(peer).get(schema));
			}
		}
		_recMode = system._recMode;
		_name = system._name;
		// _recDbs will get filled in as needed
	}

	public void setSchemaIDBinding(SchemaIDBinding bind) {
		_mapStore = bind;
	}

	public synchronized String getName() {
		return _name;
	}

	public synchronized void setName(String name) {
		_name = name;
	}

	/**
	 * Create a new OrchestraSystem from its bean representation
	 * @param bean Bean describing the system
	 * @throws DuplicatePeerIdException If two peers in the bean share a common id
	 * @throws InvalidBeanException If any of the system bean subproperties is an invalid bean
	 */
	public OrchestraSystem (OrchestraSystemBean bean)
	throws DuplicatePeerIdException, InvalidBeanException
	{ 

		this ();

		// First create the peers with no mappings
		Map<PeerBean, Peer> mapPeers = new HashMap<PeerBean, Peer> (); 	
		for (PeerBean peerBean : bean.getPeers())
		{
			Peer peer = new Peer(peerBean);
			addPeer (peer);

			mapPeers.put(peerBean, peer);
		}

		//TODO: Exception handling if a peer is missing...

		// Then complete the creation with the mappings (all referenced peers must exist)
		for (Map.Entry<PeerBean, Peer> peerEntry : mapPeers.entrySet())
			peerEntry.getValue().loadMappings(peerEntry.getKey(), this);

		if (_ssf == null)
			_ssf = new HashTableStore.Factory();
		if (_usf == null) {
			final String jdbcUrl = "jdbc:db2://grw561-3.cis.upenn.edu:50000/orchestr";
			final String username = "orchestra";
			final String password = "apollo";

			try {
				Class.forName(Config.getJDBCDriver());
				_usf = new SqlUpdateStore.Factory(jdbcUrl, username, password);
			} catch (ClassNotFoundException cnf) {
				throw new RuntimeException("Unable to connect to JDBC");
			}
		}
	}

	public synchronized boolean getRecMode() {
		return _recMode;
	}

	public synchronized void setRecMode(boolean recMode) {
		_recMode = recMode;
	}

	//TODO: Errors if ids not unique

	/**
	 * Add a new peer to the system
	 * @param peer peer to add
	 * @throws DuplicatePeerIdException If a peer already exists with the same id
	 */
	public synchronized void addPeer (Peer peer)
	throws DuplicatePeerIdException
	{
		if (_peers.containsKey(peer.getId()))
			throw new DuplicatePeerIdException (peer.getId());
		else
			_peers.put(peer.getId(), peer);

		/*
		Map<String,edu.upenn.cis.orchestra.datamodel.Schema> map = new Hashtable<String,edu.upenn.cis.orchestra.datamodel.Schema>();
		_schemas.put(peer.getId(), map);
		for (Schema s : peer.getSchemas()) {
			try {
				map.put(s.getSchemaId(), new edu.upenn.cis.orchestra.datamodel.Schema(s));
			} catch (BadColumnName bcn) {
				bcn.printStackTrace();
			}
		}*/
	}

	/** 
	 * Add a list of peers to the system
	 * @param peers new peers
	 * @throws DuplicatePeerIdException If at least one peer in <code>peers</code>
	 *     uses an id already used in the system, or if at least two peers in this 
	 *     list have a common id.
	 */
	public void addPeers (List<Peer> peers)
	throws DuplicatePeerIdException
	{
		for (Peer p : peers)
			addPeer (p);
	}

	/**
	 * Get a peer from its id
	 * @param peerId Id to look for
	 * @return Peer if it exists, null otherwise
	 */
	public synchronized Peer getPeer (String peerId)
	{
		return _peers.get(peerId);
	}

	/**
	 * Get the list of peers in this system <br>
	 * WARNING: Not a deep copy (to improve performances)
	 * @return List of peers, can be empty, cannot be null
	 */
	public synchronized Collection<Peer> getPeers ()
	{
		List<Peer> retval = new ArrayList<Peer>(_peers.values());
		Collections.sort(retval, new Comparator<Peer>() {
			public int compare(Peer o1, Peer o2) {
				return o1.getId().compareTo(o2.getId());
			}
		});
		return retval;
	}

	//TODO: Check if mappings ref this peer!
	/**
	 * Removes the peer from the system
	 * @param peerId Id of the peer to remove
	 */
	public synchronized void removePeer (String peerId)
	{
		_peers.remove(peerId);
	}


	/**
	 * Get a deep copy of this Orchestra system
	 * @return Deep copy
	 * @throws  
	 * @see OrchestraSystem#OrchestraSystem(OrchestraSystem)
	 */
	public synchronized OrchestraSystem deepCopy ()
	{
		return new OrchestraSystem(this);
	}


	/**
	 * Convert this object to its bean representation
	 * @return Bean describing the object
	 */
	public synchronized OrchestraSystemBean toBean ()
	{

		OrchestraSystemBean bean = new OrchestraSystemBean ();
		// Copy simple properties
		BeanUtils.copyProperties(this, bean, new String[] {"peers"});

		// Create the list of peers beans
		List<PeerBean> peers = new ArrayList<PeerBean> ();
		for (Peer p : getPeers())
			peers.add(p.toBean());
		bean.setPeers(peers);

		return bean;
	}


	/**
	 * String representation of this Orchestra system. <BR>
	 * Conforms with the flat file representation defined in RepositoryDAO
	 * @return String representation
	 */
	public synchronized String toString ()
	{

		StringBuffer buff = new StringBuffer ();
		buff.append("PEERS\n"); 
		for (Peer p : getPeers())
			buff.append(p.toString(1) + "\n");
		return buff.toString();
	}

	/**
	 * Extract mappings from all the system peers
	 * @param materialized If true, only materialized mappings will be returned. 
	 * 					   If false only not materialized
	 * @return Mappings
	 */
	public synchronized List<Mapping> getAllSystemMappings (boolean materialized)
	{
		List<Mapping> res = new ArrayList<Mapping> ();
		for (Peer p : getPeers())
			for (Mapping mapping : p.getMappings ())
				if (mapping.isMaterialized()==materialized)
					res.add(mapping);
		return res;
	}

	public synchronized List<RelationContext> getAllUserRelations ()
	{
		List<RelationContext>rels = new ArrayList<RelationContext>();

		for (Peer p : getPeers()){
			for (Schema s : p.getSchemas()){
				for(Relation r : s.getRelations()) {
					if(!r.isInternalRelation()){
						rels.add(new RelationContext(r, s, p, false));
					}
				}
			}
		}
		return rels;
	}

	public synchronized RelationContext getRelationByName(String peer, String schema, String relation) throws RelationNotFoundException {
		Peer p = _peers.get(peer);
		if (p != null) {
			Schema s = p.getSchema(schema);
			if (s != null) {
				Relation r = s.getRelation(relation);
				if (r != null) {
					return new RelationContext(r, s, p, false);
				}
			}
		}
		// also look in the mapping relations
		if (_mappingEngine != null && _mappingEngine.getMappingRelations() != null) {
			for (RelationContext scr : _mappingEngine.getMappingRelations()) {
				if (scr.getPeer().getPeerId().equals(peer) &&
						scr.getSchema().getSchemaId().equals(schema) &&
						scr.getRelation().getName().equals(relation)) {
					return scr;
				}
			}
		}
		if (getMappingDb().getBuiltInSchemas().containsKey(schema)) {
			Relation rel = getMappingDb().getBuiltInSchemas().get(schema).getRelation(relation);
			if (rel != null)
				return new RelationContext(rel, getMappingDb().getBuiltInSchemas().get(schema), null, false);
		}
		throw new RelationNotFoundException(peer + "." + schema + "." + relation);
	}

	public synchronized List<RelationContext> getRelationsByName(String relation) {
		ArrayList<RelationContext> list = new ArrayList<RelationContext>();
		for (Peer p : getPeers()) {
			for (Schema s : p.getSchemas()) {
				for (Relation r : s.getRelations()) {
					if (r.getName().equals(relation)) {
						list.add(new RelationContext(r, s, p, false));
					}
				}
			}
		}
		for (Schema sch : getMappingDb().getBuiltInSchemas().values()) {
			try {
				Relation rel = sch.getRelation(relation);
				list.add(new RelationContext(rel, sch, null, false));
			} catch (RelationNotFoundException rnf) {

			}
		}
		if (_mappingEngine != null) {
			for (RelationContext scr : _mappingEngine.getMappingRelations()) {
				if (scr.getRelation().getName().equals(relation)) {
					list.add(scr);
				}
			}
		}
		return list;
	}

	public synchronized Schema getSchemaByName(String peer, String schema) {
		Peer p = _peers.get(peer);
		if (p != null) {
			return p.getSchema(schema);
		} else
			return getMappingDb().getBuiltInSchemas().get(schema);

		//return null;
	}

	public synchronized List<Schema> getAllSchemas() {
		List<Schema> ret = new ArrayList<Schema>();
		for (Peer p : getPeers())
			ret.addAll(p.getSchemas());
		return ret;
	}

	public synchronized Db getRecDb(String name) throws DbException {
		//		if (! getRecMode()) {
		//		throw new DbException("Cannot get a RecDb when not in Rec mode");
		//		}
		Peer p = _peers.get(name);
		if (p == null) {
			throw new IllegalArgumentException("Peer " + name + " is not in the Orchestra system");
		}
		/*
		if (_schemas.get(name) == null || _schemas.get(name).size() != 1) {
			throw new IllegalArgumentException("Cannot get a reconciliation DB for peer " + name + " without exactly one schema");
		}
		edu.upenn.cis.orchestra.datamodel.Schema schema = null;
		for (edu.upenn.cis.orchestra.datamodel.Schema s : _schemas.get(name).values()) {
			schema = s;
		}
		String schemaName = null;
		for (String sn : _schemas.get(name).keySet()) {
			schemaName = sn;
		}
		if (_schema == null) {
			_schema = schema;
		} else {
			if (! _schema.equals(schema)) {
				throw new DbException("Cannot create reconciliation DBs with different schemas");
			}
		}*/

		//		if (_schema == null) {
		/*		
		Schema	schema = new Schema(p.getSchemas().iterator().next().getSchemaId(), 
				p.getSchemas().iterator().next().getDescription());

		// Give every schema the same name
		for (Schema sch : getAllSchemas()) {
			for (Relation r : sch.getRelations())
				try {
					schema.addRelation(r);
				} catch (DuplicateRelationIdException dri) {
				}
		}
		schema.markFinished();*/
		//		}
		//		schema = p.getSchemas().iterator().next();
		Schema schema = p.getSchemas().iterator().next();
		//		_schemas.put(p, schema);
		String schemaName = schema.getSchemaId();
		Db db = _recDbs.get(name);
		if (db == null) {
			if (_tcs.get(name) != null && _tcs.get(name).get(schemaName) != null) {
				db = new ClientCentricDb(this, _mapStore, schema, new StringPeerID(name), _tcs.get(name).get(schemaName), _usf, _ssf);
			} else {
				db = new ClientCentricDb(this, _mapStore, schema, new StringPeerID(name), _usf, _ssf);
			}
			_recDbs.put(name, db);
			return db;
		} else {
			if (! db.isConnected()) {
				db.reconnect();
			}
			return db;
		}
	}

	public boolean isLocalUpdateStore() {
		return _usf.isLocal();
	}

	public synchronized BasicEngine getMappingEngine() {
		return _mappingEngine;
	}

	public synchronized void setMappingEngine(BasicEngine engine) {
		_mappingEngine = engine;
	}

	public synchronized IDb getMappingDb() {
		return _mappingEngine.getMappingDb();
	}

	//	public synchronized IDb getUpdateDb() {
	//	return _mappingEngine.getUpdateDb();
	//	}

	public synchronized void disconnect() throws Exception {
		if (_mappingEngine != null) {
			_mappingEngine.close();
			_mappingEngine = null;
		}
		for (Db db : _recDbs.values()) {
			if (db.isConnected()) {
				db.disconnect();
			}
		}
	}

	public synchronized USDump dump(Peer p) throws DbException {
		/*		if (! getRecMode()) {
			throw new DbException("Cannot dump update store when not in rec mode");
		}
		 */
		for (Db db : _recDbs.values()) {
			if (db.isConnected()) {
				db.disconnect();
			}
		}

		return _usf.dumpUpdateStore(_mapStore, _schemas.get(p));
	}

	public synchronized void restore(Peer p, USDump dump) throws DbException {
		_usf.restoreUpdateStore(dump);
	}

	public synchronized void reset() throws Exception {
		//		if (getRecMode()) {
		for (Db db : _recDbs.values()) {
			db.disconnect();
		}
		_recDbs.clear();
		// Assume that we have only one schema
		/*
			if (_schema == null) {
				for (Map<String,edu.upenn.cis.orchestra.datamodel.Schema> schemasForPeer : _schemas.values()) {
					for (edu.upenn.cis.orchestra.datamodel.Schema s : schemasForPeer.values()) {
						if (_schema == null) {
							_schema = s;
						} else if (! _schema.equals(s)) {
							throw new DbException("Should not be in rec mode if more than one schema is present");
						}
					}
				}
			}*/


		// NEW: reset all schemas for all peers
		Set<Schema> allSchemas = new HashSet<Schema>();

		for (Peer p : _peers.values()) {
			allSchemas.addAll(p.getSchemas());
		}

		for (Schema s : allSchemas)
			_usf.resetStore(s);//_schema);
		//		} else {
		getMappingEngine().reset();
		//		}
	}

	static protected Element addChild(Document doc, Element parent, String label, String name) { 
		Element child = doc.createElement(label);
		child.setAttribute("name", name);
		parent.appendChild(child);
		return child;
	}

	public synchronized void serialize(OutputStream out) {
		try {
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = builderFactory.newDocumentBuilder();
			Document doc = builder.newDocument();
			Element cat = doc.createElement("catalog");
			cat.setAttribute("recmode", Boolean.toString(_recMode));
			cat.setAttribute("name", _name);
			doc.appendChild(cat);
			for (String id : _peers.keySet()) {
				Element peer = addChild(doc, cat, "peer", id);
				_peers.get(id).serialize(doc, peer);
			}
			for (String id : _peers.keySet()) {
				Peer peer = _peers.get(id);
				for (Mapping mapping : peer.getMappings()) {
					Element m = DomUtils.addChild(doc, cat, "mapping");
					mapping.serialize(doc, m);
				}
			}
			if (_mappingEngine != null) {
				Element engine = DomUtils.addChild(doc, cat, "engine");
				_mappingEngine.serialize(doc, engine);
			}
			Element store = DomUtils.addChild(doc, cat, "store");
			Element update = DomUtils.addChild(doc, store, "update");
			Element state = DomUtils.addChild(doc, store, "state");
			if (_usf != null) {
				_usf.serialize(doc, update);
			}
			if (_ssf != null) {
				_ssf.serialize(doc, state);
			}
			for (String peer : _tcs.keySet()) {
				for (String schema : _tcs.get(peer).keySet()) {
					Element trustConds = DomUtils.addChild(doc, cat, "trustConditions");
					trustConds.setAttribute("peer", peer);
					trustConds.setAttribute("schema", schema);
					_tcs.get(peer).get(schema).serialize(doc, trustConds, _peers.get(peer).getSchema(schema));
				}
			}
			DomUtils.write(doc, out);
		} catch (ParserConfigurationException e) {
			assert(false);	// can't happen
		}
	}

	static public void readBuiltInFunctions(InputStream in, IDb db) throws Exception {
		try {
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = builderFactory.newDocumentBuilder();
			Document document = builder.parse(in);
			Element root = document.getDocumentElement();
			if (!root.getNodeName().equals("catalog")) {
				throw new XMLParseException("Missing top-level catalog element");
			}
			NodeList list = root.getChildNodes();

			for (int i = 0; i < list.getLength(); i++) {
				Node node = list.item(i);
				if (node instanceof Element) {
					Element el = (Element)node;
					String name = el.getNodeName();
					if (name.equals("schema")) {
						Schema s = Schema.deserialize(el, true);
						db.registerBuiltInSchema(s);
					}
				}
			}
		} catch (ParserConfigurationException e) {
			assert(false);	// can't happen
		} catch (DuplicateRelationIdException e) {
			throw new XMLParseException("Duplicate relation name " + e.getRelId());
		} catch (UnknownRefFieldException e) {
			throw new XMLParseException("Unknown field in key: " + e);
		}
	}

	static public OrchestraSystem deserialize(InputStream in) throws Exception {
		try {
			System.out.println("*******deserializing catalog********");
			OrchestraSystem catalog = new OrchestraSystem();
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = builderFactory.newDocumentBuilder();
			Document document = builder.parse(in);
			Element root = document.getDocumentElement();
			if (!root.getNodeName().equals("catalog")) {
				throw new XMLParseException("Missing top-level catalog element");
			}
			String catName = root.getAttribute("name");
			if (catName == null || catName.length() == 0) {
				throw new XMLParseException("Catalog must have a name");
			}
			catalog.setName(catName);
			boolean recMode = Boolean.parseBoolean(root.getAttribute("recmode"));
			catalog.setRecMode(recMode);
			NodeList list = root.getChildNodes();

			Config.setBidirectional(false);
			for (int i = 0; i < list.getLength(); i++) {
				Node node = list.item(i);
				if (node instanceof Element) {
					Element el = (Element)node;
					String name = el.getNodeName();
					if (name.equals("peer")) {
						Peer peer = Peer.deserialize(el);
						catalog.addPeer(peer);
					} else if (name.equals("mapping")) {
						Mapping mapping = Mapping.deserialize(catalog, el);
						Peer peer = mapping.getMappingHead().get(0).getPeer();
						peer.addMapping(mapping);
					} else if (name.equals("engine")) {
						BasicEngine engine = BasicEngine.deserialize(catalog, el);
						catalog.setMappingEngine(engine);
						try {
							InputStream inFile = Config.class.getResourceAsStream("functions.schema");

							readBuiltInFunctions(inFile, catalog.getMappingDb());
						} catch (FileNotFoundException fnf) {
							throw new ParseException("Cannot find built-in functions");
						}
					} else if (el.getNodeName().equals("store")) {

						// Reset the relation ID indices on each schema so they're unique
						//						int off = 0;
						for (Schema sch : catalog.getAllSchemas()) {
							if (!sch.isFinished()) {
								//								sch.setOffset(off);
								sch.markFinished();
							}
							//							off += sch.getNumRelations();
						}

						Element update = DomUtils.getChildElementByName(el, "update");
						Element state = DomUtils.getChildElementByName(el, "state");
						if (update == null || state == null) {
							throw new XMLParseException("Missing <update> or <state> tag", el);
						}
						catalog._usf = UpdateStore.deserialize(update);
						catalog._ssf = StateStore.deserialize(state);
					} else if (el.getNodeName().equals("trustConditions")) {
						if (! el.hasAttribute("peer") || ! el.hasAttribute("schema")) {
							throw new XMLParseException("Missing 'peer' or 'schema' attribute", el);
						}
						String peer = el.getAttribute("peer");
						String schemaName = el.getAttribute("schema");
						//Schema s = catalog.getSchemaByName(peer, schemaName);
						// Check for missing peer or schema (will now get null pointer exception)
						//TrustConditions tc = TrustConditions.deserialize(el, s, new StringPeerID(peer));
						TrustConditions tc = TrustConditions.deserialize(el, catalog.getPeers(), new StringPeerID(peer));
						Map<String,TrustConditions> tcForPeer = catalog._tcs.get(peer);
						if (tcForPeer == null) {
							tcForPeer = new HashMap<String,TrustConditions>();
							catalog._tcs.put(peer, tcForPeer);
						}
						tcForPeer.put(schemaName, tc);
					}
				}
			}
			if (catalog._usf == null || catalog._ssf == null) {
				throw new XMLParseException("Missing <store> element to describe state and update stores");
			}
			return catalog;
		} catch (ParserConfigurationException e) {
			assert(false);	// can't happen
		} catch (DuplicatePeerIdException e) {
			throw new XMLParseException("Duplicate peer name " + e.getPeerId());
		} catch (DuplicateSchemaIdException e) {
			throw new XMLParseException("Duplicate schema name " + e.getSchemaId());
		} catch (DuplicateRelationIdException e) {
			throw new XMLParseException("Duplicate relation name " + e.getRelId());
		} catch (UnknownRefFieldException e) {
			throw new XMLParseException("Unknown field in key: " + e);
		} catch (DuplicateMappingIdException e) {
			throw new XMLParseException("Duplicate mapping name " + e.getMappingId());
		}
		return null;
	}

	public InetSocketAddress getBdbStorePort() {
		if (_usf instanceof BerkeleyDBStoreClient.Factory) {
			return ((BerkeleyDBStoreClient.Factory) _usf).host;
		} else {
			return null;
		}
	}

	/**
	 * Parses and unfolds a query and returns a list of rules with provenance
	 * 
	 * @param source
	 * @return
	 * @throws Exception
	 */
	public List<Rule> unfoldQuery(BufferedReader source) throws Exception {
		String next = source.readLine();
		Map<String,RelationContext> localDefs = new HashMap<String,RelationContext>();

		List<Rule> rules = new ArrayList<Rule>();
		while (next != null) {
			// Loop and build a DatalogSequence
			System.out.println("String:"+next.toString());
			if (next.startsWith("--") || next.length() == 0) {
				// Comment: ignore
			} else if (next.startsWith("*")) {
				next = next.substring(1, next.length());
				throw new RecursionException("Found *");
			} else if (next.endsWith(".")) {
				next = next.substring(0, next.length() - 1);
			} else {
				rules.add(Rule.parse(this, next, localDefs));
			}
			next = source.readLine();
		}

		Map<Atom,ProvenanceNode> prov = new HashMap<Atom,ProvenanceNode>();

		Set<String> provenanceRelations = new HashSet<String>();
		// Assume the last rule is the distinguished variable
		return DatalogViewUnfolder.unfoldQuery(rules, rules.get(rules.size() - 1).getHead().getRelationContext().toString(), prov,
				provenanceRelations, "", "", true);
	}	

	/**
	 * Parses, unfolds, and executes a query and returns a list of tuples
	 * with provenance
	 * 
	 * @param source
	 * @return
	 * @throws Exception
	 */
	public List<Tuple> runUnfoldedQuery(BufferedReader source) throws Exception {
		List<Rule> rules = unfoldQuery(source);

		return runUnfoldedQuery(rules, false, "", true); 
	}

	public List<Tuple> runUnfoldedQuery(List<Rule> rules, boolean provenance, String semiringName, boolean returnResults) throws Exception {
		List<Tuple> results = new ArrayList<Tuple>();
		BasicEngine eng = getMappingEngine();

		if(!provenance){
			long time = 0;
			long timeRes = 0;

			for (Rule r : rules) {
				Calendar before = Calendar.getInstance();
				ResultSetIterator<Tuple> result = eng.evalQueryRule(r);
				Calendar after = Calendar.getInstance();
				long oneTime = after.getTimeInMillis() - before.getTimeInMillis();
				System.out.println("EXP: LAST PROVENANCE QUERY: " + oneTime + " msec");
				time += after.getTimeInMillis() - before.getTimeInMillis();

				if(returnResults){
					while (result != null && result.hasNext()) {
						Tuple tuple = result.next();
						tuple.setOrigin(r.getHead().getRelationContext());
						results.add(tuple);
					}
					result.close();
				}
				Calendar afterRes = Calendar.getInstance();
				timeRes += afterRes.getTimeInMillis() - before.getTimeInMillis();
			}
			System.out.println("PROQL EXP: NET PROQL EVAL TIME: " + time + " msec");
			System.out.println("PROQL EXP: TOTAL PROQL EVAL TIME (INCL RESULT CONSTR): " + timeRes + " msec");
		}else{
			long time = 0;
			long timeRes = 0;
			Calendar before = Calendar.getInstance();
			List<ResultSetIterator<Tuple>> res = eng.evalRuleSet(rules, semiringName);
			Calendar after = Calendar.getInstance();
			time += after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("PROQL EXP: NET EVAL TIME: " + time + " msec");

			for(ResultSetIterator<Tuple> result : res){
				if(returnResults){
					while (result != null && result.hasNext()) {
						Tuple tuple = result.next();
						tuple.setOrigin(rules.get(0).getHead().getRelationContext());
						results.add(tuple);
					}
				}
				result.close();
			}
			Calendar afterRes = Calendar.getInstance();
			timeRes += afterRes.getTimeInMillis() - before.getTimeInMillis();

			System.out.println("PROQL EXP: TOTAL EVAL TIME (INCL RESULT CONSTR): " + timeRes + " msec");
		}
		return results;

	}

	/**
	 * Parses and runs a potentially-recursive query
	 * 
	 * @param source
	 * @return
	 * @throws Exception
	 */
	public List<Tuple> runMaterializedQuery(BufferedReader source) throws Exception {
		List<Tuple> results = new ArrayList<Tuple>();

		String next = source.readLine();
		List<Datalog> progs = new ArrayList<Datalog>();
		List<Rule> rules = new ArrayList<Rule>();

		Rule lastRule = null;

		boolean addLastProgram = false;
		boolean lastWasRecursive = false;
		boolean c4f = true;
		while (next != null) {
			// Loop and build a DatalogSequence
			if (next.startsWith("--") || next.length() == 0) {
				// Comment: ignore
			} else {
				boolean hasPeriod = false;
				boolean isRecursive = false;
				c4f = true;

				if (next.startsWith("-")) {
					c4f = false;
					next = next.substring(1, next.length());
				}
				if (next.startsWith("*")) {
					isRecursive = true;
					//lastWasRecursive = true;
					next = next.substring(1, next.length());
				}
				if (next.endsWith(".")) {
					hasPeriod = true;
					next = next.substring(0, next.length() - 1);
				}

				// Single recursive rule: gets its own program
				if (isRecursive) {
					// Close the last program
					if (addLastProgram) {
						if (lastWasRecursive) {
							DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);
							progs.add(prog);
							//lastWasRecursive = false;
						} else {
							DatalogProgram prog = new NonRecursiveDatalogProgram(rules, c4f);//isMigrated());
							progs.add(prog);
						}
					}

					// Create an independent program
					rules = new ArrayList<Rule>();
					Rule r = Rule.parse(this, next);
					lastRule = r;
					rules.add(r);

					// Don't count for fixpoint
					DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);
					progs.add(prog);

					// Create a next program for whatever rules are next
					rules = new ArrayList<Rule>();
					addLastProgram = false;

					// Period: end of a program / stratum
				} else if (hasPeriod) {
					DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);//isMigrated());
					progs.add(prog);
					Rule r = Rule.parse(this, next);
					lastRule = r;
					rules.add(r);
					rules = new ArrayList<Rule>();
					addLastProgram = false;

				} else {
					Rule r = Rule.parse(this, next);
					rules.add(r);
					lastRule = r;
					addLastProgram = true;
					lastWasRecursive = false;
				}
			}
			next = source.readLine();
		}
		// In case there was no terminating period
		if (addLastProgram) {
			//NonRecursiveDatalogProgram prog = new NonRecursiveDatalogProgram(rules, isMigrated());
			//			RecursiveDatalogProgram prog = new RecursiveDatalogProgram(rules, isMigrated());
			//			progs.add(prog);
			if (lastWasRecursive) {
				DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);//isMigrated());
				progs.add(prog);
				lastWasRecursive = false;
			} else {
				DatalogProgram prog = new NonRecursiveDatalogProgram(rules, c4f);//isMigrated());
				progs.add(prog);
			}
		}
		source.close();

		if (!getMappingDb().isConnected())
			getMappingDb().connect();
		DatalogSequence cur = new DatalogSequence(true, progs, false);
		final DatalogEngine de = new DatalogEngine(
				this, getMappingDb());

		while (de.evaluatePrograms(cur) > 0)
			;

		List<Atom> body = new ArrayList<Atom>();
		body.add(lastRule.getHead());
		Rule resultQuery = new Rule(lastRule.getHead(), body, null, lastRule.getDb());

		BasicEngine eng = getMappingEngine();
		ResultSetIterator<Tuple> result = eng.evalQueryRule(resultQuery);

		while (result != null && result.hasNext()) {
			Tuple tuple = result.next();
			results.add(tuple);
		}
		result.close();
		return results;
	}

	public void runMaterializedQuery(String filename) throws Exception {
		BufferedReader source = new BufferedReader(new FileReader(filename));

		runMaterializedQuery(source);
	}


	public List<Datalog> runP2PQuery(String filename) throws Exception {
		BufferedReader source = new BufferedReader(new FileReader(filename));
		String next = source.readLine();
		List<Datalog> progs = new ArrayList<Datalog>();
		List<Rule> rules = new ArrayList<Rule>();
		boolean addLastProgram = false;
		boolean lastWasRecursive = false;
		boolean c4f = true;
		while (next != null) {
			// Loop and build a DatalogSequence
			if (next.startsWith("--") || next.length() == 0) {
				// Comment: ignore
			} else {
				boolean hasPeriod = false;
				boolean isRecursive = false;
				c4f = true;

				if (next.startsWith("-")) {
					c4f = false;
					next = next.substring(1, next.length());
				}
				if (next.startsWith("*")) {
					isRecursive = true;
					//lastWasRecursive = true;
					next = next.substring(1, next.length());
				}
				if (next.endsWith(".")) {
					hasPeriod = true;
					next = next.substring(0, next.length() - 1);
				}

				// Single recursive rule: gets its own program
				if (isRecursive) {
					// Close the last program
					if (addLastProgram) {
						if (lastWasRecursive) {
							DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);
							progs.add(prog);
							//lastWasRecursive = false;
						} else {
							DatalogProgram prog = new NonRecursiveDatalogProgram(rules, c4f);//isMigrated());
							progs.add(prog);
						}
					}

					// Create an independent program
					rules = new ArrayList<Rule>();
					Rule r = Rule.parse(this, next);
					rules.add(r);

					// Don't count for fixpoint
					DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);
					progs.add(prog);

					// Create a next program for whatever rules are next
					rules = new ArrayList<Rule>();
					addLastProgram = false;

					// Period: end of a program / stratum
				} else if (hasPeriod) {
					DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);//isMigrated());
					progs.add(prog);
					Rule r = Rule.parse(this, next);
					rules.add(r);
					rules = new ArrayList<Rule>();
					addLastProgram = false;

				} else {
					Rule r = Rule.parse(this, next);
					rules.add(r);
					addLastProgram = true;
					lastWasRecursive = false;
				}
			}
			next = source.readLine();
		}
		// In case there was no terminating period
		if (addLastProgram) {
			if (lastWasRecursive) {
				DatalogProgram prog = new RecursiveDatalogProgram(rules, c4f);//isMigrated());
				progs.add(prog);
				lastWasRecursive = false;
			} else {
				DatalogProgram prog = new NonRecursiveDatalogProgram(rules, c4f);//isMigrated());
				progs.add(prog);
			}
		}
		source.close();
		return progs;
	}
	public void startStoreServer() throws Exception {
		OrchestraSystem _system = this;
		InetSocketAddress ias = _system.getBdbStorePort();
		if (ias == null) {
			throw new Exception("System does not use a BerkeleyDB Store server");
		}
		// TODO: check that the server should actually be running on
		// the local machine
		if (!_system.isLocalUpdateStore())
			return;

		if (_storeServer == null) {
			//			File f = new File(_system.getName() + "_env");
			File f = new File(storeName + "_env");
			if (! f.exists()) {
				f.mkdir();
			}

			//			File configFile = new File(_system.getName() + ".config");

			EnvironmentConfig ec = new EnvironmentConfig();
			ec.setAllowCreate(true);
			ec.setTransactional(true);
			_env = new Environment(f, ec);
			_storeServer = new BerkeleyDBStoreServer(_env, ias.getPort());
		}
		Map<AbstractPeerID,Integer> peerSchemas = new HashMap<AbstractPeerID,Integer>();
		for (Peer p: _system.getPeers())
			peerSchemas.put(p.getPeerId(), _system.getAllSchemas().indexOf(p.getSchemas().iterator().next()));

		_storeServer.registerAllSchemas(_system.getName(), _system.getAllSchemas(), peerSchemas);

		_system.setSchemaIDBinding(_storeServer.getBinding());
		return;
		//		throw new Exception("Server is alreadying running");
		//	}
	}
	public void stopStoreServer() throws Exception {
		if (_storeServer != null) {
			_storeServer.quit();
			_storeServer = null;
			//			_env.close();
		}
	}

	/**
	 * Tries to execute the query in the current text area
	 * 
	 * @throws Exception
	 */
	public List<Tuple> runProvenanceQuery(String q, boolean printResults, boolean BFS) throws Exception {
		//		String queryString = q.replace('\n', ' ');
		String queryString = q;
		List<Tuple> queryResults = new ArrayList<Tuple>();

		Calendar start = Calendar.getInstance();
		String evalExp = "";
		String pathExp = queryString;
		int eval = queryString.indexOf("EVALUATE");

		if(eval != -1){
			pathExp = queryString.substring(0, eval-1);
			evalExp = queryString.substring(eval);
			Config.setValueProvenance(true);
		}else{
			Config.setValueProvenance(false);
		}

		StringReader sr = new StringReader(pathExp);

		/*
		List<Tuple> results = new ArrayList<Tuple>();

		// Try to run it as non-recursive: more efficient
		try {
			results = _system.runUnfoldedQuery(new BufferedReader(sr));

		// If the program had recursion, we need to run it differently
		} catch (RecursionException re) {
			sr = new StringReader(_query.getText());
			results = _system.runMaterializedQuery(new BufferedReader(sr));
		}

		_data.clear();

//		// TODO:  get the results!!!
		for (Tuple tuple : results) {
			//_results.append(tuple.toString() + "\n");
			_data.addElement(tuple);
		}*/

		/** Some test-case queries to try:
		 * 
		 * For BIOSIMPLEZ:
		 * [PLASMODB.DOMAIN_REF $x] <- [INTERPRO.ENTRY2METH $z]
		 * [PLASMODB.REFSEQ $A] <- []
		 * [PLASMODB.REFSEQ $A] *- []
		 * 
		 * For JOIN3:
		 * [V1] *- []
		 * [V1] <- []
		 */
		Calendar before = Calendar.getInstance();

		Pattern queryPattern = QueryParser.getPatternFor(sr);

		System.out.println(queryPattern.toString());

		String semiringName = QueryParser.getAnnotationType(evalExp);
		String assgnExpr = QueryParser.getAssignmentExpression(evalExp);

		SchemaGraph g = new SchemaGraph(this);
		System.out.println("Schema graph: " + g.toString());

		Set<SchemaSubgraph> results = MatchPatterns.getSubgraphs(g, queryPattern);

		Debug.println("Query results:"); 
		for (SchemaSubgraph sg : results) {
			Debug.println(sg.toString());

			Debug.println("Original program:");
			Calendar qsbefore = Calendar.getInstance();
			List<Rule> rules = sg.toQuerySet();
			Calendar qsafter = Calendar.getInstance();
			long qstime = qsafter.getTimeInMillis() - qsbefore.getTimeInMillis();
			System.out.println("EXP: NET TO QUERY SET TIME: " + qstime + " msec");


			Map<Atom,ProvenanceNode> prov = new HashMap<Atom,ProvenanceNode>();
			Set<String> provenanceRelations = new HashSet<String>();

			// Compute the set of provenance relations -- the unfolder needs to know about them
			for (RelationContext r : this.getMappingEngine().getState().getMappingRelations())
				provenanceRelations.add(r.toString());

			// Assume the last rule is the distinguished variable - greg: NO, this is wrong and gives different results for different runs!!!
			//			List<Rule> program = DatalogViewUnfolder.unfoldQuery(rules, rules.get(0).getHead().getRelationContext().toString(),
			//			Need to add assignment here as well
			List<Rule> program = DatalogViewUnfolder.unfoldQuery(rules, sg.getRootNode().getName(),
					prov, provenanceRelations, semiringName, assgnExpr, BFS);

			System.out.println("\nUnfolded program has " + program.size() + " rules:");
			for (Rule r : program)
				Debug.println(r.toString());
			
			System.out.println("PROV: " + prov.size());
			SqlEngine engine = (SqlEngine)this.getMappingEngine();
			//			List<Rule> programWithASRs = OuterJoinUnfolder.unfoldOuterJoins(program, 
			List<Rule> programWithASRs = OuterJoinUnfolder.unfoldOuterJoins(program,
					engine.getState().getOuterJoinRelations(), 
					AtomType.NONE, AtomType.NONE,
					engine.getMappingDb());
			System.out.println("\nUnfolded program with ASRs (" + programWithASRs.size() + "):");
			for (Rule r : programWithASRs){
				r.setDistinct(false);
				Debug.println(r.toString());
			}

			Calendar after = Calendar.getInstance();
			long time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("PROQL EXP: TOTAL QUERY UNFOLDING TIME : " + time + " msec");
			prov.clear();
			queryResults.addAll(this.runUnfoldedQuery(programWithASRs, true, semiringName, printResults));
			//			List<Tuple> queryResults = _system.runUnfoldedQuery(programWithASRs, false);
		}
		if(printResults)
			System.out.println("EXP: RESULT SIZE : " + queryResults.size());

		Calendar end = Calendar.getInstance();
		long totalTime = end.getTimeInMillis() - start.getTimeInMillis();
		System.out.println("PROQL EXP: TOTAL PROQL TIME : " + totalTime + " msec");

		System.gc();
		return queryResults;
	}
}
