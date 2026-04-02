package edu.upenn.cis.orchestra.reconciliation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * Persistent binding between Schema objects and integer IDs
 * 
 * @author zives
 *
 */
public class SchemaIDBinding {
	
	/**
	 * BerkeleyDB store for peer & schema info
	 */
	Database _peerSchemaInfo;
	DatabaseConfig _schemaInfoDc;
	Environment _env = null;
	
	/**
	 * Next integer ID to assign
	 */
	static int _nextId = 1;
	
	public static synchronized int getRelationId() {
		return _nextId++;
	}
	
	Map<String,SchemaMap> _cdssSchema;
	
	/**
	 * Create a new binding store, using the BerkeleyDB environment
	 * @param env
	 * @throws DatabaseException
	 */
	public SchemaIDBinding(Environment env) throws DatabaseException {
		_schemaInfoDc = new DatabaseConfig();
		_schemaInfoDc.setAllowCreate(true);
		_schemaInfoDc.setSortedDuplicates(false);
		_schemaInfoDc.setTransactional(false);

		_peerSchemaInfo = env.openDatabase(null, "schemaInfo", _schemaInfoDc);
		_env = env;

		_cdssSchema = new HashMap<String,SchemaMap>();
	}

	/**
	 * Reset the database and the counter
	 * 
	 * @param env
	 * @throws DatabaseException
	 */
	public void clear(Environment env) throws DatabaseException {
		_peerSchemaInfo.close();
		_env = env;
		_env.truncateDatabase(null, "schemaInfo", false);
		_peerSchemaInfo = _env.openDatabase(null, "schemaInfo", _schemaInfoDc);
		_cdssSchema = new HashMap<String,SchemaMap>();
		
		_nextId = 1;
	}
	
	/**
	 * Returns the schema with the requested relation ID
	 * 
	 * @param relCode
	 * @return
	 */
	public Schema getSchemaFor(int relCode) {
		for (String cdss : _cdssSchema.keySet()) {
			Map<Schema,Map<Relation,Integer>> m = _cdssSchema.get(cdss).getRelationIdMap();
			
			for (Schema s: m.keySet()) {
				if (m.get(s).values().contains(new Integer(relCode)))
					return s;
			}
		}
		
		return null;
	}
	
	/**
	 * Returns the relation with the requested ID
	 * @param relCode
	 * @return
	 */
	public Relation getRelationFor(int relCode) {
		for (String cdss : _cdssSchema.keySet()) {
			Map<Schema,Map<Relation,Integer>> m = _cdssSchema.get(cdss).getRelationIdMap();
			
			for (Schema s: m.keySet()) {
				for (Relation r: m.get(s).keySet()) {
					if (r.getRelationID() == relCode)
						return r;
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Returns the relation with the specific name.  Scans
	 * different CDSSs in arbitrary order, so not recommended if
	 * we have the same relation in multiple schemas
	 *
	 * @deprecated
	 * @param nam
	 * @return
	 */
	public Relation getRelationNamed(String nam) {
		for (String cdss : _cdssSchema.keySet()) {
			Map<Schema,Map<Relation,Integer>> m = _cdssSchema.get(cdss).getRelationIdMap();
			
			for (Schema s: m.keySet()) {
				for (Relation r: m.get(s).keySet()) {
					if (r.getName().equals(nam))
						return r;
				}
			}
		}
		
		return null;
	}

	/**
	 * Returns the schema for the named peer
	 * 
	 * @param pid
	 * @return
	 * @throws USException
	 */
	public Schema getSchema(AbstractPeerID pid) throws USException {
		for (String cdss : _cdssSchema.keySet()) {
			Schema s = _cdssSchema.get(cdss).getSchemaForPeer(pid);
			if (s != null)
				return s;
		}
		throw new USException("Cannot find schema for peer " + pid);
		/*
		synchronized (schemas) {
			Schema s = schemas.get(pid);
			if (s == null) {
				throw new USException("Cannot find schema for peer " + pid + " in list " + schemas.keySet().toString());
			} else {
				return s;
			}
		}*/
	}

	/**
	 * Shuts down the storage
	 */
	public void quit() {
		try {
			_peerSchemaInfo.close();
		} catch (DatabaseException d) {
			
		}
	}
	

	/**
	 * For a given CDSS namespace, register all of the schemas and bind the PeerID
	 * to each schema, plus an int ID for each relation
	 * 
	 * @param namespace
	 * @param schemas
	 * @param peerSchema
	 * @return
	 */
	public Map<Schema,Map<Relation,Integer>> registerAllSchemas(String namespace, List<Schema> schemas, 
			Map<AbstractPeerID,Integer> peerSchema) {
		SchemaMap map = _cdssSchema.get(namespace);
		
		boolean save = true;
		if (map == null) {
			try {
				map = getSchemaMap(namespace);
				
				if (map != null) {
					int val = map.getMaxRelationID() + 1;
					if (_nextId < val)
						_nextId = val;
				}
				if (map == null) {
					System.out.println("Unable to find schema map " + namespace);
					map = new SchemaMap();
					map.addSchemas(schemas, peerSchema);
				} else {
					System.out.println("Successfully loaded schema map " + namespace);
					save = false;
				}
			} catch (IOException e) {
				map = new SchemaMap();
				map.addSchemas(schemas, peerSchema);
			} catch (ClassNotFoundException f) {
				map = new SchemaMap();
				map.addSchemas(schemas, peerSchema);
			} catch (DatabaseException d) {
				map = new SchemaMap();
				map.addSchemas(schemas, peerSchema);
			}
		}
		_cdssSchema.put(namespace, map);
		
		if (save) {
			boolean succ = false;
			try {
				succ = saveSchemaMap(namespace);
				_peerSchemaInfo.close();
				_peerSchemaInfo = _env.openDatabase(null, "schemaInfo", _schemaInfoDc);
			} catch (IOException e) {
				System.err.println("Unable to save schemas for " + namespace);
				e.printStackTrace();
			} catch (DatabaseException d) {
				System.err.println("Unable to save schemas for " + namespace);
				d.printStackTrace();
			}
			
			if (succ)
				Debug.println("Successfully saved schema map: " + map.getRelationIdMap());
			else
				System.err.println("Unable to save schema map");
		}
		
		return map.getRelationIdMap();
	}


	/**
	 * Returns the SchemaMap for the requested CDSS namespace
	 * 
	 * @param namespace
	 * @return
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws ClassNotFoundException
	 */
	public SchemaMap getSchemaMap(String namespace) throws IOException, DatabaseException,
	ClassNotFoundException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(namespace);
	
		DatabaseEntry key = new DatabaseEntry(bbw.getByteArray());
	
		DatabaseEntry data = new DatabaseEntry();
		
		OperationStatus os2 = _peerSchemaInfo.get(null, key, data, LockMode.DEFAULT);
		
		if (os2 == OperationStatus.SUCCESS) {
			ByteArrayInputStream schMap = new ByteArrayInputStream(data.getData());
		
			ObjectInputStream os = new ObjectInputStream(schMap);
		
			SchemaMap map = (SchemaMap)os.readObject();
			return map;
		} else
			return null;
	}

	/**
	 * Adds a new relation and schema to the namespace
	 * 
	 * @param namespace
	 * @param schema
	 * @param rel
	 * @return
	 */
	public int registerNewRelation(String namespace, Schema schema, Relation rel) {
		SchemaMap map = _cdssSchema.get(namespace);
	
		map.addMappingforSchema(schema);
		
		return map.getIdforRelation(schema, rel);
	}

	/**
	 * Saves the Schema ID Binding to disk
	 * 
	 * @param namespace
	 * @return
	 * @throws IOException
	 * @throws DatabaseException
	 */
	private boolean saveSchemaMap(String namespace) throws IOException, DatabaseException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(namespace);
	
		DatabaseEntry key = new DatabaseEntry(bbw.getByteArray());
		
		ByteArrayOutputStream schMap = new ByteArrayOutputStream();
		
		SchemaMap map = _cdssSchema.get(namespace);
		
		ObjectOutputStream os = new ObjectOutputStream(schMap);
		
		os.writeObject(map);
		
		os.flush();
	
		DatabaseEntry data = new DatabaseEntry(schMap.toByteArray());
		
		OperationStatus os2 = _peerSchemaInfo.put(null, key, data);
		
		try {
			if (os2 == OperationStatus.SUCCESS) {
				SchemaMap ret = getSchemaMap(namespace);
				
				if (ret != null)
					System.out.println("Successfully verified schema " + namespace);
			}
		} catch (Exception e) {
			
		}
		
		return (os2 == OperationStatus.SUCCESS);
	}


	/**
	 * Mapping between Peers, Schemas, Relations, and int IDs for the relations
	 * 
	 * @author zives
	 *
	 */
	static class SchemaMap implements Serializable {
		public SchemaMap() {
			_schemaRelationIdMap = new HashMap<Schema,Map<Relation,Integer>>();
			_peerSchemaMap = new HashMap<AbstractPeerID,Schema>();
		}
		
		public Map<Schema,Map<Relation,Integer>> getRelationIdMap() {
			return _schemaRelationIdMap;
		}
		
		public void addSchemas(List<Schema> schemas, Map<AbstractPeerID,Integer> peers){
			for (AbstractPeerID p : peers.keySet())
				_peerSchemaMap.put(p, schemas.get(peers.get(p).intValue()));
			
			for (Schema s: schemas)
				addMappingforSchema(s);
		}
		
		public int getMaxRelationID() {
			int ret = Integer.MIN_VALUE;
			for (Schema s: _schemaRelationIdMap.keySet())
				for (Integer i : _schemaRelationIdMap.get(s).values())
					if (i > ret)
						ret = i;
			
			return ret;
		}
		
		/**
		 * Takes a given schema and adds an int mapping for each relation
		 * @param s
		 */
		public void addMappingforSchema(Schema s) {
			Map<Relation,Integer> relMap;
			
			synchronized (_schemaRelationIdMap) {
				relMap = _schemaRelationIdMap.get(s); 
				if (relMap == null) {
					relMap = new HashMap<Relation,Integer>();
					_schemaRelationIdMap.put(s, relMap);
				}
				for (Relation r: s.getRelations()) {
					if (!relMap.containsKey(r))
						relMap.put(r, r.getRelationID());// new Integer(getRelationId()));
					
					r.setRelationID(relMap.get(r));
				}
			}
			s.resetIDs();
		}
		
		public int getIdforRelation(Schema s, Relation r) {
			synchronized(_schemaRelationIdMap) {
				return _schemaRelationIdMap.get(s).get(r).intValue();
			}
		}
		
		public Schema getSchemaForPeer(AbstractPeerID p) {
			synchronized (_peerSchemaMap) {
				return _peerSchemaMap.get(p);
			}
		}
		
		public void addPeerSchema(AbstractPeerID p, Schema s) {
			synchronized (_peerSchemaMap) {
				_peerSchemaMap.put(p, s);
			}
		}
		
		Map<Schema,Map<Relation,Integer>> _schemaRelationIdMap;
		Map<AbstractPeerID,Schema> _peerSchemaMap;
		
		public static final long serialVersionUID = 1;
	}
}