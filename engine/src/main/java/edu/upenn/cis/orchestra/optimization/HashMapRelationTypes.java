package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;

public class HashMapRelationTypes<P extends PhysicalProperties,S extends AbstractRelation> implements RelationTypes<P,S> {


	private static class Column {
		public String name;
		public Type type;
		public Column(String name, Type type) {
			this.name = name;
			this.type = type;
		}
	}
	
	private final Map<String,List<Column>> columns = new HashMap<String,List<Column>>();
	private final Map<String,P> relationProps = new HashMap<String,P>();
	private final Map<String,VariablePosition> relationVarPos = new HashMap<String,VariablePosition>();
	private final Map<String,Set<AtomVariable>> relationAtoms = new HashMap<String,Set<AtomVariable>>();
	private final Map<String,S> relationSchemas = new HashMap<String,S>();	
	private final Map<String,RelationMetadata> metadata = new HashMap<String,RelationMetadata>();
	private final Map<String,MaterializedView<P,S>> materializedViews = new HashMap<String,MaterializedView<P,S>>();
	
	public HashMapRelationTypes() {
		
	}
	
	/**
	 * Given an Orchestra TableSchema, initialize based
	 * on the DB types
	 * 
	 * @author zives
	 * @param rel TableSchema type from Orchestra repository
	 */

	public HashMapRelationTypes(S rel, P relationProps, List<Histogram<?>> relationColHistograms) {
		addRelation(rel, relationProps, relationColHistograms);
	}

	
	public void addRelation(S schema, P storedRelationProps, List<Histogram<?>> storedRelationHistograms) {
		if (! schema.isFinished()) {
			throw new IllegalArgumentException("Cannot add a non-finished schema to a RelationTypes object");
		}
		final int size = schema.getNumCols();
		if (storedRelationHistograms.size() != size) {
			throw new IllegalArgumentException("Incorrect number of histograms supplied");
		}
		final String relName = schema.getRelationName();
		if (relationSchemas.containsKey(relName)) {
			throw new IllegalArgumentException("Duplication relation name " + relName);
		}
		relationSchemas.put(relName, schema);
		
		List<Column> cols = new ArrayList<Column>(size);
		columns.put(relName, cols);
		relationProps.put(relName, storedRelationProps);
		
		for (int i = 0; i < size; ++i) {
			cols.add(new Column(schema.getColName(i), schema.getColType(i).getOptimizerType()));
		}

		
		VariablePosition varPos = new VariablePosition(size);
		List<AtomVariable> avs = new ArrayList<AtomVariable>(size);
		for (int i = 0; i < size; ++i) {
			AtomVariable av = new AtomVariable(relName, 1, i, this);
			varPos.addVariable(av);
			avs.add(av);
		}
		varPos.finish();
		relationVarPos.put(relName, varPos);
		relationAtoms.put(relName, new HashSet<AtomVariable>(avs));

		// Generate primary keys, foreign keys etc.
		Map<Variable,Histogram<?>> colHists = new HashMap<Variable,Histogram<?>>(size);
		for (AtomVariable v : avs) {
			Histogram<?> hist = storedRelationHistograms.get(v.position);
//			if (hist == null) {
//				throw new NullPointerException("Histograms cannot be null");
//			}
			colHists.put(v, hist);
		}
		
		Set<Variable> primaryKey = new HashSet<Variable>();
		PrimaryKey pk = schema.getPrimaryKey();
		if (pk == null) {
			// No primary key is the same as all elements being the primary key
			primaryKey.addAll(avs);
		} else {
			for (RelationField f : pk.getFields()) {
				primaryKey.add(avs.get(schema.getColNum(f.getName())));
			}
		}
		
		List<RelationMetadata.ForeignKeyDef> fks = new ArrayList<RelationMetadata.ForeignKeyDef>();

		for (ForeignKey fk : schema.getForeignKeys()) {
			List<Variable> thisVars = new ArrayList<Variable>();
			
			for (RelationField f : fk.getFields()) {
				thisVars.add(avs.get(schema.getColNum(f.getName())));
			}
			
			S otherSchema = getBaseRelationSchema(fk.getRefRelation().getName());
			String otherRel = otherSchema.getName();
			List<Integer> otherCols = new ArrayList<Integer>();
			for (RelationField f : fk.getRefFields()) {
				otherCols.add(otherSchema.getColNum(f.getName()));
			}
			fks.add(new RelationMetadata.ForeignKeyDef(otherRel, otherCols, thisVars));
		}
		
		metadata.put(relName, new RelationMetadata(primaryKey, fks, colHists, RelationMetadata.FunctionalDependencies.EMPTY));
	}
	
	public void addMaterializedView(MaterializedView<P,S> mv) {
		materializedViews.put(mv.schema.getName(), mv);
	}
	
	public String[] getColumnNames(String rel) throws NoSuchRelation {
		List<Column> cols = columns.get(rel);
		if (cols == null) {
			throw new NoSuchRelation(rel);
		}
		String[] retval = new String[cols.size()];
		for (int i = 0; i < cols.size(); ++i) {
			retval[i] = cols.get(i).name;
		}
		
		return retval;
	}

	public int getColumnPos(String rel, String col) throws NoSuchRelation,
			NoSuchColumn {
		// TODO: use an index instead of search
		List<Column> cols = columns.get(rel);
		if (cols == null) {
			throw new NoSuchRelation(rel);
		}
		
		for (int i = 0; i < cols.size(); ++i) {
			if (cols.get(i).name.equals(cols)) {
				return i;
			}
		}
		
		throw new NoSuchColumn(rel,col);
	}

	public Type getColumnType(String rel, int col) throws NoSuchRelation,
			NoSuchColumn {
		List<Column> cols = columns.get(rel);
		if (cols == null) {
			throw new NoSuchRelation(rel);
		}
		
		try {
			return cols.get(col).type;
		} catch (IndexOutOfBoundsException ioobe) {
			throw new NoSuchColumn(rel,col);
		}
	}

	public Type[] getRelationType(String rel) throws NoSuchRelation {
		List<Column> cols = columns.get(rel);
		if (cols == null) {
			throw new NoSuchRelation(rel);
		}
		Type[] retval = new Type[cols.size()];
		for (int i = 0; i < cols.size(); ++i) {
			retval[i] = cols.get(i).type;
		}
		
		return retval;
	}

	public int getNumColumns(String rel) throws NoSuchRelation {
		List<Column> cols = columns.get(rel);
		if (cols == null) {
			throw new NoSuchRelation(rel);
		}
		return cols.size();
	}

	public P getRelationProps(String rel) throws NoSuchRelation {
		P props = relationProps.get(rel);
		if (props == null) {
			throw new NoSuchRelation(rel);
		}
		return props;
	}
	
	public RelationMetadata getRelationMetadata(String relation) throws NoSuchRelation {
		RelationMetadata md = metadata.get(relation);
		if (md == null) {
			throw new NoSuchRelation(relation);
		}
		return md;
	}

	
	public Set<AtomVariable> getAtomVariablesForRelation(String name) {
		Set<AtomVariable> atoms = relationAtoms.get(name);
		if (atoms == null) {
			throw new NoSuchRelation(name);
		}
		return atoms;
	}

	
	public S getBaseRelationSchema(String name) {
		S schema = relationSchemas.get(name);
		if (schema == null) {
			throw new NoSuchRelation(name);
		}
		return schema;
	}

	
	public VariablePosition getBaseRelationVarPos(String relName)
			throws NoSuchRelation {
		VariablePosition varPos = relationVarPos.get(relName);
		if (varPos == null) {
			throw new NoSuchRelation(relName);
		}
		return varPos;
	}

	public MaterializedView<P,S> getMaterializedView(String name) throws NoSuchRelation {
		MaterializedView<P,S> retval = materializedViews.get(name);
		if (retval == null) {
			throw new NoSuchRelation(name);
		}
		return retval;
	}

	public Collection<String> getMaterializedViewNames() {
		return Collections.unmodifiableSet(materializedViews.keySet());
	}

	public Map<String,S> getAllSchemas() {
		HashMap<String,S> schemas = new HashMap<String,S>();
		schemas.putAll(relationSchemas);
		for (MaterializedView<?,S> mv : this.materializedViews.values()) {
			schemas.put(mv.schema.getName(), mv.schema);
		}
		return schemas;
	}
}
