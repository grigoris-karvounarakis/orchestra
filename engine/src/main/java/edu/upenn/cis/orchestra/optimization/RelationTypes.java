package edu.upenn.cis.orchestra.optimization;

import java.util.Collection;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;

/**
 * @author netaylor
 *
 */
public interface RelationTypes<P extends PhysicalProperties,S extends AbstractRelation> {
	public static class TypeException extends Exception {
		private static final long serialVersionUID = 1L;

		public TypeException(String err) {
			super(err);
		}
		
		public TypeException() {
			super();
		}
	}
	
	public class NoSuchRelation extends RuntimeException {
		private static final long serialVersionUID = 1L;
		String relName;
		NoSuchRelation(String relName) {
			super("Relation '" + relName + "' is unknown");
			this.relName = relName;
		}
	}
	
	public class NoSuchColumn extends RuntimeException {
		private static final long serialVersionUID = 1L;
		String relName;
		Integer col;
		String colName;

		NoSuchColumn(String relName, int col) {
			super("Relation '" + relName + "' has no column " + col);
			this.relName = relName;
			this.col = col;
		}
		
		NoSuchColumn(String relName, String colName) {
			super("Relation '" + relName + "' has no column " + colName);
			this.relName = relName;
			this.colName = colName;
		}
	}
	
	/**
	 * Get the type of the specified relation
	 * 
	 * @param name			The name of the relation of interest
	 * @return				The <code>Type</code>s of the columns of that relation
	 * @throws NoSuchRelation	If the specified relation does not exist
	 */
	Type[] getRelationType(String name);
	
	/**
	 * Get the type of the specified column of the specified relation
	 * 
	 * @param name			The name of the relation of interest
	 * @param col			The position in that relation
	 * @return				The <code>Type</code> of that column
	 * @throws NoSuchRelation	If the specified relation does not exist
	 * @throws NoSuchColumn		If the specified column does not exist
	 */
	Type getColumnType(String name, int col);

	/**
	 * Get the position of the specified column of the specified relation
	 * 
	 * @param rel			The name of the relation of interest
	 * @param col			The name of the column of interest
	 * @return				The position of the column within the relation
	 * @throws NoSuchRelation	If the specified relation does not exist
	 * @throws NoSuchColumn		If the specified column does not exist
	 */
	int getColumnPos(String rel, String col);

	/**
	 * Get the names of the columns in the specified relation
	 * 
	 * @param rel			The name of the relation of interest
	 * @return				The list of column names
	 * @throws NoSuchRelation	If the relation does not exist
	 */
	String[] getColumnNames(String rel);
	
	/**
	 * Get the number of columns in the specified relation
	 * 
	 * @param rel			The name of the relation of interest
	 * @return				The number of columns in <code>rel</code>
	 * @throws NoSuchRelation
	 * 						If <code>rel</code> does not exist
	 */
	int getNumColumns(String rel);
	
	/**
	 * Get the physical properties data for the specified relation
	 * 
	 * @param rel			The name of the relation of interest
	 * @return				The physical properties for that relation
	 * @throws NoSuchRelation
	 * 						If <code>rel</code> does not exist
	 */
	P getRelationProps(String rel) throws NoSuchRelation;
	
	RelationMetadata getRelationMetadata(String rel);
	
	VariablePosition getBaseRelationVarPos(String relName);	
	Set<AtomVariable> getAtomVariablesForRelation(String name);
	
	S getBaseRelationSchema(String name) throws NoSuchRelation;
	
	public static class MaterializedView<P extends PhysicalProperties,S extends AbstractRelation> {
		final Expression exp;
		final P props;
		final S schema;
		final VariablePosition varPos;
		
		public MaterializedView(Expression exp, P props, S schema, VariablePosition varPos) {
			this.exp = exp;
			this.props = props;
			this.schema = schema;
			this.varPos = varPos;
		}
	}
	
	Collection<String> getMaterializedViewNames();
	
	MaterializedView<P,S> getMaterializedView(String name) throws NoSuchRelation;
}
