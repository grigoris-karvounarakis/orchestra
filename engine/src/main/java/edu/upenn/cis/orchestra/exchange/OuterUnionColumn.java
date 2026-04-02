package edu.upenn.cis.orchestra.exchange;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.RelationField;

/**
 * 
 * @author zives, gkarvoun
 *
 */
public class OuterUnionColumn {
	public OuterUnionColumn(RelationField col, boolean inx, int previous, List<RelationField> src,
			List<RelationField> dist, AtomArgument srcVar) {
		setColumn(col);
		setIndex(inx);
		setSourceColumns(new ArrayList<List<RelationField>>());
		setDistinguishedColumns(new ArrayList<List<RelationField>>());
		setSourceVariables(new ArrayList<AtomArgument>());
		for (int i = 0; i < previous; i++) {
			getSourceColumns().add(null);
			getDistinguishedColumns().add(null);
			getSourceArgs().add(null);
		}
		
		getSourceColumns().add(src);
		getSourceArgs().add(srcVar);
		getDistinguishedColumns().add(dist);
	}
	/**
	 * @param sourceColumns the sourceColumns to set
	 */
	public void setSourceColumns(List<List<RelationField>> sourceColumns) {
		this.sourceColumns = sourceColumns;
	}
	/**
	 * @return the sourceColumns
	 */
	public List<List<RelationField>> getSourceColumns() {
		return sourceColumns;
	}
	/**
	 * @param sourceColumns the sourceColumns to set
	 */
	public void setDistinguishedColumns(List<List<RelationField>> sourceColumns) {
		this.distinguishedColumns = sourceColumns;
	}
	/**
	 * @return the sourceColumns
	 */
	public List<List<RelationField>> getDistinguishedColumns() {
		return distinguishedColumns;
	}
	/**
	 * @param column the column to set
	 */
	public void setColumn(RelationField column) {
		this.column = column;
	}
	/**
	 * @return the column
	 */
	public RelationField getColumn() {
		return column;
	}
	/**
	 * @param isIndex the isIndex to set
	 */
	public void setIndex(boolean isIndex) {
		this.isIndex = isIndex;
	}
	/**
	 * @return the isIndex
	 */
	public boolean isIndex() {
		return isIndex;
	}
	/**
	 * @param sourceVars the sourceVars to set
	 */
	public void setSourceVariables(List<AtomArgument> sourceArgs) {
		this.sourceArgs = sourceArgs;
	}
	/**
	 * @return the sourceVars
	 */
	public List<AtomArgument> getSourceArgs() {
		return sourceArgs;
	}
	public String toString() {
		return column.getName() + ((isIndex) ? "*" : "") + sourceColumns.toString() + "/" + sourceArgs.toString();
	}
	private RelationField column;
	private boolean isIndex;
	private List<List<RelationField>> sourceColumns;
	private List<List<RelationField>> distinguishedColumns;
	private List<AtomArgument> sourceArgs;

	public final static String ORIGINAL_NULL = "NULL";
}
