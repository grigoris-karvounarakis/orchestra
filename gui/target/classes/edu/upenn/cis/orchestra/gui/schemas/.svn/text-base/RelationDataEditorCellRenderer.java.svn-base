package edu.upenn.cis.orchestra.gui.schemas;

import java.awt.Component;

import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.DefaultTableCellRenderer;

public class RelationDataEditorCellRenderer extends DefaultTableCellRenderer 
{
	public static final long serialVersionUID = 1L;

	public static RelationDataEditorCellRenderer _sharedInstance = new RelationDataEditorCellRenderer ();


	private RelationDataEditorCellRenderer ()
	{

	}

	@Override
	public Component getTableCellRendererComponent(JTable table, 
			Object val, 
			boolean isSelected, 
			boolean hasFocus, 
			int row, 
			int col) {
		Component cell = super.getTableCellRendererComponent(table, val, isSelected, hasFocus, row, col);
		RelationDataModelAbs absModel = (RelationDataModelAbs) table.getModel();
		try {
			if (isSelected) {
				cell.setForeground(UIManager.getColor("Relation.selected"));
			} else if (absModel.isSkolem(row, col)) {
				cell.setForeground(UIManager.getColor("Relation.skolem"));
			} else {				
				cell.setForeground(UIManager.getColor("Relation.normal"));
			}
		} catch (IndexOutOfBoundsException e) {
			// TODO: why is this happening in the first place?
			cell.setForeground(UIManager.getColor("Relation.normal"));
		}
		return cell;
	}

	public static RelationDataEditorCellRenderer getSharedInstance ()
	{
		return _sharedInstance;
	}

}
