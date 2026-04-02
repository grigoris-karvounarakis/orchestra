package edu.upenn.cis.orchestra.gui.schemas.browsertree;

import javax.swing.JTree;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreeSelectionModel;

public class SchemaBrowserTree extends JTree 
{
	public static final long serialVersionUID = 1L;
	
	public SchemaBrowserTree (TreeNode root)
	{
		super (root);
		getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
		
	}
}
