package edu.upenn.cis.orchestra.gui.mappings.graph;

import java.awt.Point;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgraph.graph.DefaultGraphCell;

import com.jgraph.layout.JGraphFacade;
import com.jgraph.layout.organic.JGraphOrganicLayout;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.gui.graphs.BasicGraph;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultEdge;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;

public class MappingGraph extends BasicGraph {

	public static final long serialVersionUID = 1L;
		
	/** 
	 * Create a new mapping graph and run the layout algorithm
	 * @param mapping Mapping to show with the graph
	 */
	public MappingGraph (Mapping mapping)
	{
		super (true);
		setMapping (mapping);
	}
	
	public void setMapping (Mapping mapping)
	{

		getGraphLayoutCache().removeCells(getGraphLayoutCache().getCells(true, true, true, true));
		// Cells list to be added to the graph
		List<DefaultGraphCell> cells = new ArrayList<DefaultGraphCell> ();

		// Create the cell used to relate head and body
		ImplyCell implyCell = new ImplyCell();
		
		// List of cells in the body that need to have an input edge to the 
		// head/body separator
		List<DefaultGraphCell> finalCellsBody = new ArrayList<DefaultGraphCell> ();
		
		
		// Create the body graph
		createJoinsGraph(mapping.getBody(), cells, finalCellsBody, true);
		
		// Create the head graph
		List<DefaultGraphCell> finalCellsHead = new ArrayList<DefaultGraphCell> ();
		createJoinsGraph(mapping.getMappingHead(), cells, finalCellsHead, false);
		
		if (finalCellsBody.size()+finalCellsHead.size()>2)
		{
			cells.add (implyCell);

			// All final joins (or relations if no join) will have an edge to the 
			// implication cell
			for (DefaultGraphCell bodyFinalCell : finalCellsBody)
			{
				GuiDefaultEdge edge = new GuiDefaultEdge ();
				GuiGraphConstants.setLineEnd(edge.getAttributes(), GuiGraphConstants.ARROW_CLASSIC);
				GuiGraphConstants.setLineWidth(edge.getAttributes(), 3.0F);
				edge.setSource(bodyFinalCell.getChildAt(0));
				edge.setTarget(implyCell.getChildAt(0));
				cells.add (edge);
			}
			
			// All final joins in the head (or relations if no join) will have an edge from the 
			// implication cell
			for (DefaultGraphCell headFinalCell : finalCellsHead)
			{
				GuiDefaultEdge edge = new GuiDefaultEdge ();
				GuiGraphConstants.setLineEnd(edge.getAttributes(), GuiGraphConstants.ARROW_CLASSIC);
				GuiGraphConstants.setLineWidth(edge.getAttributes(), 3.0F);
				edge.setSource(implyCell.getChildAt(0));
				edge.setTarget(headFinalCell.getChildAt(0));
				cells.add (edge);
			}
		} else
		{
			GuiDefaultEdge edge = new GuiDefaultEdge ();
			GuiGraphConstants.setLineEnd(edge.getAttributes(), GuiGraphConstants.ARROW_CLASSIC);
			GuiGraphConstants.setLineWidth(edge.getAttributes(), 3.0F);
			edge.setSource(finalCellsBody.get(0).getChildAt(0));
			edge.setTarget(finalCellsHead.get(0).getChildAt(0));
			cells.add (edge);
			
		}

		// Add the cells to the graph
		getGraphLayoutCache().insert(cells.toArray());
		
		
		// Apply the layout
		applyLayout();
		
	}
	
	/**
	 * Create a graph or the relations in the head or body with join cells for 
	 * each set of fields used as a join
	 * @param atoms
	 * @param cells
	 * @param finalCells
	 */
	private void createJoinsGraph (List<Atom> atoms, 
									List<DefaultGraphCell> cells,
									List<DefaultGraphCell> finalCells,
									boolean isBody)
	{
		
		DefaultGraphCell latestJoinCell = null;
		List<Atom> alreadyAddedAtoms = new ArrayList<Atom> (atoms.size());

		for (Atom atom : atoms)
		{
			if (!alreadyAddedAtoms.contains(atom))
			{
				// Create a new cell for this atom
				TableCell tabCell = new TableCell (atom, isBody);
				alreadyAddedAtoms.add (atom);
				cells.add (tabCell);
				latestJoinCell = tabCell;
				
				List<AtomArgument> joinValues = new ArrayList<AtomArgument> ();
				joinValues.addAll (atom.getValues());
				
				Map<List<String>, JoinCell> prevJoins = new HashMap<List<String>, JoinCell> ();						

				
				for (Atom atom2 : atoms)
					if (!alreadyAddedAtoms.contains(atom2))
					{						
						String joinFields = "";
						List<String> locJoins = new ArrayList<String> ();
						for (AtomArgument val : atom2.getValues())
							if (joinValues.contains(val) && !val.toString().equals("-"))
							{
								joinFields = joinFields + (joinFields.length()==0?"":",") + val.toString();
								locJoins.add(val.toString());
							}
						
						if (joinFields.length()>0)
						{
							TableCell tabCell2 = new TableCell(atom2, isBody);
							alreadyAddedAtoms.add(atom2);
							cells.add (tabCell2);

							JoinCell join;
							if (prevJoins.containsKey(locJoins))
								join = prevJoins.get(locJoins);
							else
							{
								join = new JoinCell (joinFields, isBody);
								cells.add (join);
								prevJoins.put (locJoins, join);
								GuiDefaultEdge edge1 = new GuiDefaultEdge ();
								edge1.setSource(latestJoinCell.getChildAt(0));
								edge1.setTarget(join.getChildAt(0));
								cells.add (edge1);
							}
							joinValues.addAll (atom2.getValues());
							
							
							GuiDefaultEdge edge2 = new GuiDefaultEdge ();
							edge2.setSource(tabCell2.getChildAt(0));
							edge2.setTarget(join.getChildAt(0));
							cells.add (edge2);
							
							latestJoinCell = join;
						}
					}
				finalCells.add(latestJoinCell);
			}			
		
		}		
		
	}
	

	
	/**
	 * Apply the graph layout
	 */
	@Override
	public void applyLayout() {
		final JGraphOrganicLayout orgLayout = new JGraphOrganicLayout ();
		orgLayout.setOptimizeBorderLine(false);
		orgLayout.setEdgeLengthCostFactor(0.015);
		orgLayout.setMaxIterations(orgLayout.getMaxIterations()*2);

		JGraphFacade facade = new JGraphFacade (getGraphLayoutCache());
		applyLayout(facade, orgLayout, true, new Point(10,10));
		
	}
		
		

}
