package edu.upenn.cis.orchestra.datamodel.exceptions;

public class RelationNotFoundException extends Exception {

	public static final long serialVersionUID=1L;

	public RelationNotFoundException ()
	{
		super ();
	}

	public RelationNotFoundException (String msg)
	{
		super (msg);
	}

}
