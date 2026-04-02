package edu.upenn.cis.orchestra.repository.dao.exceptions;

public class DAOException extends Exception {
	
	public static final long serialVersionUID=1L;
	
	public DAOException (String msg){
		super (msg);
	}
}
