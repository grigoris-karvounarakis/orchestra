/*
 * CSVStatementHandler.java
 */

package edu.upenn.cis.orchestra.owldb;

import org.openrdf.rio.StatementHandler;

/**
 * This interface extends the StatementHandler interface and adds a method,
 * which writes the data to the files.
 * 
 * @author Soeren Auer
 * @version 0.1, 2007-10-25
 */
public interface CSVStatementHandler extends StatementHandler {

	/**
	 * This method writes the collected data into the csv files.
	 */
	public void writeDataToFiles();
}
