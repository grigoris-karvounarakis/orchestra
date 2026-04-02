package edu.upenn.cis.orchestra.evolution;

import java.sql.SQLException;

public class WrappedSQLException extends Exception {
	static final long serialVersionUID = 0;
	protected SQLException m_exception;
	protected String m_query;
	
	public WrappedSQLException(String query, SQLException e) {
		m_query = query;
		m_exception = e;
	}
	
	public String getQuery() {
		return m_query;
	}
	
	public SQLException getException() {
		return m_exception;
	}
	
	public Integer getSQLState() {
		String sqlstate = m_exception.getSQLState();
		if (sqlstate == null) {
			return null;
		} else {
			return Integer.parseInt(m_exception.getSQLState());
		}
	}

	public String toString() {
		return "Query: " + m_query + "; Exception: " + m_exception.toString();
	}
}
