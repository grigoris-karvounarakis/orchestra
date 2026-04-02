package edu.upenn.cis.orchestra.util;

import java.util.Map;

import org.w3c.dom.Element;

public class XMLParseException extends Exception {
	private static final long serialVersionUID = 1L;

	public final String node;
	
	public XMLParseException(String msg) {
		this(msg,(Element) null);
	}
	
	public XMLParseException(Throwable cause) {
		super(cause);
		this.node = null;
	}
	
	public XMLParseException(String msg, Throwable cause) {
		super(msg,cause);
		this.node = null;
	}
	
	public XMLParseException(String msg, Element el) {
		super(msg);
		this.node = getString(el);
	}
	
	public XMLParseException(Throwable cause, Element el) {
		super(cause);
		this.node = getString(el);
	}
	
	public XMLParseException(String msg, Throwable cause, Element el) {
		super(msg,cause);
		this.node = getString(el);
	}
	
	private String getString(Element el) {
		if (el == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder("<" + el.getTagName());
		Map<String,String> attr = DomUtils.getAttributes(el);
		for (Map.Entry<String, String> att : attr.entrySet()) {
			sb.append(" " + att.getKey() + "=\"" + att.getValue() + "\"");
		}
		sb.append(">");
		return sb.toString();
		
	}
	
	public String toString() {
		if (node == null) {
			return getMessage();
		} else {
			return getMessage() + ": " + node;
		}
	}
}
