package edu.upenn.cis.orchestra.console;

import java.util.Map;

public abstract class BaseCommand implements ConsoleCommand {
	protected String m_name;
	protected String m_params;
	protected String m_help;
	protected ParameterParser m_parser;
	
	public BaseCommand(String name, String params, String help) {
		m_name = name;
		m_params = params;
		m_help = help;
		m_parser = new ParameterParser(params);
	}
	
	public String name() { 
		return m_name; 
	}

	public String params() {
		return m_params;
	}

	public String help() {
		return m_help;
	}
	
	public String usage() {
		return m_name + "\t" + m_params + "\t" + m_help;
	}
	
	public void execute(String[] args) throws CommandException {
		StringBuffer buf = new StringBuffer();
		for (int i = 1; i < args.length; i++) {
			buf.append(args[i]);
			buf.append(' ');
		}
		Map<String,String> params = m_parser.parse(buf.toString());
		if (params == null) {
			throw new CommandException(shortUsage());
		} else {
			myExecute(params);
		}
	}
	
	protected String shortUsage() {
		return "Usage: " + m_name + " " + m_params;
	}
	
	protected abstract void myExecute(Map<String,String> args) throws CommandException;
}
