package edu.upenn.cis.orchestra.console;

public interface ConsoleCommand {
	public abstract String name();
	public abstract String params();
	public abstract String help();
	public abstract void execute(String[] args) throws CommandException;
}
