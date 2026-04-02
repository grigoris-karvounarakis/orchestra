package edu.upenn.cis.orchestra.p2pqp.plan;

public class NamedLoc extends Location {
	private static final long serialVersionUID = 1L;
	private final String name;
	
	public NamedLoc(String name) {
		if (name == null) {
			throw new NullPointerException();
		}
		this.name = name;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null || (!(o instanceof NamedLoc))) {
			return false;
		}
		
		return name.equals(((NamedLoc) o).name);
	}

	@Override
	public boolean isNamed() {
		return true;
	}
	
	@Override
	public String getName() {
		return name;
	}
}
