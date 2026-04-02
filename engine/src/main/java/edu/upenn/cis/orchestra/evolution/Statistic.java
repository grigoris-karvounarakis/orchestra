package edu.upenn.cis.orchestra.evolution;

public class Statistic {
	public Union origPlan;
	public Union optPlan;
	public long processorTime;
	public long optimizerCalls;
	public long optimizerTime;
	public int cacheHits;
	public double origCost;
	public double optCost;
	public long origExecuteTime;
	public long optExecuteTime;
	
	public Statistic() {
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("origPlan: ");
		buf.append(origPlan.toString());
		buf.append("\noptPlan: ");
		buf.append(optPlan.toString());
		buf.append("\nprocessorTime: ");
		buf.append(processorTime);
		buf.append(" ms\noptimizerCalls: ");
		buf.append(optimizerCalls);
		buf.append("\noptimizerTime: ");
		buf.append(optimizerTime);
		buf.append(" ms\ncacheHits: ");
		buf.append(cacheHits);
		buf.append("\norigCost: ");
		buf.append(origCost);
		buf.append(" timerons\noptCost: ");
		buf.append(optCost);
		buf.append(" timerons\norigExecuteTime: ");
		buf.append(origExecuteTime);
		buf.append(" ms\noptExecuteTime: ");
		buf.append(optExecuteTime);
		buf.append(" ms");
		return buf.toString();
	}
}
