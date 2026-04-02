package edu.upenn.cis.orchestra.p2pqp;

public interface TupleLoadingObserver {
	int getTupleCountGranularity();
	void loadedTupleCountIs(int count);
	void sentTupleCountIs(int count, int total);

	int getIndexPageGranularity();
	void processedIndexPages(int currCount, int estimatedTotalCount);
}