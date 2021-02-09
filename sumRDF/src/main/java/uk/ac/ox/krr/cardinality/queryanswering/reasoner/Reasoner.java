package uk.ac.ox.krr.cardinality.queryanswering.reasoner;

import uk.ac.ox.krr.cardinality.queryanswering.model.SPARQLQuery;
import uk.ac.ox.krr.cardinality.queryanswering.model.UnificationFreeTD;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;

public class Reasoner {
	
	private Summary summary;
	
	public Reasoner(Summary s) {
		this.summary = s;
		System.out.println("Fully indexing the summary");
		summary.createFullIndexes();
		//assert summary.checkIndexes();
		System.out.println("Done.");
	}
	
	public double approximate(SPARQLQuery query) {
		return new ApproximateEvaluator(summary, query).evaluate();
	}
	
	public double answer(SPARQLQuery query) {
		return new SPARQLEvaluator(summary,query).evaluate();
	}
	
	public double answer(UnificationFreeTD query) {
		return new TDEvaluator(summary, query).evaluate();
	}
	
//	public double standardDeviation(SPARQLQuery query, double expectation, long timeout) {
//		double squaredExp = expectation * expectation;
//		double expDoubleQuery;
//		expDoubleQuery = new TimedSPARQLEvaluator(summary, SPARQLQuery.standardDeviationQuery(query), timeout).evaluate();
//		if (expDoubleQuery < 0) 
//			return -1;
//		return Math.sqrt(expDoubleQuery - squaredExp);	
//	}
//	
	
	

	
}
