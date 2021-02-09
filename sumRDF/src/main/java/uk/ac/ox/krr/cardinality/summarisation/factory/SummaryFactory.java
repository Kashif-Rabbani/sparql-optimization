package uk.ac.ox.krr.cardinality.summarisation.factory;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;

public abstract class SummaryFactory {
	
	public final Summary inputGraph;
	protected Summary summary = null;
	
	public SummaryFactory(Summary graph) {
		this.inputGraph = graph;
	}
	
	protected void summariseGraph() {
		summary.clearEdges();
		for (Edge gEdge : inputGraph) {
			summary.addEdge(gEdge.subject.resources.getHead(), 
					gEdge.predicate.resources.getHead(), 
					gEdge.object.resources.getHead());
		}
	}
	
	protected void schemaBuckets() {
		assert inputGraph != null && summary != null;
		Bucket schemaBucket = inputGraph.schemaBuckets.getHead();
		while (schemaBucket != null) {
			assert schemaBucket.size() == 1;
			Resource schemaResource = schemaBucket.resources.getHead();
			summary.getOrCreateSchemaBucket(schemaResource);
			schemaBucket = schemaBucket.nextBucket;
		}
	}
	
	protected abstract void createSummary();
	
	public Summary getSummary() {
		if (summary == null)
			createSummary();
		return summary;
	}
	
	public void update(Summary s) {
		this.summary = s;
		createSummary();
	}

}
