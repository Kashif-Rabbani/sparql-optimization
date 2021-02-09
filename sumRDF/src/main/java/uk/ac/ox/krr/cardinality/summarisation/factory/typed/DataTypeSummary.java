package uk.ac.ox.krr.cardinality.summarisation.factory.typed;

import uk.ac.ox.krr.cardinality.summarisation.model.Summary;
import uk.ac.ox.krr.cardinality.summarisation.model.TypeManager;
import uk.ac.ox.krr.cardinality.util.Dictionary;
import uk.ac.ox.krr.cardinality.util.Importer;

public class DataTypeSummary extends TypedSummary {

	
	public DataTypeSummary(Summary graph, Dictionary dict) {
		super(graph, dict);
	}

	@Override
	protected void createSummary() {
		summary = new Summary();
		System.out.println("Indexing buckets by class");
		TypeManager typeManager = new TypeManager(inputGraph, dictionary);
		typeManager.typeByDataType();
		System.out.println("Done.");
		System.out.println("Assigning resources to buckets.");
		schemaBuckets();
		typeBuckets();
		System.out.println("Done.");
		System.out.println("Creating summary graph.");
		summariseGraph();
		System.out.println("Typed summary successfully created.");
		System.out.println("Summary contains " + summary.numberOfBuckets() + " buckets and " + summary.multiplicity() + " edges.");
	}
	
	public static void main(String[] args) throws Exception {
		String filename = "YAGO/facts/yago-fixed.ttl";
		Object[] res  = Importer.readGraph(filename, "");
		Dictionary dict = (Dictionary) res[0];
		Summary inputGraph = (Summary) res[1];
		System.out.println("number of buckets: " + inputGraph.numberOfBuckets());
		System.out.println("\t of which schema buckets" + inputGraph.schemaBuckets.size());
		System.out.println("multiplicity: " + inputGraph.multiplicity());
		System.out.println("Datatype Summary...");
		Summary dtSummary = new DataTypeSummary(inputGraph, dict).getSummary();
		System.out.println("number of buckets: " + dtSummary.numberOfBuckets());
		System.out.println("\t of which schema buckets" + dtSummary.schemaBuckets.size());
		System.out.println("multiplicity: " + dtSummary.multiplicity());
		

	}
}
