package uk.ac.ox.krr.cardinality.summarisation.factory.typed;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.eclipse.rdf4j.RDF4JException;

import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;
import uk.ac.ox.krr.cardinality.summarisation.model.TypeManager;
import uk.ac.ox.krr.cardinality.util.Dictionary;
import uk.ac.ox.krr.cardinality.util.Importer;

public class ComplexTypedSummary extends TypedSummary {
	
	
	private Bucket[] keepSeparated;
	
	public ComplexTypedSummary(Summary inputGraph, Dictionary dict) {
		super(inputGraph, dict);
	}
	
	public ComplexTypedSummary(Summary inputGraph, Dictionary dict, String[] separate) {
		super(inputGraph, dict);
		keepSeparated = new Bucket[separate.length];
		for (int index = 0; index < keepSeparated.length; index++) {
			keepSeparated[index] = inputGraph.getBucket(dict.getIRIReference(separate[index]));
			assert keepSeparated[index] != null;
		}
	}

	@Override
	protected void createSummary() {
		summary = new Summary();
		System.out.println("Indexing buckets by class, and outgoing and ingoing predicates.");
		TypeManager typeManager = new TypeManager(inputGraph,dictionary);
		if (keepSeparated != null) {
			typeManager.typeByComplex(keepSeparated);
		} else {
			typeManager.typeByComplex();
		}
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
	
	public static void main(String[] args) throws RDF4JException, FileNotFoundException, IOException {
		String filename = "/Users/gstef/Documents/academia/data/LUBM/facts/LUBM-010-mat.ttl";
		Object[] res  = Importer.readGraph(filename, "");
		Dictionary dict = (Dictionary) res[0];
		Summary inputGraph = (Summary) res[1];
		long start = System.currentTimeMillis();
		Summary result = new ComplexTypedSummary(inputGraph, dict, new String[] {"http://swat.cse.lehigh.edu/onto/univ-bench.owl#Course"}).getSummary();
		System.out.println("Done in " + (System.currentTimeMillis() - start) /1000);
		System.out.println(result.numberOfBuckets() +  " " + result.multiplicity());
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("lubm-complex-001.obj"));
		result.writeExternal(oos);
		oos.close();
	}
}
