package uk.ac.ox.krr.cardinality.summarisation.factory.typed;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import uk.ac.ox.krr.cardinality.queryanswering.model.SPARQLQuery;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.Reasoner;
import uk.ac.ox.krr.cardinality.queryanswering.util.Prefixes;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;
import uk.ac.ox.krr.cardinality.summarisation.model.TypeManager;
import uk.ac.ox.krr.cardinality.util.Dictionary;
import uk.ac.ox.krr.cardinality.util.Importer;

public class ClassTypedSummary extends TypedSummary {

	private Bucket[] keepSeparated;

	
	public ClassTypedSummary(Summary inputGraph, Dictionary dictionary) {
		super(inputGraph, dictionary);
	}

	public ClassTypedSummary(Summary inputGraph, Dictionary dict, String[] separate) {
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
		System.out.println("Indexing buckets by class");
		TypeManager typeManager = new TypeManager(inputGraph, dictionary);
		if (keepSeparated != null) {
			typeManager.typeByClass(keepSeparated);
		} else {
			typeManager.typeByClass();
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
	
	public static void main(String[] args) throws Exception {
		System.out.println("Typed");
		String filename = "DBLP/facts/dblp-cleaned.ttl";
		Object[] res  = Importer.readGraph(filename, "");
		Dictionary dict = (Dictionary) res[0];
		Summary inputGraph = (Summary) res[1];
		System.out.println("Complex typed...");
		Summary complexSummary = new ComplexTypedSummary(inputGraph, dict).getSummary();
		System.out.println("number of buckets: " + complexSummary.numberOfBuckets());
		System.out.println("\t of which schema buckets" + complexSummary.schemaBuckets.size());
		System.out.println("multiplicity: " + complexSummary.multiplicity());
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("DBLP/summaries/complex-typed.obj"));
		complexSummary.writeExternal(oos);
		oos.close();

	}
	
	protected static void runDBLPQueries(Summary summary, Dictionary dictionary) throws IOException {
		System.out.println("Creating reasoner");
		Reasoner reasoner = new Reasoner(summary);
		System.out.println("Running queries");
		for (int i = 1; i <= 3; i++) {
			Prefixes prefixes = new Prefixes();
			prefixes.declareSemanticWebPrefixes();
			String queryFile = "RDFdata/DBLP/queries/sparql/" + "query" + i + ".sparql";
			SPARQLQuery query = new SPARQLQuery(new String(Files.readAllBytes(Paths.get(queryFile))), dictionary, prefixes);		
			long start = System.currentTimeMillis();
			double answer = reasoner.answer(query);
			System.out.println(i + "\t answer = " + answer + "\t time = " + (System.currentTimeMillis()- start));
		}
	}
	
}
