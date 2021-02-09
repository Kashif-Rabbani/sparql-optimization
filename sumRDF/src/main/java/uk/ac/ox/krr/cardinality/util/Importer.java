package uk.ac.ox.krr.cardinality.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;

public class Importer {
	

	public static void main(String[] args) throws RDF4JException, FileNotFoundException, IOException {	
		String filename = "/Users/gstef/Documents/academia/data/DBLP/facts/data.ttl";
		readGraph(filename, "");
	}
	
	public static Object[] readGraph(String pathToFile, String baseIRI) throws RDF4JException,  FileNotFoundException, IOException {
		Object[] result = new Object[2];
		RDFParser parser = Rio.createParser(RDFFormat.TURTLE);

		System.out.println("Starting to parse " + pathToFile);
		GraphHandler handler = new  GraphHandler();
		parser.setRDFHandler(handler);
		parser.parse(new BufferedInputStream(new FileInputStream(pathToFile)), baseIRI);
		result[0] = handler.dictionary;
		result[1] = handler.summary;	
		return result;
	}

	private static class GraphHandler implements RDFHandler {
		
		private int triplesCount = 0;
		private final Resource[] triple;
		private final Dictionary dictionary;
		private final Summary summary;
		private final StringBuilder builder = new StringBuilder();
		
		
		public GraphHandler() {
			triple = new Resource[3];
			dictionary = new Dictionary();
			summary = new Summary();
		}
		
		@Override
		public void startRDF() throws RDFHandlerException {
			System.out.println("Parsing the input graph.");
		}

		@Override
		public void endRDF() throws RDFHandlerException {
			//assert summary.checkIndexes();
			System.out.println("Done parsing.");
			classifyBuckets();
			System.out.println("Total number of buckets: " + summary.numberOfBuckets());
			System.out.println("\t Schema buckets: " + summary.schemaBuckets.size());
			System.out.println("\t Normal buckets: " + summary.buckets.size());
			System.out.println("Summary multiplicity: " +  summary.multiplicity());
		}
		
		private void classifyBuckets() {
			System.out.println("Indexing buckets.");
			Bucket current = summary.buckets.getHead();
			while (current != null) {
				Bucket nextBucket = current.nextBucket;
				if (current.isSchemaBucket) {
					summary.buckets.delete(current);
					summary.schemaBuckets.append(current);
				}
				current = nextBucket;
			}
			System.out.println("Done indexing buckets.");
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {			
		}

		@Override
		public void handleStatement(Statement st) throws RDFHandlerException {
			triple[0] = get(st.getSubject());
			triple[1] = get(st.getPredicate());
			triple[2] = get(st.getObject());
			Edge edge = summary.addDistinctEdge(triple[0], triple[1], triple[2]);
			edge.predicate.isSchemaBucket = true;
			if (triple[1] == dictionary.rdf_type)
				edge.object.isSchemaBucket = true;
			if ((++triplesCount) % 100000 == 0)
				System.out.println("Processed " + triplesCount + " triples; added " + dictionary.size() + " resources to dictionary and " + summary.multiplicity()  + " tuples to the graph.");
		}

		private Resource get(Value value) {
			Resource result;
			if (value instanceof IRI) {
				result = dictionary.getIRIReference(value.stringValue());
			} else if (value instanceof BNode) {
				result = dictionary.getBlankNode(value.stringValue());
			} else if (value instanceof Literal) {
				Literal literal = (Literal) value;
				builder.setLength(0);
				builder.append(literal.getLabel());
				Optional<String> language = literal.getLanguage();
				if (language.isPresent()) {
					builder.append('@');
					builder.append(language.get());
				}
				result = dictionary.getLiteral(builder.toString(), literal.getDatatype().stringValue());				
			} else {
				throw new IllegalArgumentException();
			}
			return result;
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
		}
	}
}
