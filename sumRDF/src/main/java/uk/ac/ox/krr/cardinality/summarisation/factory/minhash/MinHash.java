package uk.ac.ox.krr.cardinality.summarisation.factory.minhash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import uk.ac.ox.krr.cardinality.summarisation.factory.SummaryFactory;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;
import uk.ac.ox.krr.cardinality.summarisation.model.Type;
import uk.ac.ox.krr.cardinality.summarisation.model.TypeManager;
import uk.ac.ox.krr.cardinality.util.Dictionary;
import uk.ac.ox.krr.cardinality.util.Importer;

public class MinHash extends SummaryFactory {

	final private MinHashConfiguration configuration;
	final private MinHashScheme scheme;
	final private BinManager bins;
	final private Dictionary dictionary;
	
	private Set<Bucket> disallowedOutPredicates;
	private Set<Bucket> disallowedInPredicates;
	
	private int[] signatures;
		
	private final int N;
	private final int M;
	private final int NtimesM;
	
	public MinHash(Dictionary dict, Summary graph, MinHashConfiguration config) {
		super(graph);
		configuration = config;
		scheme = new MinHashScheme(config.schemeRows(), config.schemeCols());
		bins = new BinManager(config.bins);
		disallowedInPredicates = new HashSet<>();
		disallowedOutPredicates = new HashSet<>();
		dictionary = dict;
		N = configuration.scheme_rows;
		M = configuration.scheme_cols + 1;
		NtimesM = N * M;
	}
	
	public void update(Summary s) {
		this.summary = s;
		createSummary();
	}
	
	protected void initialisePredicates() {
		System.out.println("Initialise forbidden predicates.");
		Bucket predicate;
		Bucket rdfType = summary.getBucket(dictionary.rdf_type);
		if (rdfType != null) {
			disallowedOutPredicates.add(rdfType);
			disallowedInPredicates.add(rdfType);
		}
		if (configuration.disallowedOutgoing  !=  null) {
			for (String outPredicate : configuration.disallowedOutgoing) {
				predicate = summary.getBucket(dictionary.getIRIReference(outPredicate));
				assert predicate != null;
				disallowedOutPredicates.add(predicate);
			}
		}
		
		if (configuration.disallowedIngoing  !=  null) {
			for (String inPredicate : configuration.disallowedIngoing) {
				predicate = summary.getBucket(dictionary.getIRIReference(inPredicate));
				assert predicate != null;
				disallowedInPredicates.add(predicate);
			}
		}
		System.out.println("Done.");
	}

	@Override
	protected void createSummary() {
		long start;
		getInitialSummary();

		initialisePredicates();
		
		System.out.println("Typing resources.");
		start = System.currentTimeMillis();
		TypeManager typeManager = new TypeManager(summary, dictionary);
		if (configuration.classTyped) {
			System.out.println("Using class type");
			typeManager.typeByClass();
		} else  {
			System.out.println("Using complex typing");
//			typeManager.complexType(this.disallowedOutPredicates, this.disallowedInPredicates);
			typeManager.typeByComplex();;

		}
		System.out.println("Done in " + (System.currentTimeMillis() - start)/1000 + " seconds.");
		
		initialiseSignatureArray();
		
		boolean merged = true;
		int initialBucketCount, initialMultiplicity, deltaEdges;
		Type headType;
		Bucket bucket, b1, b2;
		int pass = 0;
		while (merged) {
			merged = false;
			headType = summary.getType();
			++pass;
			initialBucketCount = summary.numberOfBuckets();
			initialMultiplicity = summary.multiplicity();
			while (headType != null) {
				createSignature(headType);				
				for (int row = 0; row < configuration.scheme_rows; row++) {
					bins.clear();
					bucket = headType.bucketByType.getHead();
					while (bucket != null) {
						int value = signatures[bucket.typeIndex * NtimesM + row * M + configuration.scheme_cols];
						bins.add(bucket, value);
						bucket = bucket.nextByType;
					}
					for (Iterator<List<Bucket>> it = bins.iterator(); it.hasNext();) {
						List<Bucket> candidates = it.next();
						b1 = candidates.remove(0);
						for (int i = 0; i < candidates.size(); ++i) {
							b2 = candidates.get(i);
							if (similarity(b1, b2) >= configuration.threshold) {
								candidates.remove(b2);
								summary.shallowMerge(headType, b1, b2);
								merged = true;
								if (summary.multiplicity() <= configuration.target)
									return;
								break;
							}
						}
					}
				} 
				headType = headType.nextType;
			}
			System.out.println("Finished pass " + pass);
			System.out.println("Summarising graph again.");
			start = System.currentTimeMillis();
			summariseGraph();
			System.out.println("Done in " + (System.currentTimeMillis() - start)/1000 + " seconds.");
			System.out.println("Current summary has " + summary.numberOfBuckets() + " and " + summary.multiplicity() + " edges.");
			System.out.println("Delta Buckets: " + (initialBucketCount - summary.numberOfBuckets()));
			deltaEdges = initialMultiplicity - summary.multiplicity();
			System.out.println("Delta Edges: " + deltaEdges);
			if (deltaEdges < 1000)
				break;
		}
	}

	protected void getInitialSummary() {
		if (summary == null) {
			System.out.println("Creating new summary.");
			long start = System.currentTimeMillis();
			summary = new Summary(inputGraph);
			System.out.println("Done in " + (System.currentTimeMillis() - start)/1000 + " seconds.");
		} else {
			System.out.println("Updating the given summary.");
		}
		inputGraph.indexManager.clear();
	}

	protected void initialiseSignatureArray() {
		int maxBucketsPerType = 0;
		Type type = summary.getType();
		int count;
		while (type != null) {
			count = 0;
			Bucket bucket = type.bucketByType.getHead();
			while (bucket != null) {
				bucket.typeIndex = count;
				count++;
				bucket = bucket.nextByType;
			}
			if (count > maxBucketsPerType)
				maxBucketsPerType = count;
			type = type.nextType;
		}
		signatures = new int[maxBucketsPerType * NtimesM];
	}

	protected double similarity(Bucket b1, Bucket b2) {
		double value = 0;
		int index1;
		int index2;
		for (int row = 0; row < configuration.scheme_rows; row++) {
			index1 = b1.typeIndex * NtimesM + row*M;
			index2 = b2.typeIndex * NtimesM + row*M;
			for (int col = 0; col < configuration.scheme_cols; col++) {
				if (signatures[index1  + col] == signatures[index2  + col]) {
					value++;
				}
			}
		}
		return value / configuration.scheme_size;
	}

	private void createSignature(Type type) {
		Arrays.fill(signatures, Integer.MAX_VALUE);
		Bucket bucket = type.bucketByType.getHead();
		while (bucket != null) {
			Edge outEdge = summary.indexManager.getBySubject(bucket);
			while (outEdge != null) {
				assert outEdge.subject == bucket;
				if (disallowedOutPredicates == null || !disallowedOutPredicates.contains(outEdge.predicate)) {
					minHash(bucket, outEdge.predicate, outEdge.object);
				}
				outEdge = outEdge.nextSP;
			}
			Edge inEdge = summary.indexManager.getByObject(bucket);
			while (inEdge != null) {
				assert inEdge.object == bucket;
				if (disallowedInPredicates == null || !disallowedInPredicates.contains(inEdge.predicate)) {
					minHash(bucket, inEdge.subject, inEdge.predicate);
				}
				inEdge = inEdge.nextOP;
			}
			
			int index, hash, c;
			for (int r = 0; r < configuration.scheme_rows; r++) {
				index = bucket.typeIndex * NtimesM + r * M;
				hash = 1;
				c = 0;
				for (c = 0; c < configuration.scheme_cols; c++) {
					hash = 31 * hash + signatures[index + c];
				}
				signatures[index + c] = hash;
			}
			bucket = bucket.nextByType;
		}
	}

	protected void minHash(Bucket targetBucket, Bucket edgePoint1, Bucket edgePoint2) {
		int hash, index;
		for (int r = 0; r < this.configuration.scheme_rows; r++) {
			index = targetBucket.typeIndex * NtimesM + r * M;
			for (int c = 0; c < this.configuration.scheme_cols; c++) {
				hash = scheme.hash(edgePoint1.bucket_id, edgePoint2.bucket_id, r, c);
				if (hash < signatures[index + c])
					signatures[index + c] = hash;
			}
		}
	}


	protected static class MinHashScheme {

		int[][] A;
		int[][] B;
		
		public MinHashScheme(short rows, short cols) {
			Random r = new Random();
			A = new int[rows][cols];
			B = new int[rows][cols];
			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < cols; j++) {
					A[i][j] = r.nextInt();
					B[i][j] = r.nextInt();
				}
			}
		}

		public MinHashScheme(long seed, short rows, short cols) {
			Random r = new Random();
			A = new int[rows][cols];
			B = new int[rows][cols];
			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < cols; j++) {
					A[i][j] = r.nextInt();
					B[i][j] = r.nextInt();
				}
			}
		}

		public int hash(int x, int y, int row, int col) {
			int value = (x * 127 + y) * 31;
			return (value * A[row][col]) + B[row][col];
		}

		public int hash(int[] signature) {
			return Arrays.hashCode(signature);
		}
	}

	public static class BinManager implements Iterable<List<Bucket>> {

		final private int capacity;
		private List<Bucket>[] table;

		@SuppressWarnings("unchecked")
		public BinManager(int c) {
			capacity = c;
			table = (ArrayList<Bucket>[]) new ArrayList[capacity];
		}

		protected int hash(int hash) {
			hash = hash ^ (hash >>> 16);
			return (hash & 0x7fffffff) & (capacity - 1);
		}

		public void add(Bucket bucket, int code) {
			int index = hash(code);
			if (table[index] == null) {
				table[index] = new ArrayList<Bucket>();
			}
			table[index].add(bucket);
		}

		@Override
		public Iterator<List<Bucket>> iterator() {
			return new BinIterator();
		}

		public void clear() {
			for (List<Bucket> list : table) {
				if (list != null)
					list.clear();
			}
		}

		protected class BinIterator implements Iterator<List<Bucket>> {

			private int index;
			private List<Bucket> current;

			public BinIterator() {
				index = 0;
				while (index < table.length) {
					current = table[index];
					if (current != null && current.size() > 1)
						break;
					index++;
				}
			}

			@Override
			public boolean hasNext() {
				return index < capacity;
			}

			@Override
			public List<Bucket> next() {
				List<Bucket> result = current;
				index++;
				while (index < table.length) {
					current = table[index];
					if (current != null && current.size() > 1)
						break;
					index++;
				}
				return result;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String filename = "/Users/gstef/Documents/academia/data/watdiv/facts/100/watdiv-100.ttl";
		long start = System.currentTimeMillis();

		Importer.readGraph(filename, "");
		System.out.println("Read summary in " + (System.currentTimeMillis() - start));
//
//		ObjectInputStream ois = new ObjectInputStream(new FileInputStream("input.obj"));
//		Summary inputGraph = new Summary();
//		long start = System.currentTimeMillis();
//		inputGraph.readExternal(ois);
//		System.out.println("Read summary in " + (System.currentTimeMillis() - start));
			

	}

}
