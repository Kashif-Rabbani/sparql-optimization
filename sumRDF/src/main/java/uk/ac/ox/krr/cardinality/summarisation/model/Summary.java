package uk.ac.ox.krr.cardinality.summarisation.model;

import java.io.Externalizable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;

import uk.ac.ox.krr.cardinality.model.DoublyLinkedList;
import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.EdgeIterator;
import uk.ac.ox.krr.cardinality.summarisation.model.IndexManager.ComplexIndexManager;
import uk.ac.ox.krr.cardinality.summarisation.model.IndexManager.SimpleIndexManager;

public class Summary implements Iterable<Edge>, Externalizable {

	// we maintain a counter for buckets id and the total number of buckets in
	// the current summary.
	// As buckets get merged, we will have that bucketCounter > numberOfBuckets.
	private int bucketCounter;
	private int numberOfBuckets;

	private int multiplicity;

	final public DoublyLinkedList<Bucket> schemaBuckets;
	final public DoublyLinkedList<Bucket> buckets;

	public IndexManager indexManager;
	final public EdgeTable edges;

	final public SummarisationMapping summarisationMapping;
	
	Type headType;


	public Summary() {
		schemaBuckets = new BucketList();
		buckets = new BucketList();
		edges = new EdgeTable();
		numberOfBuckets = 0;
		bucketCounter = 0;
		multiplicity = 0;
		summarisationMapping = new SummarisationMapping();
		indexManager = new SimpleIndexManager();
	}

	public Summary(Summary inputGraph) {
		schemaBuckets = new BucketList();
		buckets = new BucketList();
		edges = new EdgeTable(inputGraph.edges.capacity);
		numberOfBuckets = 0;
		bucketCounter = 0;
		multiplicity = 0;
		summarisationMapping = new SummarisationMapping(inputGraph.summarisationMapping.capacity);
		indexManager = new SimpleIndexManager(inputGraph.indexManager.edgesByS.capacity(), 
				inputGraph.indexManager.edgesByO.capacity());
		Bucket schemaBucket = inputGraph.schemaBuckets.getHead();
		while (schemaBucket != null) {
			assert schemaBucket.size() == 1;
			assert summarisationMapping.get(schemaBucket.getFirst()) == null;
			getOrCreateSchemaBucket(schemaBucket.getFirst()).isSchemaBucket = true;
			schemaBucket = schemaBucket.nextBucket;
		}
		Bucket bucket = inputGraph.buckets.getHead();
		while (bucket != null) {
			assert bucket.size() == 1;
			assert summarisationMapping.get(bucket.getFirst()) == null;
			getOrCreateBucket(bucket.getFirst()).isSchemaBucket = false;
			bucket = bucket.nextBucket;
		}
		for (Edge edge : inputGraph) {
			assert edge.weight == 1;
			addDistinctEdge(edge.subject.resources.getHead(), edge.predicate.resources.getHead(),
					edge.object.resources.getHead());
		}
		assert inputGraph.numberOfBuckets() == numberOfBuckets() && inputGraph.multiplicity() == multiplicity();
	}

	public int numberOfBuckets() {
		return numberOfBuckets;
	}

	public int multiplicity() {
		return multiplicity;
	}

	@Override
	public Iterator<Edge> iterator() {
		return edges.iterator();
	}
	
	public EdgeIterator iteratorByPredicate(int predicateIndex) {
		return indexManager.edgesByP.iterator(predicateIndex);
	}
	
	public EdgeIterator iteratorBySubject(int subjectIndex) {
		return indexManager.edgesByS.iterator(subjectIndex);
	}
	
	public EdgeIterator iteratorByObject(int objectIndex) {
		return indexManager.edgesByO.iterator(objectIndex);
	}
	
	public EdgeIterator iteratorByObjectPredicate(int objectIndex, int predicateIndex) {
		return indexManager.edgesByOP.iterator(objectIndex, predicateIndex);
	}
	
	public EdgeIterator iteratorBySubjectPredicate(int subjectIndex, int predicateIndex) {
		return indexManager.edgesBySP.iterator(subjectIndex, predicateIndex);
	}
	
	public EdgeIterator iteratorByEdge(final int subjectIndex, final int predicateIndex, final int objectIndex) {
		return edges.singleEdgeIterator(subjectIndex, predicateIndex, objectIndex);
	}

	public Type getType() {
		return headType;
	}

	public void addToBucket(Resource r, Bucket b) {
		assert summarisationMapping.get(r) == null;
		b.add(r);
		summarisationMapping.put(r, b);
	}

	public Bucket createBucket() {
		++numberOfBuckets;
		++bucketCounter;
		Bucket bucket = new Bucket(bucketCounter);
		buckets.append(bucket);
		return bucket;
	}
	
	public Bucket getBucket(Resource r) {
		return summarisationMapping.get(r);
	}

	public Bucket getOrCreateSchemaBucket(Resource r) {
		Bucket bucket = summarisationMapping.get(r);
		if (bucket == null) {
			++numberOfBuckets;
			++bucketCounter;
			bucket = new Bucket(bucketCounter, r);
			summarisationMapping.put(r, bucket);
			bucket.isSchemaBucket = true;
			schemaBuckets.append(bucket);
		}
		return bucket;
	}

	public Bucket getOrCreateBucket(Resource r) {
		Bucket bucket = summarisationMapping.get(r);
		if (bucket == null) {
			++numberOfBuckets;
			++bucketCounter;
			bucket = new Bucket(bucketCounter, r);
			summarisationMapping.put(r, bucket);
			buckets.append(bucket);
		}
		return bucket;
	}

	public void shallowMerge(Type t, Bucket stays, Bucket goes) {
		if (goes.bucket_id < stays.bucket_id) {
			shallowMerge(t, goes, stays);
			return;
		}
		int size = stays.size();
		Resource resource = goes.resources.getHead();
		while (resource != null) {
			summarisationMapping.put(resource, stays);
			stays.size++;
			resource = resource.nextInBucket;
		}
		stays.resources.append(goes.resources);
		assert stays.size() == size + goes.size();
		assert t.bucketByType.contains(goes);
		t.bucketByType.delete(goes);
		t.numberOfBuckets--;
		buckets.delete(goes);
		numberOfBuckets--;
		assert t.bucketByType.getHead() != null;
	}

	public void clearEdges() {
		edges.clear();
		indexManager.clear();
		multiplicity = 0;
	}
	
	public Edge getEdge(Bucket subject, Bucket predicate, Bucket object) {
		return edges.get(subject, predicate, object);
	}

	public Edge addEdge(Resource subject, Resource predicate, Resource object) {
		Bucket subjectBucket = getOrCreateBucket(subject);
		Bucket predicateBucket = getOrCreateBucket(predicate);
		Bucket objectBucket = getOrCreateBucket(object);
		Edge edge = edges.get(subjectBucket, predicateBucket, objectBucket);
		if (edge == null) {
			return createNewEdge(subjectBucket, predicateBucket, objectBucket);
		}
		edge.weight++;
		return edge;
	}

	public Edge addDistinctEdge(Resource subject, Resource predicate, Resource object) {
		Bucket subjectBucket = getOrCreateBucket(subject);
		Bucket predicateBucket = getOrCreateBucket(predicate);
		Bucket objectBucket = getOrCreateBucket(object);
		Edge edge = edges.get(subjectBucket, predicateBucket, objectBucket);
		if (edge == null) {
			edge = createNewEdge(subjectBucket, predicateBucket, objectBucket);
		} 
		return edge;
	}
	

	protected Edge createNewEdge(Bucket subjectBucket, Bucket predicateBucket, Bucket objectBucket) {
		Edge edge = new Edge(subjectBucket, predicateBucket, objectBucket);
		edges.add(edge);
		multiplicity++;
		indexManager.index(edge);
		return edge;
	}
	
	public void createFullIndexes() {
		if (indexManager instanceof SimpleIndexManager) {
			SimpleIndexManager oldIndexes = (SimpleIndexManager) indexManager;
			indexManager = new ComplexIndexManager(oldIndexes.edgesByS.capacity(), oldIndexes.edgesByO.capacity());
			oldIndexes.clear();
			for (Edge edge : this) {
				indexManager.index(edge);
			}
		}
	}
	
	public boolean checkIndexes() {
		return indexManager.check(this);
	}

	public static class SummarisationMapping {

		private static final int INIT_CAPACITY = 32;

		private int capacity;

		private int size;

		private int threshold;

		private Resource[] keys;
		private Bucket[] values;

		public SummarisationMapping() {
			this(INIT_CAPACITY);
		}

		public SummarisationMapping(int m) {
			capacity = m;
			size = 0;
			keys = new Resource[m];
			values = new Bucket[m];
			threshold = (int) (capacity /2);
		}

		public int size() {
			return size;
		}

		public boolean isEmpty() {
			return size() == 0;
		}

		private int hash(Resource r) {
			return (r.hashCode() & 0x7fffffff) & (capacity - 1);
		}

		public Bucket get(Resource r) {
			int index = hash(r);
			while (keys[index] != null) {
				if (keys[index].equals(r)) {
					return values[index];
				}
				index = (index + 1) & (capacity - 1);
			}
			return null;
		}

		private void resize(int newCapacity) {
			SummarisationMapping map = new SummarisationMapping(newCapacity);
			for (int i = 0; i < capacity; i++) {
				if (keys[i] != null) {
					map.put(keys[i], values[i]);
				}
			}
			this.capacity = newCapacity;
			this.keys = map.keys;
			this.values = map.values;
			this.threshold = (capacity /2);
		}

		public void put(Resource r, Bucket b) {
			if (size >= threshold) {
				resize(capacity * 2);
			}

			int index = hash(r);

			while (keys[index] != null) {
				if (keys[index].equals(r)) {
					values[index] = b;
					return;
				}
				index = (index + 1) & (capacity - 1);
			}
			// found the right place where to insert
			keys[index] = r;
			values[index] = b;
			size++;
		}
	}

	public static class EdgeTable implements Iterable<Edge> {

		private static final int INIT_CAPACITY = 32;

		private int capacity;
		private int size;
		private int threshold;

		public void clear() {
			size = 0;
			threshold = threshold();
			Arrays.fill(table, null);
		}

		private int threshold() {
			return (int) (capacity * 0.5);
		}

		public Edge[] table;

		public EdgeTable() {
			this(INIT_CAPACITY);
		}

		public EdgeTable(int m) {
			this.capacity = m;
			this.size = 0;
			table = new Edge[capacity];
			threshold = threshold(); // resize if 1/2 full
		}

		public int size() {
			return size;
		}

		public int capacity() {
			return capacity;
		}

		public boolean isEmpty() {
			return size() == 0;
		}

		public int hash(Bucket s, Bucket p, Bucket o) {
			return hash(Edge.hashCode(s, p, o));
		}

		private int hash(Edge e) {
			return hash(e.hashCode);
		}

		private int hash(int hashCode) {
			return (hashCode & 0x7fffffff) & (capacity - 1);
		}

		public Edge get(Bucket s, Bucket p, Bucket o) {
			assert s != null && p != null && o != null;
			int index = hash(s, p, o);
			final int capacityMinusOne =capacity - 1;
			while (table[index] != null) {
				if (table[index].subject == s && table[index].predicate == p && table[index].object == o) {
					return table[index];
				}
				index = (index + 1) & capacityMinusOne;
			}
			return null;
		}

		private void resize(int newCapacity) {
			EdgeTable temp = new EdgeTable(newCapacity);
			for (Edge edge : table) {
				if (edge != null)
					temp.add(edge);
			}
			table = temp.table;
			capacity = temp.capacity;
			threshold = threshold();
		}

		public boolean add(Edge edge) {
			assert edge != null;

			if (size >= threshold) {
				resize(2 * capacity);
			}

			int index = hash(edge);
			Edge current = table[index];
			while (current != null) {
				// the edge already occurs in the table, we stop
				if (current.subject == edge.subject && current.predicate == edge.predicate
						&& current.object == edge.object)
					return false;
				index = (index + 1) & (capacity - 1);
				current = table[index];
			}
			// we have found the right place where to put the edge
			table[index] = edge;
			size++;
			return true;
		}

		@Override
		public Iterator<Edge> iterator() {
			return new TableIterator();
		}
		
		public EdgeIterator singleEdgeIterator(int s, int p, int o) {
			return new SingleEdgeIterator(s, p, o);
		}
		
		protected class SingleEdgeIterator implements EdgeIterator {
			
			private int subject;
			private int predicate;
			private int object;
			private boolean taken;
			private Edge current;
			
			public  SingleEdgeIterator(int subjectIndex, int predicateIndex, int objectIndex) {
				subject = subjectIndex;
				predicate = predicateIndex;
				object = objectIndex;
			}

			@Override
			public void open(Bucket[] tau) {
				current = get(tau[subject], tau[predicate], tau[object]);
				taken = false;
			}

			@Override
			public boolean hasNext() {
				return current != null && !taken;
			}

			@Override
			public Edge next() {
				taken = true;
				return current;
			}
			
		}

		protected class TableIterator implements Iterator<Edge> {

			private int index;
			private Edge current;

			public TableIterator() {
				index = 0;
				while (index < table.length) {
					current = table[index];
					if (current != null)
						break;
					index++;
				}
			}

			@Override
			public boolean hasNext() {
				return index < table.length;
			}

			@Override
			public Edge next() {
				Edge result = current;
				index++;
				while (index < table.length) {
					current = table[index];
					if (current != null)
						break;
					index++;
				}
				return result;
			}

		}
	}

	public static class BucketList extends DoublyLinkedList<Bucket> {

		@Override
		public Bucket getNext(Bucket element) {
			return element.nextBucket;
		}

		@Override
		protected void setNext(Bucket element, Bucket next) {
			element.nextBucket = next;
		}

		@Override
		public Bucket getPrevious(Bucket element) {
			return element.prevBucket;
		}

		@Override
		protected void setPrevious(Bucket element, Bucket previous) {
			element.prevBucket = previous;

		}
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(numberOfBuckets);
		Bucket schemaBucket = schemaBuckets.getHead();
		while (schemaBucket != null) {
			schemaBucket.isSchemaBucket = true;
			schemaBucket.writeExternal(out);
			schemaBucket.getFirst().writeExternal(out);
			schemaBucket = schemaBucket.nextBucket;
		}
		out.flush();
		
		Bucket normalBucket = buckets.getHead();
		while(normalBucket != null) {
			normalBucket.writeExternal(out);
			Resource res = normalBucket.getFirst();
			while(res != null) {
				res.writeExternal(out);
				res = res.nextInBucket;
			}
			normalBucket = normalBucket.nextBucket;
		}
		out.flush();
		
		out.writeInt(multiplicity);
		for (Edge edge : this) {
			edge.subject.getFirst().writeExternal(out);
			edge.predicate.getFirst().writeExternal(out);
			edge.object.getFirst().writeExternal(out);
			out.writeInt(edge.weight);
		}
	}
	
	

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int N = in.readInt();
		System.out.println("Total number of buckets " + N);
		long start = System.currentTimeMillis();
		for (int i = 0; i < N; i++) {
			Bucket bucket = Bucket.read(in);
			int M = bucket.size();
			bucket.size = 0;
			for (int j = 0; j < M; j++) {
				Resource res = Resource.read(in);
				addToBucket(res, bucket);
			}
			if (bucket.isSchemaBucket)
				schemaBuckets.append(bucket);
			else
				buckets.append(bucket);
			numberOfBuckets++;
			System.out.println(i);
			if (i % 100 == 0) {
				System.out.println("Done " + i + " buckets in " + (System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}
		}
		int K = in.readInt();
		System.out.println("Total number of edges " + K);
		start = System.currentTimeMillis();
		for (int i = 0; i < K; i++) {
			Resource s = Resource.read(in);
			Resource p = Resource.read(in);
			Resource o = Resource.read(in);
			Edge edge = createNewEdge(getBucket(s),getBucket(p),getBucket(o));
			edge.weight = in.readInt();
			if (i % 10000 == 0) {
				System.out.println("Done " + i + " edges in " + (System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}
		}
	}
	
	public static Summary read(String file) throws Exception {
		Summary s = new Summary();
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
		s.readExternal(ois);
		ois.close();
		return s;
	}

}
