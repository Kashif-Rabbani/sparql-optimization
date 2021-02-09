package uk.ac.ox.krr.cardinality.summarisation.model;

import java.util.Arrays;

import uk.ac.ox.krr.cardinality.queryanswering.reasoner.EdgeIterator;

public abstract class IndexManager {

	final public SingleKeyHashIndex edgesByS; // first edge with given subject id
	final public SingleKeyHashIndex edgesByP; // first edge with given predicate id
	final public SingleKeyHashIndex edgesByO; // first edge with given object id

	final public TwoKeyHashIndex edgesBySP;
	final public TwoKeyHashIndex edgesByOP;

	public IndexManager() {
		edgesByS = new IndexBySubject();
		edgesByP = new IndexByPredicate();
		edgesByO = new IndexByObject();

		edgesBySP = new IndexBySubjectPredicate();
		edgesByOP = new IndexByObjectPredicate();
	}

	public IndexManager(int sCapacity, int pCapacity, int oCapacity, int spCapacity, int opCapacity) {
		edgesByS = new IndexBySubject(sCapacity);
		edgesByP = new IndexByPredicate(pCapacity);
		edgesByO = new IndexByObject(oCapacity);

		edgesBySP = new IndexBySubjectPredicate(spCapacity);
		edgesByOP = new IndexByObjectPredicate(opCapacity);
	}

	public void clear() {
		edgesByS.clear();
		edgesByP.clear();
		edgesByO.clear();

		edgesBySP.clear();
		edgesByOP.clear();
	}

	public Edge getBySubject(Bucket subject) {
		return edgesByS.get(subject);
	}

	public Edge getByObject(Bucket object) {
		return edgesByO.get(object);
	}
	
	public abstract Edge getByPredicate(Bucket predicate);

	public abstract Edge getBySubjectPredicate(Bucket subject, Bucket predicate);

	public abstract Edge getByObjectPredicate(Bucket object, Bucket predicate);

	public abstract void index(Edge edge);

	public abstract  boolean check(Summary summary);

	
	public static class SimpleIndexManager extends IndexManager {
		
		public SimpleIndexManager() {
			super();
		}
		
		public SimpleIndexManager(int sCapacity, int oCapacity) {
			super(sCapacity, SingleKeyHashIndex.INIT_CAPACITY, oCapacity, TwoKeyHashIndex.INIT_CAPACITY, TwoKeyHashIndex.INIT_CAPACITY);
		}

		@Override
		public boolean check(Summary summary) {
			System.out.println("Checking indexes.");
			Bucket bucket = summary.buckets.getHead();
			while (bucket != null) {
				Edge edge = getBySubject(bucket);
				while (edge != null) {
					if (edge.subject != bucket)
						return false;
					edge = edge.nextSP;
				}
				edge = getByObject(bucket);
				while (edge != null) {
					if (edge.object != bucket)
						return false;
					edge = edge.nextOP;
				}
				bucket = bucket.nextBucket;
			}

			for (Edge edge : summary) {
				if (!checkSPIntegrity(edge) || !checkOPIntegrity(edge))
					return false;
			}
			System.out.println("Indexes are fine.");
			return true;
		}

		@Override
		public Edge getByPredicate(Bucket predicate) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Edge getBySubjectPredicate(Bucket subject, Bucket predicate) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Edge getByObjectPredicate(Bucket object, Bucket predicate) {
			throw new UnsupportedOperationException();
		}

		protected void indexSP(Edge edge) {
			int oIndex = edgesByS.getIndex(edge);
			if (oIndex < 0) {
				edgesByS.add(edge);
			} else {
				Edge oEdge = edgesByS.table[oIndex];
				edge.nextSP = oEdge;
				edgesByS.table[oIndex] = edge;
			}
		}

		protected void indexOP(Edge edge) {
			int oIndex = edgesByO.getIndex(edge);
			if (oIndex < 0) {
				edgesByO.add(edge);
			} else {
				Edge oEdge = edgesByO.table[oIndex];
				edge.nextOP = oEdge;
				edgesByO.table[oIndex] = edge;
			}
		}

		protected boolean checkSPIntegrity(Edge edge) {
			Edge current = edgesByS.get(edge);
			while (current != null) {
				if (current == edge) 
					return true;
				current = current.nextSP;
			}
			System.err.println("SPList Error: edge " + edge + " not found in subject list");
			return false;
		}

		protected boolean checkOPIntegrity(Edge edge) {
			Edge current = edgesByO.get(edge);
			while (current != null ) {
				if (current == edge) 
					return true;
				current = current.nextOP;
			}
			System.err.println("OList Error: edge " + edge + " not found in object list");
			return false;
		}

		@Override
		public void index(Edge edge) {
			indexSP(edge);
			indexOP(edge);
		}
	}

	public static class ComplexIndexManager extends IndexManager {
		
		public ComplexIndexManager() {
			super();
		}
		
		public ComplexIndexManager(int sCapacity, int oCapacity) {
			super(sCapacity, SingleKeyHashIndex.INIT_CAPACITY, oCapacity, TwoKeyHashIndex.INIT_CAPACITY, TwoKeyHashIndex.INIT_CAPACITY);
		}

		@Override
		public boolean check(Summary summary) {
			System.out.println("Checking complex indexes.");
			Bucket bucket = summary.buckets.getHead();
			while (bucket != null) {
				Edge edge = getBySubject(bucket);
				while (edge != null) {
					if (edge.subject != bucket)
						return false;
					edge = edge.nextSP;
				}

				edge = getByObject(bucket);
				while (edge != null) {
					if (edge.object != bucket)
						return false;
					edge = edge.nextOP;
				}

				edge = getByPredicate(bucket);
				while (edge != null) {
					if (edge.predicate != bucket)
						return false;
					edge = edge.nextP;
				}
				bucket = bucket.nextBucket;
			}

			for (Edge edge : summary) {
				if (!checkSPIntegrity(edge) || !checkOPIntegrity(edge) || !checkPIntegrity(edge))
					return false;
			}
			System.out.println("Indexes are fine.");
			return true;
		}
		
		@Override
		public Edge getByPredicate(Bucket predicate) {
			return edgesByP.get(predicate);
		}

		@Override
		public Edge getBySubjectPredicate(Bucket subject, Bucket predicate) {
			return edgesBySP.get(subject, predicate);
		}

		@Override
		public Edge getByObjectPredicate(Bucket object, Bucket predicate) {
			return edgesByOP.get(object, predicate);
		}

		@Override
		public void index(Edge edge) {
			edge.nextSP = null;
			edge.nextP =null;
			edge.nextOP  =null;
			indexSP(edge);
			indexOP(edge);
			indexP(edge);
		}

		protected void indexP(Edge edge) {
			Edge pEdge = edgesByP.get(edge);
			if (pEdge == null) {
				edgesByP.add(edge);
			} else {
				Edge next = pEdge.nextP;
				edge.nextP = next;
				pEdge.nextP = edge;
			}
		}

		protected void indexOP(Edge edge) {
			int opIndex = edgesByOP.getIndex(edge);
			if (opIndex < 0) {
				int oIndex = edgesByO.getIndex(edge);
				if (oIndex < 0) {
					edgesByOP.add(edge);
					edgesByO.add(edge);
				} else {
					Edge oEdge = edgesByO.table[oIndex];
					edge.nextOP = oEdge;
					edgesByO.table[oIndex] = edge;
					edgesByOP.add(edge);
				}
			} else {
				Edge opEdge = edgesByOP.table[opIndex];
				Edge next = opEdge.nextOP;
				edge.nextOP = next;
				opEdge.nextOP = edge;
			}
		}

		protected void indexSP(Edge edge) {
			int spIndex = edgesBySP.getIndex(edge);
			if (spIndex < 0) {
				int sIndex = edgesByS.getIndex(edge);
				if (sIndex < 0) {
					edgesBySP.add(edge); // adding it to the SPindex
					edgesByS.add(edge); // adding it to the SIndex
				} else {
					Edge sEdge = edgesByS.table[sIndex];
					edge.nextSP = sEdge;
					edgesByS.table[sIndex] = edge;
					edgesBySP.add(edge);
				}
			} else {
				Edge spEdge = edgesBySP.table[spIndex];
				Edge next = spEdge.nextSP;
				spEdge.nextSP = edge;
				edge.nextSP = next;
			}
		}

		protected boolean checkSPIntegrity(Edge edge) {
			boolean found = false;
			Edge current = edgesByS.get(edge);
			while (current != null && !found) {
				if (current == edge) {
					found = true;
				}
				current = current.nextSP;
			}
			if (!found) {
				System.err.println("SPList Error: edge " + edge + " not found in subject list");
				return false;
			}

			found = false;
			current = edgesBySP.get(edge);
			while (current != null && !found) {
				if (current == edge) {
					found = true;
				}
				current = current.nextSP;
			}
			if (!found) {
				System.err.println("SPList Error: edge " + edge + " not found in subject-predicate list");
				return false;
			}
			return true;
		}

		protected boolean checkOPIntegrity(Edge edge) {
			Edge current = edgesByO.get(edge);
			boolean found = false;
			while (current != null && !found) {
				if (current == edge) {
					found = true;
				}
				current = current.nextOP;
			}
			if (!found) {
				System.err.println("OList Error: edge " + edge + " not found in object list");
				return false;
			}

			found = false;

			current = edgesByOP.get(edge);
			while (current != null && !found) {
				if (current == edge) {
					found = true;
				}
				current = current.nextOP;
			}
			if (!found) {
				System.err.println("OPList Error: edge " + edge + " not found in object-predicate list");
				return false;
			}
			return true;
		}

		protected boolean checkPIntegrity(Edge edge) {
			Edge current = edgesByP.get(edge);
			boolean found = false;
			while (current != null && !found) {
				if (current == edge) {
					found = true;
				}
				current = current.nextP;
			}
			if (!found) {
				System.err.println("PList Error: edge " + edge + " not found in predicate list");
				return false;
			}
			return true;
		}
	}

	public static abstract class TwoKeyHashIndex {

		private static final int INIT_CAPACITY = 32;

		private int capacity;
		private int size;
		private int threshold;

		public Edge[] table;

		public TwoKeyHashIndex() {
			this(INIT_CAPACITY);
		}

		public TwoKeyHashIndex(int m) {
			this.capacity = m;
			this.size = 0;
			table = new Edge[capacity];
			threshold = threshold();
		}

		public void clear() {
			this.size = 0;
			this.threshold = threshold();
			Arrays.fill(this.table, null);
		}

		public int size() {
			return size;
		}

		public abstract Bucket getFirstKey(Edge edge);

		public abstract Bucket getSecondKey(Edge edge);

		public abstract TwoKeyHashIndex newInstance(int capacity);

		public int capacity() {
			return capacity;
		}

		public boolean isEmpty() {
			return size() == 0;
		}

		private int threshold() {
			return (int) (capacity * 0.75);
		}

		protected int hash(int firstHash, int secondHash) {
			int result = firstHash;
			
		    result += (result << 10);
		    result ^= (result >> 6);
		    
		    result += secondHash;
		    
		    result += (result << 10);
		    result ^= (result >> 6);
		    
		    result += (result << 3);
		    result ^= (result >> 11);
		    result += (result << 15);
		    
			return (result & 0x7fffffff) & (capacity - 1);
		}

		private void resize(int newCapacity) {
			TwoKeyHashIndex temp = newInstance(newCapacity);
			for (Edge edge : table) {
				if (edge != null)
					temp.add(edge);
			}
			table = temp.table;
			capacity = temp.capacity;
			threshold = threshold();
		}

		public int getIndex(Bucket first, Bucket second) {
			int index = hash(first.hashCode(), second.hashCode());
			int capacityMinusOne = capacity-1;

			while (table[index] != null) {
				if (first == getFirstKey(table[index]) && second == getSecondKey(table[index]))
					return index;
				index = (index + 1) & capacityMinusOne;
			}
			return -1;
		}

		public Edge get(Bucket first, Bucket second) {
			int index = hash(first.hashCode(), second.hashCode());
			final int capacityMinusOne = capacity-1;
			while (table[index] != null) {
				if (first == getFirstKey(table[index]) && second == getSecondKey(table[index]))
					return table[index];
				index = (index + 1) & capacityMinusOne;
			}
			return null;
		}

		public int getIndex(Edge edge) {
			return getIndex(getFirstKey(edge), getSecondKey(edge));
		}

		public Edge get(Edge edge) {
			return get(getFirstKey(edge), getSecondKey(edge));
		}

		public void add(Edge edge) {

			if (size >= threshold) {
				resize(2 * capacity);
			}

			Bucket first = getFirstKey(edge);
			Bucket second = getSecondKey(edge);
			int index = hash(first.hashCode(), second.hashCode());
			final int capacityMinusOne = capacity -1;
			while (table[index] != null) {
				if (getFirstKey(table[index]) == first && getSecondKey(table[index]) == second) {
					return;
				}
				index = (index + 1) & capacityMinusOne;
			}
			table[index] = edge;
			size++;
		}
		
		public EdgeIterator iterator(int index1, int index2) {
			return new TwoKeyIndexIterator(index1, index2);
		}
		
		protected abstract Edge getNext(Edge edge);
		
		protected class TwoKeyIndexIterator implements EdgeIterator {

			private final int index1;
			private final int index2;
			private Bucket predicateKey;
			private Edge current;
			
			public  TwoKeyIndexIterator(int index1, int index2) {
				this.index1 = index1;
				this.index2 = index2;
				current = null;
				predicateKey = null;
			}
			
			@Override
			public boolean hasNext() {
				return current != null && current.predicate == predicateKey;
			}

			@Override
			public Edge next() {
				Edge result = current;
				current = getNext(current);
				return result;
			}
			
			@Override
			public void open(Bucket[] tau) {
				assert tau[index1] != null && tau[index2] != null;
				predicateKey = tau[index2];
				current = get(tau[index1], predicateKey);
			}
		}
	}

	public static class IndexBySubjectPredicate extends TwoKeyHashIndex {

		public IndexBySubjectPredicate() {
			super();
		}

		public IndexBySubjectPredicate(int capacity) {
			super(capacity);
		}

		@Override
		public Bucket getFirstKey(Edge edge) {
			return edge.subject;
		}

		@Override
		public Bucket getSecondKey(Edge edge) {
			return edge.predicate;
		}

		@Override
		public IndexBySubjectPredicate newInstance(int capacity) {
			return new IndexBySubjectPredicate(capacity);
		}

		@Override
		protected Edge getNext(Edge edge) {
			return edge.nextSP;
		}

	}

	public static class IndexByObjectPredicate extends TwoKeyHashIndex {

		public IndexByObjectPredicate() {
			super();
		}

		public IndexByObjectPredicate(int c) {
			super(c);
		}

		@Override
		public Bucket getFirstKey(Edge edge) {
			return edge.object;
		}

		@Override
		public Bucket getSecondKey(Edge edge) {
			return edge.predicate;
		}

		@Override
		public TwoKeyHashIndex newInstance(int capacity) {
			return new IndexByObjectPredicate(capacity);
		}

		@Override
		protected Edge getNext(Edge edge) {
			return edge.nextOP;
		}

	}

	public static abstract class SingleKeyHashIndex {

		private static final int INIT_CAPACITY = 32;

		private int capacity;
		private int size;
		private int threshold;

		public Edge[] table;

		public SingleKeyHashIndex() {
			this(INIT_CAPACITY);
		}

		public SingleKeyHashIndex(int m) {
			this.capacity = m;
			this.size = 0;
			table = new Edge[capacity];
			threshold = threshold();
		}

		public void clear() {
			this.size = 0;
			Arrays.fill(table, null);
			threshold = threshold();
		}

		public int size() {
			return size;
		}

		public abstract Bucket getKey(Edge edge);

		public int capacity() {
			return capacity;
		}

		public boolean isEmpty() {
			return size() == 0;
		}

		private int threshold() {
			return (int) (capacity * 0.75);
		}

		protected int hash(int hashCode) {			
			return (hashCode & 0x7fffffff) & (capacity - 1);
		}

		protected abstract SingleKeyHashIndex newInstance(int capacity);

		private void resize(int newCapacity) {
			SingleKeyHashIndex temp = newInstance(newCapacity);
			for (Edge edge : table) {
				if (edge != null)
					temp.add(edge);
			}
			table = temp.table;
			capacity = temp.capacity;
			threshold = threshold();
		}

		public Edge get(Bucket key) {
			int index = hash(key.hashCode());
			final int capacityMinusOne = capacity -1;
			while (table[index] != null) {
				if (getKey(table[index]) == key)
					return table[index];
				index = (index + 1) & capacityMinusOne;
			}
			return null;
		}

		public int getIndex(Bucket key) {
			int index = hash(key.hashCode());
			final int capacityMinusOne = capacity -1;
			while (table[index] != null) {
				if (getKey(table[index]) == key)
					return index;
				index = (index + 1) & capacityMinusOne;
			}
			return -1;
		}

		public int getIndex(Edge edge) {
			return getIndex(getKey(edge));
		}

		public Edge get(Edge edge) {
			return get(getKey(edge));
		}

		public boolean add(Edge edge) {
			assert edge != null;
			if (size >= threshold) {
				resize(2 * capacity);
			}
			Bucket key = getKey(edge);
			int index = hash(key.hashCode());
			final int capacityMinusOne = (capacity -1);
			while (table[index] != null) {
				if (getKey(table[index]) == key)
					return false;
				index = (index + 1) & capacityMinusOne;
			}
			table[index] = edge;
			size++;
			return true;
		}
		
		public abstract Edge getNext(Edge edge);
		
		public EdgeIterator iterator(int index) {
			return new SingleKeyHashIndexIterator(index);
		}
		
		protected class SingleKeyHashIndexIterator implements EdgeIterator {
			private int substitutionIndex;
			private Bucket key;
			private Edge current;
			
			public SingleKeyHashIndexIterator(int index) {
				substitutionIndex = index;
				current = null;
				key = null;
				
			}

			@Override
			public boolean hasNext() {
				return current != null;
			}

			@Override
			public Edge next() {
				Edge result = current;
				current = getNext(result);
				return result;
			}

			@Override
			public void open(Bucket[] tau) {
				this.key = tau[substitutionIndex];
				current = get(key);
			}
		}
	}	

	public static class IndexBySubject extends SingleKeyHashIndex {

		public IndexBySubject() {
			super();
		}

		public IndexBySubject(int capacity) {
			super(capacity);
		}

		@Override
		public Bucket getKey(Edge edge) {
			return edge.subject;
		}

		@Override
		protected IndexBySubject newInstance(int capacity) {
			return new IndexBySubject(capacity);
		}
	
		@Override
		public Edge getNext(Edge edge) {
			return edge.nextSP;
		}

	}

	public static class IndexByPredicate extends SingleKeyHashIndex {

		public IndexByPredicate() {
			super();
		}

		public IndexByPredicate(int capacity) {
			super(capacity);
		}

		@Override
		public Bucket getKey(Edge edge) {
			return edge.predicate;
		}

		@Override
		protected IndexByPredicate newInstance(int capacity) {
			return new IndexByPredicate(capacity);
		}
		
		@Override
		public Edge getNext(Edge edge) {
			return edge.nextP;
		}

	}

	public static class IndexByObject extends SingleKeyHashIndex {

		public IndexByObject() {
			super();
		}

		public IndexByObject(int capacity) {
			super(capacity);
		}

		@Override
		public Bucket getKey(Edge edge) {
			return edge.object;
		}

		@Override
		protected IndexByObject newInstance(int capacity) {
			return new IndexByObject(capacity);
		}
		
		@Override
		public Edge getNext(Edge edge) {
			return edge.nextOP;
		}
	}
}
