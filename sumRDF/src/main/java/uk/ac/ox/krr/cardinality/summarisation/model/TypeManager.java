package uk.ac.ox.krr.cardinality.summarisation.model;

import uk.ac.ox.krr.cardinality.model.DoublyLinkedList;
import uk.ac.ox.krr.cardinality.util.Dictionary;

public class TypeManager {

	final private Summary summary;
	final private TypeDictionary typeDictionary;
	final private Dictionary dictionary;

	public TypeManager(Summary s, Dictionary dictionary) {
		this.summary = s;
		this.summary.headType = null;

		this.typeDictionary = new TypeDictionary();
		this.dictionary = dictionary;
	}

	public void typeByDataType() {
		clear();
		Type key = new Type();
		Bucket headBucket = summary.buckets.getHead();
		while (headBucket != null) {
			key.clear();
			key.dataType = headBucket.getFirst().datatypeIRIIndex;
			key.computeHashCode();
			Type bucketType = typeDictionary.get(key);
			if (bucketType == null) {
				bucketType = new Type(key);
				typeDictionary.add(bucketType);
			}
			headBucket.type = bucketType;
			bucketType.bucketByType.append(headBucket);
			headBucket = headBucket.nextBucket;
		}
		for (DoublyLinkedList<Type> list : typeDictionary.table) {
			Type t = list.getHead();
			while (t != null) {
				t.nextType = summary.headType;
				t.numberOfBuckets = t.bucketByType.size();
				summary.headType = t;
				t = t.nextInDictionary;
			}
		}
	}

	public void typeByClass() {
		typeByClass(null);
	}

	public void typeByClass(Bucket[] keepSeparated) {
		clear();
		Type key = new Type();
		final Bucket rdfType = summary.getBucket(dictionary.rdf_type);
		Bucket bucket = summary.buckets.getHead();
		while (bucket != null) {
			key.clear();
			key.dataType = bucket.getFirst().datatypeIRIIndex;
			if (keepSeparated(bucket, rdfType, keepSeparated)) {
				key.addClass(bucket);
			}
			Edge edge = summary.indexManager.edgesByS.get(bucket);
			while (edge != null) {
				if (edge.predicate == rdfType)
					key.addClass(edge.object);
				edge = edge.nextSP;
			}
			key.computeHashCode();

			Type type4Bucket = typeDictionary.get(key);
			if (type4Bucket == null) {
				type4Bucket = new Type(key);
				typeDictionary.add(type4Bucket);
			}
			bucket.type = type4Bucket;
			type4Bucket.bucketByType.append(bucket);
			bucket = bucket.nextBucket;
		}
		int numberOfTypes = 0;
		for (DoublyLinkedList<Type> list : typeDictionary.table) {
			Type t = list.getHead();
			while (t != null) {
				t.nextType = summary.headType;
				t.numberOfBuckets = t.bucketByType.size();
				summary.headType = t;
				t = t.nextInDictionary;
				++numberOfTypes;
			}
		}
		System.out.println("Created " + numberOfTypes + " distinct types");
	}

	public void typeByComplex() {
		clear();
		final Type key = new Type();
		final Bucket rdfType = summary.getBucket(dictionary.rdf_type);
		Bucket bucket = summary.buckets.getHead();
		while (bucket != null) {
			if (!bucket.isSchemaBucket) {
				key.clear();
				key.dataType = bucket.getFirst().datatypeIRIIndex;

				Edge outEdge = summary.indexManager.edgesByS.get(bucket);
				while (outEdge != null) {
					if (outEdge.predicate == rdfType)
						key.addClass(outEdge.object);
					else {
						key.outgoing.add(outEdge.predicate);
					}
					outEdge = outEdge.nextSP;
				}

				Edge inEdge = summary.indexManager.edgesByO.get(bucket);
				while (inEdge != null) {
					key.ingoing.add(inEdge.predicate);
					inEdge = inEdge.nextOP;
				}
				key.computeHashCode();

				Type bucketType = typeDictionary.get(key);
				if (bucketType == null) {
					bucketType = new Type(key);
					typeDictionary.add(bucketType);
				}
				bucket.type = bucketType;
				bucketType.bucketByType.append(bucket);
			}
			bucket = bucket.nextBucket;
		}
		for (DoublyLinkedList<Type> list : typeDictionary.table) {
			Type t = list.getHead();
			while (t != null) {
				t.nextType = summary.headType;
				t.numberOfBuckets = t.bucketByType.size();
				summary.headType = t;
				t = t.nextInDictionary;
			}
		}
	}

	public void typeByComplex(Bucket[] keepSeparated) {
		clear();
		final Type key = new Type();
		final Bucket rdfType = summary.getBucket(dictionary.rdf_type);

		Bucket headBucket = summary.buckets.getHead();
		while (headBucket != null) {
			if (!headBucket.isSchemaBucket) {
				key.clear();
				key.dataType = headBucket.getFirst().datatypeIRIIndex;
				if (keepSeparated(headBucket, rdfType, keepSeparated)) {
					key.addClass(headBucket);
				}
				Edge outEdge = summary.indexManager.edgesByS.get(headBucket);
				while (outEdge != null) {
					if (outEdge.predicate == rdfType)
						key.addClass(outEdge.object);
					else {
						key.outgoing.add(outEdge.predicate);
					}
					outEdge = outEdge.nextSP;
				}

				Edge inEdge = summary.indexManager.edgesByO.get(headBucket);
				while (inEdge != null) {
					key.ingoing.add(inEdge.predicate);
					inEdge = inEdge.nextOP;
				}
				key.computeHashCode();

				Type bucketType = typeDictionary.get(key);
				if (bucketType == null) {
					bucketType = new Type(key);
					typeDictionary.add(bucketType);
				}
				headBucket.type = bucketType;
				bucketType.bucketByType.append(headBucket);
			}
			headBucket = headBucket.nextBucket;
		}
		for (DoublyLinkedList<Type> list : typeDictionary.table) {
			Type t = list.getHead();
			while (t != null) {
				t.nextType = summary.headType;
				t.numberOfBuckets = t.bucketByType.size();
				summary.headType = t;
				t = t.nextInDictionary;
			}
		}
	}

	private boolean keepSeparated(Bucket headBucket, Bucket rdfType, Bucket[] classesToKeepSeperate) {
		if (classesToKeepSeperate == null)
			return false;
		for (Bucket cls : classesToKeepSeperate) {
			if (summary.getEdge(headBucket, rdfType, cls) != null)
				return true;
		}
		return false;
	}

	private void clear() {
		Bucket bucket = summary.buckets.getHead();
		while (bucket != null) {
			bucket.type = null;
			bucket.nextByType = null;
			bucket = bucket.nextBucket;
		}
	}

	public static class TypeDictionary {

		private DoublyLinkedList<Type>[] table;
		private int capacity;
		private int size;

		private final static int INITIAL_CAPACITY = 32;

		public TypeDictionary() {
			this(INITIAL_CAPACITY);
		}

		@SuppressWarnings("unchecked")
		public TypeDictionary(int c) {
			table = (DoublyLinkedList<Type>[]) new DoublyLinkedList[c];
			for (int index = 0; index < table.length; index++) {
				table[index] = new DoublyLinkedList<Type>() {

					@Override
					protected void setPrevious(Type element, Type previous) {
						element.prevInDictionary = previous;

					}

					@Override
					protected void setNext(Type element, Type next) {
						element.nextInDictionary = next;

					}

					@Override
					public Type getPrevious(Type element) {
						return element.prevInDictionary;
					}

					@Override
					public Type getNext(Type element) {
						return element.nextInDictionary;
					}
				};
			}
			this.capacity = c;
			this.size = 0;
		}

		public void remove(Type t) {
			int index = hash(t.hashCode());
			DoublyLinkedList<Type> typeList = table[index];
			if (typeList != null) {
				typeList.delete(t);
			}
			size--;
		}

		protected int hash(int hashCode) {
			return (hashCode & 0x7fffffff) & (capacity - 1);
		}

		public void add(Type t) {
			if (size >= (capacity / 2)) {
				resize(capacity * 2);
			}
			int index = hash(t.hashCode());
			table[index].append(t);
			size++;
		}

		public Type get(Type t) {
			int index = hash(t.hashCode());
			DoublyLinkedList<Type> list = table[index];
			Type head = list.getHead();
			while (head != null) {
				if (head.equals(t))
					return head;
				head = head.nextInDictionary;
			}
			return null;
		}

		protected void resize(int cap) {
			TypeDictionary temp = new TypeDictionary(cap);
			for (DoublyLinkedList<Type> list : table) {
				Type tail = list.getTail();
				while (tail != null) {
					Type prev = tail.prevInDictionary;
					temp.add(tail);
					tail = prev;
				}
			}
			this.table = temp.table;
			this.capacity = cap;
		}
	}
}
