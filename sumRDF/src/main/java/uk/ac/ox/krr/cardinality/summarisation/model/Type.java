package uk.ac.ox.krr.cardinality.summarisation.model;

import java.util.HashSet;
import java.util.Set;

import uk.ac.ox.krr.cardinality.model.DoublyLinkedList;

public class Type {
	
	final public Set<Bucket> classes;
	final public Set<Bucket> outgoing;
	final public Set<Bucket> ingoing;
	public int dataType;
	public int hashCode;
	
	public Type nextType;
	
	public Type nextInDictionary;
	public Type prevInDictionary;

	
	final public DoublyLinkedList<Bucket> bucketByType;
	
	public int numberOfBuckets;
	
	public Type() {
		classes = new HashSet<>();
		outgoing = new HashSet<>();
		ingoing = new HashSet<>();
		dataType = -1;
		bucketByType = null;
		computeHashCode();
	}
	
	public Type(int dt) {
		classes = new HashSet<>();
		outgoing = new HashSet<>();
		ingoing = new HashSet<>();
		dataType = dt;
		bucketByType = new BucketByTypeList();
		computeHashCode();
	}
	
	public Type(Type t) {
		classes = new HashSet<>(t.classes);
		outgoing = new HashSet<>(t.outgoing);
		ingoing = new HashSet<>(t.ingoing);
		dataType = t.dataType;
		bucketByType = new BucketByTypeList();
		computeHashCode();
	}
	
	public boolean containsClass(Bucket cls) {
		return classes.contains(cls);
	}
	
	public boolean containsIngoing(Bucket predicate) {
		return ingoing.contains(predicate);
	}
	
	public boolean containsOutgoing(Bucket predicate) {
		return outgoing.contains(predicate);
	}
	
	public void addClass(Bucket cls) {
		classes.add(cls);
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}
	
	public void computeHashCode() {
		int result = 0;
		result +=  classes.hashCode();
		result += (result << 10);
		result ^= (result >> 6);
		
		result += dataType;
		result += (result << 10);
		result ^= (result >> 6);
		
		result += ingoing.hashCode();
		result += (result << 10);
		result ^= (result >> 6);
		
		
		result +=  outgoing.hashCode();
		result+= (result << 10);
		result ^= (result >> 6);
		
		result += (result << 3);
		result ^= (result >> 11);
		result += (result << 15);
		hashCode = result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Type)) {
			return false;
		}
		Type other = (Type) obj;
		if (classes == null) {
			if (other.classes != null) {
				return false;
			}
		} else if (!classes.equals(other.classes)) {
			return false;
		}
		if (dataType !=  other.dataType) {
			return false;
		}
		if (ingoing == null) {
			if (other.ingoing != null) {
				return false;
			}
		} else if (!ingoing.equals(other.ingoing)) {
			return false;
		}
		if (outgoing == null) {
			if (other.outgoing != null) {
				return false;
			}
		} else if (!outgoing.equals(other.outgoing)) {
			return false;
		}
		return true;
	}
	
	
	public String toString() {
		return classes + " | " +
				outgoing + " | " + ingoing + " | " + dataType;
	}
	
	
	public static class BucketByTypeList extends DoublyLinkedList<Bucket> {

		@Override
		public Bucket getNext(Bucket element) {
			return element.nextByType;
		}

		@Override
		public Bucket getPrevious(Bucket element) {
			return element.previousByType;
		}

		@Override
		protected void setNext(Bucket element, Bucket next) {
			element.nextByType = next;
		}

		@Override
		protected void setPrevious(Bucket element, Bucket previous) {
			element.previousByType = previous;
		}
		
	}


	public void clear() {
		classes.clear();
		ingoing.clear();
		outgoing.clear();
		dataType = -1;
		
	}

	

	

}
