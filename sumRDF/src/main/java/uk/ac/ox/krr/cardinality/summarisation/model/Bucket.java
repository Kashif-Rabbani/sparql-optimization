package uk.ac.ox.krr.cardinality.summarisation.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.model.SinglyLinkedList;

public class Bucket implements Externalizable {
	
	public int bucket_id;
	
	public boolean isSchemaBucket;

	public int size;

	public SinglyLinkedList<Resource> resources;
	
	public Bucket nextBucket;
	public Bucket prevBucket;
	
	public Bucket nextByType;
	public Bucket previousByType;
	
	public int hashCode;

	public int typeIndex;
	
	public Type type;
	
	public Bucket() {
		resources = new ResourceList();
	}
	
	public Bucket(int id) {
		this.bucket_id = id;
		resources = new ResourceList();
		this.isSchemaBucket = false;
		size = 0;
		hashCode = hashCode(id);
	}
	
	protected static int hashCode(int id) {
		int result = id; 
		result += (result << 10);
	    result ^= (result >> 6);
	    result += (result << 3);
	    result ^= (result >> 11);
	    result += (result << 15);
	    return result;
	}

	public Bucket(int id, Resource r) {
		this(id);
		this.resources.append(r);
		size++;
	}
	
	public void add(Resource r) {
		this.resources.append(r);
		size++;
	}
	
	public Resource getFirst() {
		return resources.getHead();
	}
	
	public int size() {
		return size;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("{");
		Resource r = getFirst();
		final int MAX = 1000;
		int counter = 0;
		while (r != null && counter < MAX) {
			builder.append(r.lexicalValue);
			builder.append(" ");
			counter++;
			r = r.nextInBucket;
		}
		builder.append("}:" + size);
		return builder.toString();
	}
	
	@Override
	public int hashCode() {
		return this.hashCode;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Bucket)) {
			return false;
		}
		Bucket bucket = (Bucket)obj;
		return bucket.bucket_id == bucket_id;
	}
	
	public boolean deepEquals(Bucket bucket) {
		if (!equals(bucket))
			return false;
		if (size() != bucket.size())
			return false;
		if (hashCode() != bucket.hashCode())
			return false;
		Resource r1 = getFirst();
		Resource r2 = bucket.getFirst();
		while (r1 != null && r2 != null) {
			if (!r1.equals(r2))
				return false;
			r1 = r1.nextInBucket;
			r2 = r2.nextInBucket;
		}
		return true;
		
	}
	
	public static class ResourceList extends SinglyLinkedList<Resource> {

		@Override
		public Resource getNext(Resource element) {
			return element.nextInBucket;
		}

		@Override
		protected void setNext(Resource element, Resource next) {
			element.nextInBucket = next;
		}		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(bucket_id);
		out.writeBoolean(isSchemaBucket);
		out.writeInt(size);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		bucket_id = in.readInt();
		isSchemaBucket = in.readBoolean();
		size = in.readInt();
		hashCode = hashCode(bucket_id);
	}
	
	public static Bucket read(ObjectInput in) throws ClassNotFoundException, IOException {
		Bucket b = new Bucket();
		b.readExternal(in);
		return b;
	}

}
