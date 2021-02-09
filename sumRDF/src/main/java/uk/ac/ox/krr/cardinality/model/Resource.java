package uk.ac.ox.krr.cardinality.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Resource implements Externalizable {
	
	public Resource nextInBucket;
	
	public ResourceType resourceType;
	public String lexicalValue;
	private int hashCode;
	public Resource nextInDictionary;
	public int datatypeIRIIndex;
	
	public Resource() {
		
	}
	
		
	public Resource(ResourceType type, String lexicalValue, int datatype, Resource nextUnique) {
		this.resourceType = type;
		this.lexicalValue = lexicalValue;
		this.nextInDictionary = nextUnique;
		this.datatypeIRIIndex = datatype;
		this.hashCode = hashCode(this.resourceType, this.lexicalValue, this.datatypeIRIIndex);

	}
	
	@Override
	public int hashCode() {
		return this.hashCode;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		toString(buffer);
		return buffer.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Resource))
			return false;
		Resource r = (Resource)o;
		return  (this.hashCode == r.hashCode && this.resourceType == r.resourceType && this.lexicalValue.equals(r.lexicalValue) && this.datatypeIRIIndex == r.datatypeIRIIndex);
	}

	public static int hashCode(ResourceType type, String lexicalValue, int datatype) {
		int result = 0;
		result += type.hashCode();
		result += (result << 10);
	    result ^= (result >> 6);
	    
	    result += lexicalValue.hashCode();
		result += (result << 10);
	    result ^= (result >> 6);
	    
	    
	    result += datatype;
	    result += (result << 10);
	    result ^= (result >> 6);
	    
	    result += (result << 3);
	    result ^= (result >> 11);
	    result += (result << 15);

		return result;
	    
	}

	public void toString(StringBuffer buffer) {
		final String separator = "|";
		buffer.append(this.resourceType);
		buffer.append(separator);
		buffer.append(this.lexicalValue);
		buffer.append(separator);
		buffer.append(this.datatypeIRIIndex);
		buffer.append(separator);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(lexicalValue);
		out.writeObject(resourceType);
		out.writeInt(datatypeIRIIndex);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		lexicalValue = (String) in.readObject();
		resourceType = (ResourceType) in.readObject();
		datatypeIRIIndex = in.readInt();
		hashCode = hashCode(resourceType, lexicalValue, datatypeIRIIndex);
	}
	
	public static Resource read(ObjectInput in) throws ClassNotFoundException, IOException {
		Resource r = new Resource();
		r.readExternal(in);
		return r;
	}

}
