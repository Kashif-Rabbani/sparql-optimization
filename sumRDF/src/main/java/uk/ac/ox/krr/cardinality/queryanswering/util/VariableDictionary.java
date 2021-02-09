package uk.ac.ox.krr.cardinality.queryanswering.util;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.model.ResourceType;
import uk.ac.ox.krr.cardinality.util.Dictionary;

public class VariableDictionary {
	
	public static final double LOAD_FACTOR = 0.75;
	protected Resource[] table;
	protected int size;
	protected int threshold;
	
	public VariableDictionary() {
		table = new Resource[32];
		size = 0;
		threshold = (int)(table.length * LOAD_FACTOR);
	}
	
	public Resource getVariable(String variable) {
		final int hashCode = Resource.hashCode(ResourceType.VARIABLE, variable, Dictionary.VARIABLE_DATATYPE);
		final int bucketIndex = hashCode & (table.length - 1);
		Resource resource = table[bucketIndex];
		while (resource != null) {
			if (hashCode == resource.hashCode() 
					&& ResourceType.VARIABLE.equals(resource.resourceType) 
					&& variable.equals(resource.lexicalValue) 
					&& Dictionary.VARIABLE_DATATYPE == resource.datatypeIRIIndex)
				return resource;
			resource = resource.nextInDictionary;
		}
		resource = new Resource(ResourceType.VARIABLE, variable, Dictionary.VARIABLE_DATATYPE, table[bucketIndex]);
		table[bucketIndex] = resource;
		size++;
		if (size >= threshold) {
			final int capacity = table.length * 2;
			final int capacityMinusOne = capacity - 1;
			Resource[] newBuckets = new Resource[capacity];
			for (Resource currentResource : table) {
				while (currentResource != null) {
					Resource nextResource = currentResource.nextInDictionary;
					final int newIndex = currentResource.hashCode() & capacityMinusOne;
					currentResource.nextInDictionary = newBuckets[newIndex];
					newBuckets[newIndex] = currentResource;
					currentResource = nextResource;
				}
			}
			table = newBuckets;
			threshold = (int)(table.length * LOAD_FACTOR);
		}
		return resource;
	}
}