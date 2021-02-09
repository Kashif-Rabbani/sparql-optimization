package uk.ac.ox.krr.cardinality.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.model.ResourceType;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;

public class Dictionary implements Iterable<Resource> {
	
	public static final double LOAD_FACTOR = 0.75;
	public static final int EMPTY_IRI = 0;
	public static final int IRI_DATATYPE = 1;
	public static final int BLANK_NODE_DATATYPE = 2;
	public static final int XML_STRING_DATATYPE = 3;
	public static final int VARIABLE_DATATYPE = 4;
	public static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

	public final List<String> m_IRIsByIndex;
	protected final HashMap<String, Integer> m_IRIsToIndexes;
	protected Resource[] m_buckets;
	protected int m_size;
	protected int m_resizeThreshold;
	public final Resource rdf_type;
	
	public Dictionary() {
		m_IRIsByIndex = new ArrayList<String>();
		m_IRIsToIndexes = new HashMap<>();
		
		m_IRIsByIndex.add("");
		m_IRIsToIndexes.put("", EMPTY_IRI);
		
		m_IRIsByIndex.add("internal:iri");
		m_IRIsToIndexes.put("internal:iri", IRI_DATATYPE);
		
		m_IRIsByIndex.add("internal:blank-node");
		m_IRIsToIndexes.put("internal:blank-node", BLANK_NODE_DATATYPE);
		
		m_IRIsByIndex.add("http://www.w3.org/2001/XMLSchema#string");
		m_IRIsToIndexes.put("http://www.w3.org/2001/XMLSchema#string", XML_STRING_DATATYPE);
		
		m_IRIsByIndex.add("interval:variable");
		m_IRIsToIndexes.put("internal:variable", VARIABLE_DATATYPE);

		
		m_buckets = new Resource[16];
		m_size = 0;
		m_resizeThreshold = (int)(m_buckets.length * LOAD_FACTOR);
		rdf_type = getIRIReference(RDF_TYPE);
	}
	
	public int size() {
		return m_size;
	}
	
	public String getIRI(int iriIndex) {
		return m_IRIsByIndex.get(iriIndex);
	}
	
	public Iterator<Resource> iterator() {
		return new DictionaryIterator();
	}
	
	
	
	public Resource getResource(ResourceType type, String lexicalValue, int datatypeIRIIndex) {
		final int hashCode = Resource.hashCode(type, lexicalValue, datatypeIRIIndex);
		final int bucketIndex = hashCode & (m_buckets.length - 1);
		Resource resource = m_buckets[bucketIndex];
		while (resource != null) {
			if (hashCode == resource.hashCode() && type.equals(resource.resourceType) && lexicalValue.equals(resource.lexicalValue) && datatypeIRIIndex == resource.datatypeIRIIndex)
				return resource;
			resource = resource.nextInDictionary;
		}
		resource = new Resource(type, lexicalValue, datatypeIRIIndex, m_buckets[bucketIndex]);
		m_buckets[bucketIndex] = resource;
		m_size++;
		if (m_size >= m_resizeThreshold) {
			final int newBucketsLength = m_buckets.length * 2;
			final int newBucketsLengthMinusOne = newBucketsLength - 1;
			Resource[] newBuckets = new Resource[newBucketsLength];
			for (Resource currentResource : m_buckets) {
				while (currentResource != null) {
					Resource nextResource = currentResource.nextInDictionary;
					final int newIndex = currentResource.hashCode() & newBucketsLengthMinusOne;
					currentResource.nextInDictionary = newBuckets[newIndex];
					newBuckets[newIndex] = currentResource;
					currentResource = nextResource;
				}
			}
			m_buckets = newBuckets;
			m_resizeThreshold = (int)(m_buckets.length * LOAD_FACTOR);
		}
		return resource;
	}
	
	public Resource getIRIReference(String iriReference) {
		return getResource(ResourceType.IRI_REFERENCE, iriReference, IRI_DATATYPE);
	}
	
	public Resource getBlankNode(String blankNode) {
		return getResource(ResourceType.BNODE, blankNode, BLANK_NODE_DATATYPE);
	}
	
	public Resource getLiteral(String lexicalValue, int datatypeIRIIndex) {
		return getResource(ResourceType.LITERAL, lexicalValue, datatypeIRIIndex);
	}
	
	public Resource getLiteral(String lexicalValue, String datatypeIRI) {
		return getResource(ResourceType.LITERAL, lexicalValue, getIRIIndex(datatypeIRI));
	}
	
	public static Dictionary getDictionary(Summary s) {
		Dictionary dict = new Dictionary();
		Bucket schemaBucket = s.schemaBuckets.getHead();
		while (schemaBucket != null) {
			assert schemaBucket.size() ==1;
			dict.add(schemaBucket.getFirst());
			schemaBucket = schemaBucket.nextBucket;
		}
		
		Bucket normalBucket = s.buckets.getHead();
		while (normalBucket != null) {
			Resource r = normalBucket.getFirst();
			while (r != null) {
				dict.add(r);
				r = r.nextInBucket;
			}
			normalBucket = normalBucket.nextBucket;
		}
		return dict;
	}
	
	protected void add(Resource resource) {
		final int hashCode = resource.hashCode();
		final int bucketIndex = hashCode & (m_buckets.length - 1);
		Resource current = m_buckets[bucketIndex];
		while (current != null) {
			if (current == resource)
				return;
			current = current.nextInDictionary;
		}
		m_buckets[bucketIndex] = resource;
		m_size++;
		if (m_size >= m_resizeThreshold) {
			final int newBucketsLength = m_buckets.length * 2;
			final int newBucketsLengthMinusOne = newBucketsLength - 1;
			Resource[] newBuckets = new Resource[newBucketsLength];
			for (Resource currentResource : m_buckets) {
				while (currentResource != null) {
					Resource nextResource = currentResource.nextInDictionary;
					final int newIndex = currentResource.hashCode() & newBucketsLengthMinusOne;
					currentResource.nextInDictionary = newBuckets[newIndex];
					newBuckets[newIndex] = currentResource;
					currentResource = nextResource;
				}
			}
			m_buckets = newBuckets;
			m_resizeThreshold = (int)(m_buckets.length * LOAD_FACTOR);
		}
	}
	
	protected int getIRIIndex(String iri) {
		if (m_IRIsToIndexes.containsKey(iri)) {
			return m_IRIsToIndexes.get(iri);
		}
		int iriIndex = m_IRIsByIndex.size();
		m_IRIsByIndex.add(iri);
		m_IRIsToIndexes.put(iri, iriIndex);
		return iriIndex;
	}
	
	protected class DictionaryIterator implements Iterator<Resource> {
		protected int m_currentIndex;
		protected Resource m_currentResource;
		
		public DictionaryIterator() {
			m_currentIndex = 0;
			while (m_currentIndex < m_buckets.length) {
				m_currentResource = m_buckets[m_currentIndex];
				if (m_currentResource != null)
					break;
				m_currentIndex++;
			}
		}

		public boolean hasNext() {
			return m_currentIndex < m_buckets.length;
		}

		public Resource next() {
			Resource result = m_currentResource;
			m_currentResource = m_currentResource.nextInDictionary;
			if (m_currentResource == null) {
				m_currentIndex++;
				while (m_currentIndex < m_buckets.length) {
					m_currentResource = m_buckets[m_currentIndex];
					if (m_currentResource != null)
						break;
					m_currentIndex++;
				}
			}
			return result;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	

}
