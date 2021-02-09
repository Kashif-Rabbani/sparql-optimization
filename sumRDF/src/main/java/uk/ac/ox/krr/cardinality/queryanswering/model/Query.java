package uk.ac.ox.krr.cardinality.queryanswering.model;

import java.util.Map;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.queryanswering.util.Prefixes;
import uk.ac.ox.krr.cardinality.queryanswering.util.VariableDictionary;

public abstract class Query {
	
	protected static final String RDF_TYPE = Prefixes.s_semanticWebPrefixes.get("rdf:") + "type";
	protected static final String RDF_PLAIN_LITERAL = Prefixes.s_semanticWebPrefixes.get("rdf:") + "type";
	protected static final String XSD_STRING = Prefixes.s_semanticWebPrefixes.get("xsd:") + "string";
	
	public Map<Resource, Integer> resourcesToPositions;
	public int[] variablePositions;
	public Resource[] m_bindings;
	public Atom[] m_atoms;
	public VariableDictionary variableDictionary;
	
	public int countAtoms() {
		return m_atoms.length;
	}
	
	public static class Atom {
		public final int subjectIndex;
		public final int predicateIndex;
		public final int objectIndex;

		public final boolean boundSubject;
		public final boolean boundObject;

		public final boolean equalityCheck;

		public static int ARITY = 3;
		public final int hashCode;


		public Atom(int subjIndex, int predIndex, int objIndex, boolean subjBound, boolean objBound) {
			this.subjectIndex = subjIndex;
			this.predicateIndex = predIndex;
			this.objectIndex = objIndex;
			this.boundSubject = subjBound;
			this.boundObject = objBound;
			if (!boundSubject && subjectIndex == objectIndex) {
				this.equalityCheck = true;
			} else {
				this.equalityCheck = false;
			}
			hashCode = hashCode(boundObject, boundSubject, equalityCheck, objectIndex, predicateIndex, subjectIndex);
		}
		
		private static int hashCode(boolean boundObject, boolean boundSubject, boolean equalityCheck, int objectIndex, int predicateIndex, int subjectIndex) {
			final int prime = 31;
			int result = 1;
			result = prime * result + objectIndex;
			result = prime * result + predicateIndex;
			result = prime * result + subjectIndex;
			return result;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}


		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Atom other = (Atom) obj;
			if (objectIndex != other.objectIndex)
				return false;
			if (predicateIndex != other.predicateIndex)
				return false;
			if (subjectIndex != other.subjectIndex)
				return false;
			return true;
		}

		public int arity() {
			return ARITY;
		}

		public String toString(SPARQLQuery query) {
			StringBuffer buffer = new StringBuffer();
			buffer.append(' ');
			buffer.append(query.m_bindings[subjectIndex].toString());

			buffer.append(' ');
			buffer.append(query.m_bindings[predicateIndex].toString());

			buffer.append(' ');
			buffer.append(query.m_bindings[objectIndex].toString());
			return buffer.toString();
		}
	}
	

}
