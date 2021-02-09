package uk.ac.ox.krr.cardinality.queryanswering.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.model.ResourceType;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.EdgeIterator;
import uk.ac.ox.krr.cardinality.queryanswering.util.Prefixes;
import uk.ac.ox.krr.cardinality.queryanswering.util.VariableDictionary;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;
import uk.ac.ox.krr.cardinality.util.Dictionary;

public class UnificationFreeTD extends Query {
	
	public final DNode root;
	
	public UnificationFreeTD(String pathToXMLFile, Dictionary dictionary, Prefixes prefixes) throws Exception {
		prefixes.declareSemanticWebPrefixes();
		resourcesToPositions = new HashMap<Resource, Integer>();
		this.variableDictionary = new VariableDictionary();
		ArrayList<Resource> bindings = new ArrayList<>();
		ArrayList<Atom> atoms = new ArrayList<>();

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(pathToXMLFile);
		root = parseQuery(doc.getDocumentElement(), dictionary, prefixes, bindings, atoms);
		m_bindings = bindings.toArray(new Resource[bindings.size()]);
		m_atoms = atoms.toArray(new Atom[atoms.size()]);
		int outputArraySize = 0;
		int outputArrayIndex = 0;
		for (Map.Entry<Resource, Integer> entry : resourcesToPositions.entrySet())
			if (ResourceType.VARIABLE.equals(entry.getKey().resourceType))
				outputArraySize++;
		variablePositions = new int[outputArraySize];
		for (Map.Entry<Resource, Integer> entry : resourcesToPositions.entrySet())
			if (ResourceType.VARIABLE.equals(entry.getKey().resourceType))
				variablePositions[outputArrayIndex++] = entry.getValue().intValue();
		initialiseVariables(root, null);
	}
	
	
	public void initialiseVariables(DNode node, Set<Integer> parentVariables) {
		Set<Integer> nodeVariables = getVariables(node);
		node.variables = new int[nodeVariables.size()];
		int i = 0;
		for (int nodeVariable : nodeVariables) {
			node.variables[i++] = nodeVariable;
		}
		if (parentVariables == null) {
			node.intersectionVariables = new int[0];
		} else {
			HashSet<Integer> intersection = new HashSet<>(nodeVariables);
			intersection.retainAll(parentVariables);
			node.intersectionVariables = new int[intersection.size()];
			int index = 0;
			for (int variable : intersection) {
				node.intersectionVariables[index] = variable;
				index++;
			}
		}
		for (Object o : node.children) {
			if (o instanceof DNode) {
				initialiseVariables((DNode) o, nodeVariables);
			}
		}
		Arrays.sort(node.variables);
		Arrays.sort(node.intersectionVariables);
	}
	
	private Set<Integer> getVariables(DNode node) {
		HashSet<Integer> vars = new HashSet<>();
		for (Object child : node.children) {
			if (child instanceof Atom) {
				Atom atom = (Atom) child;
				if (m_bindings[atom.subjectIndex].resourceType == ResourceType.VARIABLE) {
					vars.add(atom.subjectIndex);
				}
				if (m_bindings[atom.objectIndex].resourceType == ResourceType.VARIABLE ) {
					vars.add(atom.objectIndex);
				}
				
			}
		}
		return vars;
	}


	public String toString() {
		StringBuilder builder = new StringBuilder("SELECT * WHERE {\n");
		stringify(root, builder);
		builder.append("}");
		return builder.toString();
		
	}
	
	public void stringify(DNode node, StringBuilder builder) {
		for (Object o : node.children) {
			if (o instanceof DNode) {
				stringify((DNode) o,  builder);
			} else if (o instanceof Atom) {
				Atom atom = (Atom) o;
				Resource subject = m_bindings[atom.subjectIndex];
				appendArgument(subject, builder);
				if (atom.boundSubject)
					builder.append('&');

				Resource predicate = m_bindings[atom.predicateIndex];
				appendArgument( predicate, builder);
				builder.append('&');

				Resource object = m_bindings[atom.objectIndex];
				appendArgument(object, builder);

				if (atom.boundObject)
					builder.append('&');
				builder.append("\n");
			}
		}
	}
	
	protected void appendArgument(Resource argument, StringBuilder buffer) {
		buffer.append(' ');
		switch (argument.resourceType) {
		case VARIABLE:
			buffer.append(argument.lexicalValue);
			break;
		case IRI_REFERENCE:
			buffer.append(argument.toString());
			break;
		case BNODE:
			buffer.append(argument.toString());
			break;
		case LITERAL:
			buffer.append('"');
			buffer.append(argument.lexicalValue);
			buffer.append('"');
		}
	}
	
	
	public DNode parseQuery(Element element, Dictionary dictionary, Prefixes prefixes, ArrayList<Resource> bindings, ArrayList<Atom> atoms) throws IOException {
		final NodeList children = element.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
            final Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
            	Element child = (Element) n;
            	if (child.getTagName().equals("prefixes")) {
            		parsePrefixes(child, dictionary, prefixes);
            	} else if (child.getTagName().equals("nodes")) {
            		return parseNodes(child, dictionary, prefixes, bindings, atoms);
            	}
            }
		}
		return null;
	}
	
	public void parsePrefixes(Element element, Dictionary dictionary, Prefixes prefixes) {
		final NodeList children = element.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
            final Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
            	Element child = (Element) n;
            	if (child.getTagName().equals("prefix")) {
            		parsePrefix(child, dictionary, prefixes);
            	}
            }
		}
	}
	
	public void parsePrefix(Element prefix, Dictionary dictionary, Prefixes prefixes) {
		final NodeList children = prefix.getChildNodes();
		String prefixIRI = null;
		String prefixName = null;
		for (int i = 0; i < children.getLength(); i++) {
            final Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
            	Element child = (Element) n;
            	if (prefixName == null && child.getTagName().equals("name")) {
            		prefixName = child.getTextContent();
            	} else if (prefixIRI == null && child.getTagName().equals("uri")) {
            		String uri = child.getTextContent();
            		if (!uri.startsWith("<") || !uri.endsWith(">"))
        				throw new IllegalArgumentException("Invalid quoted IRI: " + uri);
            		prefixIRI = uri.substring(1, uri.length() - 1);
            	}
            }
		}
		prefixes.declarePrefix(prefixName, prefixIRI);
		
	}
	
	public DNode parseNodes(Element nodes, Dictionary dictionary, Prefixes prefixes, ArrayList<Resource> bindings, ArrayList<Atom> atoms) throws IOException {
		final NodeList children = nodes.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
            final Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
            	Element child = (Element) n;
            	if (child.getTagName().equals("node")) {
            		return parseNode(child, dictionary, prefixes, bindings, atoms);
            	}
            }
		}
		return null;
	}
	
	
	public DNode parseNode(Element xmlNode, Dictionary dictionary, Prefixes prefixes, ArrayList<Resource> bindings, ArrayList<Atom> atoms) throws IOException {
		DNode queryNode = new DNode();
		ArrayList<Object> queryChildren = new ArrayList<>();
		final NodeList children = xmlNode.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			final Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
            	Element child = (Element) n;
            	if (child.getTagName().equals("atom")) {
            		queryChildren.add(parseAtom(child, dictionary, prefixes, bindings, atoms));
            	} else if (child.getTagName().equals("node")) {
            		queryChildren.add(parseNode(child, dictionary, prefixes, bindings,atoms));
            	}
            }
		}
		queryNode.children = queryChildren.toArray(new Object[queryChildren.size()]);
		return queryNode;
	}
	
	
	public Atom parseAtom(Element element, Dictionary dictionary, Prefixes prefixes, ArrayList<Resource> bindings, ArrayList<Atom> atoms) throws  IOException {
		final NodeList children = element.getChildNodes();
		Resource subject = null, predicate = null, object = null;
		for (int i = 0; i < children.getLength(); i++) {
			final Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
            	Element child = (Element) n;
            	if (subject== null && child.getTagName().equals("subject")) {
            		subject = parseResource(child.getTextContent(), dictionary, prefixes);
            	} else if (predicate == null && child.getTagName().equals("predicate")) {
            		predicate = parseResource(child.getTextContent(), dictionary, prefixes);
            	} else if (object == null && child.getTagName().equals("object")) {
            		object = parseResource(child.getTextContent(), dictionary, prefixes);
            	}
            }
		}
		boolean boundSubj = isBound(subject);
		boolean boundObj = isBound(object);
		Atom atom =  new Atom(getOrCreatePosition(bindings, subject), getOrCreatePosition(bindings, predicate), getOrCreatePosition(bindings, object), boundSubj, boundObj);
		atoms.add(atom);
		return atom;
	}
	
	protected boolean isBound(Resource resource) {
		return !ResourceType.VARIABLE.equals(resource.resourceType) || resourcesToPositions.containsKey(resource);
	}
	
	
	
	protected Resource parseResource(String label, Dictionary dictionary, Prefixes prefixes) throws IOException  {
		if (label.startsWith("?"))
			return variableDictionary.getVariable(label);
		else if (label.startsWith("_:"))
			return dictionary.getBlankNode(label);
		else if (label.startsWith("\"")) {
			return parseLiteral(label, dictionary, prefixes);
		}
		else if ("a".equals(label))
			return dictionary.rdf_type;
		else 
			return dictionary.getIRIReference(prefixes.expandAbbreviatedIRI(label));
	}
	
	protected Resource parseLiteral(String label, Dictionary dictionary, Prefixes prefixes) throws IOException {
		
		String literal = label.substring(label.indexOf('\"') + 1, label.lastIndexOf('\"'));
		String tags = label.substring(label.lastIndexOf('\"')+1);
		
		int datatypeSeparator = tags.indexOf('^');
		if (datatypeSeparator < 0) {
			return dictionary.getLiteral(literal, XSD_STRING);
		}
		if (tags.charAt(datatypeSeparator+1) != '^')  {
			throw new IOException("Invalid datatype URI.");
		}
		String datatype = prefixes.expandAbbreviatedIRI(tags.substring(datatypeSeparator+2));
		return dictionary.getLiteral(literal, datatype);
	}
	
	protected Integer getOrCreatePosition(List<Resource> bindings, Resource resource) {
		Integer position = resourcesToPositions.get(resource);
		if (position == null) {
			position = bindings.size();
			bindings.add(resource);
			resourcesToPositions.put(resource, position);
		}
		return position;
	}
	
	
	public static class DNode {
		public Object[] children;
		public final SubstitutionTable hashTable = new SubstitutionTable();
		public int[] intersectionVariables;
		public int[] variables;
		public EdgeIterator[] iterators;
	
	}

	public void clear() {
		clear(root);
	}

	private void clear(DNode node) {
		node.hashTable.clear();
		for (Object o : node.children) {
			if (o instanceof DNode) {
				clear((DNode) o);
			}
		}
	}

	public static class Substitution {
		
		public Bucket[] buckets;
		
		int hashCode;
		
		UnificationFreeTD query;

		public Substitution(Substitution rho) {
			this.query = rho.query;
			buckets = Arrays.copyOf(rho.buckets, rho.buckets.length);
			hashCode = hashCode(buckets);
		}
		
		public static int hashCode(Bucket[] tau) {
			int result = 0;
			for (Bucket bucket : tau) {
				result += bucket.bucket_id;
			    result += (result << 10);
			    result ^= (result >> 6);
			}
			result += (result << 3);
		    result ^= (result >> 11);
		    result += (result << 15);
			return result;

		}

		public Substitution(UnificationFreeTD q, Bucket[] values) {
			this.query = q;
			buckets = values;
			hashCode = Arrays.hashCode(buckets);
		}

		public Bucket get(int index) {
			return buckets[index];
		}

		@Override
		public int hashCode() {
			return this.hashCode;
		}

		public void update(Atom atom, Edge edge) {
			if (query.m_bindings[atom.subjectIndex].resourceType == ResourceType.VARIABLE)
				buckets[atom.subjectIndex] = edge.subject;
			if (query.m_bindings[atom.objectIndex].resourceType == ResourceType.VARIABLE)
				buckets[atom.objectIndex] = edge.object;
		}

		public Substitution restrict(int[] variables) {
			Bucket[] subs = new Bucket[query.m_bindings.length];
			int varPos = 0;
			for (int position = 0; position < buckets.length; position++) {
				if (query.m_bindings[position].resourceType != ResourceType.VARIABLE)
					subs[position] = buckets[position];
				else if (varPos < variables.length && position == variables[varPos]) {
					subs[position] = buckets[position];
					varPos++;
				}
			}
			return new Substitution(query, subs);
		}

		public boolean equals(Object o) {
			if (o instanceof Substitution) {
				Substitution nt = (Substitution) o;
				return nt.hashCode == hashCode() && Arrays.deepEquals(buckets, nt.buckets);
			}
			return false;
		}

		public String toString() {
			String result = "{";
			for (Bucket b : buckets) {
				result += b + "; ";
			}
			return result + "}";
		}
	}
	
	
	public static class SubstitutionTable {
		
		private final static int INIT_CAPACITY = 1024;
		
		int size;
		int capacity;
		
		Substitution[] keys;
		double[] values;
		
		public SubstitutionTable() {
			this(INIT_CAPACITY);
		}
		
		public void clear() {
			size = 0;
			capacity = INIT_CAPACITY;
			keys = new Substitution[capacity];
			values = new double[capacity];
		}

		public SubstitutionTable(int cap) {
			capacity = cap;
			keys = new Substitution[capacity];
			values = new double[capacity];
			size = 0;
		}
		
		protected int hash(Substitution sub) {
			return (sub.hashCode() & 0x7fffffff) & (capacity - 1);
		}
		
		public double get(Substitution sub) {
			int index = hash(sub);
			while (keys[index] != null) {
				if (keys[index].equals(sub))
					return values[index];
				index = (index + 1) & (capacity -1);
			}
			return 0.0;
		}
		
		public void add(Substitution sub, double value) {
			if (size > (capacity/2))
				resize(capacity * 2);
			int index = hash(sub);
			while (keys[index] != null) {
				if (keys[index].equals(sub)) {
					values[index] += value;
					return;
				}
				index = (index + 1) & (capacity -1);
			}
			keys[index] = sub;
			values[index] = value;
			size++;
		}

		private void resize(int newCapacity) {
			SubstitutionTable temp = new SubstitutionTable(newCapacity);
			for (int index = 0; index < keys.length; index++) {
				if (keys[index] != null)
					temp.add(keys[index], values[index]);
			}
			this.keys = temp.keys;
			this.values = temp.values;
			this.capacity = newCapacity;			
		}

		public boolean containsKey(Substitution sigma) {
			int index = hash(sigma);
			while (keys[index] != null) {
				if (keys[index].equals(sigma)) {
					return true;
				}
				index = (index + 1) & (capacity -1);
			}
			return false;
		}
		
		
	}

	public static class MyDouble {

		public double value;

		public MyDouble(double d) {
			value = d;
		}

		public void add(double s) {
			value += s;
		}

		public double get() {
			return value;
		}

		public String toString() {
			return Double.toString(value);
		}
	}
	
	public static void main(String[] args) throws Exception {
		UnificationFreeTD query = new UnificationFreeTD("/Volumes/RDFdata/DBLP/queries/sparql/snowflake/query09.xml", new Dictionary(), new Prefixes());
		System.out.println(query);
	}
}
