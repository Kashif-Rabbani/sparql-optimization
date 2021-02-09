package uk.ac.ox.krr.cardinality.queryanswering.model;

import java.io.File;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.model.ResourceType;
import uk.ac.ox.krr.cardinality.queryanswering.util.Prefixes;
import uk.ac.ox.krr.cardinality.queryanswering.util.VariableDictionary;
import uk.ac.ox.krr.cardinality.util.Dictionary;


public class SPARQLQuery extends Query {
	
	
	/**
	 * Constructs a new SPARQL query by applying the mgu to the given query.
	 * The mgu defines an equivalence relation over atoms; for each
	 * equivalence class, we introduce a single atom in the new query.
	 * @param query
	 * @param mguForBindings
	 */
	public SPARQLQuery(SPARQLQuery query, int[] mguForBindings, int[] atomMap) {
		resourcesToPositions = new HashMap<Resource, Integer>();
		m_bindings = new Resource[query.m_bindings.length];
		Set<Atom> atoms = new LinkedHashSet<Atom>();
		int index = 0;
		for (int atomIndex = 0; atomIndex < query.m_atoms.length; atomIndex++) {
			Atom atom = query.m_atoms[atomIndex];
			Resource subject = query.m_bindings[mguForBindings[atom.subjectIndex]];
			Resource predicate = query.m_bindings[mguForBindings[atom.predicateIndex]];
			Resource object = query.m_bindings[mguForBindings[atom.objectIndex]];
			if (addAtom(query, atoms, subject, predicate, object )) {
				atomMap[index] = atomIndex;
				index++;
			}
		}
		m_atoms = atoms.toArray(new Atom[atoms.size()]);
		atomMap = Arrays.copyOf(atomMap, m_atoms.length);

		int outputArraySize = 0;
		int outputArrayIndex = 0;
		for (Map.Entry<Resource, Integer> entry : resourcesToPositions.entrySet())
			if (ResourceType.VARIABLE.equals(entry.getKey().resourceType))
				outputArraySize++;
		variablePositions = new int[outputArraySize];
		for (Map.Entry<Resource, Integer> entry : resourcesToPositions.entrySet())
			if (ResourceType.VARIABLE.equals(entry.getKey().resourceType))
				variablePositions[outputArrayIndex++] = entry.getValue().intValue();
	}
	
	public SPARQLQuery(File queryFile, Dictionary dictionary, Prefixes prefixes) throws IOException  {
		this(new String(Files.readAllBytes(queryFile.toPath())), dictionary, prefixes);
	}
	
	public SPARQLQuery(String queryText, Dictionary dictionary, Prefixes prefixes) throws IOException  {
		prefixes.declareSemanticWebPrefixes();
		variableDictionary = new VariableDictionary();
		resourcesToPositions = new HashMap<Resource, Integer>();
		List<Resource> bindings = new ArrayList<Resource>();
		List<Atom> atoms = new ArrayList<Atom>();
		StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(queryText));
		initialiseSyntax(tokenizer);
		parsePrefixes(prefixes, tokenizer);
		if (tokenizer.ttype == StreamTokenizer.TT_WORD && "DISTINCT".equalsIgnoreCase(tokenizer.sval))
			tokenizer.nextToken();
		while (tokenizer.ttype != StreamTokenizer.TT_EOF && !"WHERE".equalsIgnoreCase(tokenizer.sval)) {
			tokenizer.nextToken();
		}
		parseWhereClause(dictionary, prefixes, bindings, atoms, tokenizer);
		if (tokenizer.ttype != StreamTokenizer.TT_EOF)
			throw new IllegalArgumentException("Unsuspected characters after '}'.");

		// Create the members of Query
		m_bindings = bindings.toArray(new Resource[bindings.size()]);
		m_atoms = atoms.toArray(new Atom[atoms.size()]);
		
		int outputArraySize = 0;
		for (Map.Entry<Resource, Integer> entry : resourcesToPositions.entrySet())
			if (ResourceType.VARIABLE.equals(entry.getKey().resourceType))
				outputArraySize++;
		variablePositions = new int[outputArraySize];
		int outputArrayIndex = 0;
		for (Map.Entry<Resource, Integer> entry : resourcesToPositions.entrySet()) {
			if (ResourceType.VARIABLE.equals(entry.getKey().resourceType)) {
				variablePositions[outputArrayIndex++] = entry.getValue();
			}
		}
	}
	
	
	private SPARQLQuery(Query query) {
		resourcesToPositions = new HashMap<Resource, Integer>();
		variableDictionary = new VariableDictionary();

		HashMap<Resource, Resource> variablesToPrimed = new HashMap<>();
		for (int i = 0; i < query.variablePositions.length; i++) {
			Resource variable = query.m_bindings[query.variablePositions[i]];
			String prime_vname = variable.lexicalValue + "Primed";
			Resource primed_variable = variableDictionary.getVariable(prime_vname);
			variablesToPrimed.put(variable, primed_variable);
		}
		List<Resource> bindings = new ArrayList<Resource>();

		List<Atom> atoms = new ArrayList<Atom>();
		for (Atom atom : query.m_atoms) {
			addAtom(bindings, atoms, query.m_bindings[atom.subjectIndex],  query.m_bindings[atom.predicateIndex],  query.m_bindings[atom.objectIndex]);
			Resource subjectPrimed = null;
			Resource objectPrimed =  null;
			if (query.m_bindings[atom.subjectIndex].resourceType == ResourceType.VARIABLE) {
				subjectPrimed = variablesToPrimed.get(query.m_bindings[atom.subjectIndex]);
			}
			if (query.m_bindings[atom.objectIndex].resourceType == ResourceType.VARIABLE) {
				objectPrimed = variablesToPrimed.get(query.m_bindings[atom.objectIndex]);
			}
			if (subjectPrimed != null || objectPrimed != null) {
				Resource subject = (subjectPrimed == null) ? query.m_bindings[atom.subjectIndex] : subjectPrimed;
				Resource object = (objectPrimed == null) ? query.m_bindings[atom.objectIndex] : objectPrimed;
				addAtom(bindings, atoms, subject,  query.m_bindings[atom.predicateIndex],  object);
			}
		}
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
	}
	
	public static SPARQLQuery standardDeviationQuery(Query query) {
		return new SPARQLQuery(query);
	}

	protected void parseWhereClause(Dictionary dictionary, Prefixes prefixes, List<Resource> bindings, List<Atom> atoms,
			StreamTokenizer tokenizer) throws IOException {
		tokenizer.nextToken();
		if (tokenizer.ttype == '{')
			tokenizer.nextToken();
		while (tokenizer.ttype != StreamTokenizer.TT_EOF && tokenizer.ttype != '}') {
			parseTriple(tokenizer, dictionary, prefixes, bindings, atoms);
			if (tokenizer.ttype == StreamTokenizer.TT_WORD && ".".equals(tokenizer.sval))
				tokenizer.nextToken();
			else if (tokenizer.ttype != '}')
				throw new IllegalArgumentException("Invalid triple separator.");
		}
		if (tokenizer.ttype == '}')
			tokenizer.nextToken();
	}



	protected void parsePrefixes(Prefixes prefixes, StreamTokenizer tokenizer) throws IOException {
		tokenizer.nextToken();
		while (tokenizer.ttype != StreamTokenizer.TT_EOF) {
			if (tokenizer.ttype == StreamTokenizer.TT_WORD) {
				if ("PREFIX".equalsIgnoreCase(tokenizer.sval)) {
					tokenizer.nextToken();
					String prefixName = getNextString(tokenizer);
					if (!prefixName.endsWith(":"))
						throw new IllegalArgumentException("Invalid prefix: " + prefixName);
					prefixes.declarePrefix(prefixName, getNextQuotedString(tokenizer));
				} else if ("SELECT".equalsIgnoreCase(tokenizer.sval)) {
					tokenizer.nextToken();
					break;
				} else
					throw new IllegalArgumentException("Invalid keyword: " + tokenizer.sval);
			} else
				throw new IllegalArgumentException("Invalid query token.");
		}
	}

	protected void initialiseSyntax(StreamTokenizer tokenizer) {
		tokenizer.resetSyntax();
		tokenizer.whitespaceChars(' ', ' ');
		tokenizer.whitespaceChars('\t', '\t');
		tokenizer.whitespaceChars('\n', '\n');
		tokenizer.whitespaceChars('\r', '\r');
		tokenizer.quoteChar('"');
		tokenizer.wordChars('a', 'z');
		tokenizer.wordChars('A', 'Z');
		tokenizer.wordChars('0', '9');
		tokenizer.wordChars('_', '_');
		tokenizer.wordChars('<', '<');
		tokenizer.wordChars('>', '>');
		tokenizer.wordChars(':', ':');
		tokenizer.wordChars('?', '?');
		tokenizer.wordChars('.', '.');
		tokenizer.wordChars('/', '/');
		tokenizer.wordChars('#', '#');
		tokenizer.wordChars('+', '+');
		tokenizer.wordChars('-', '-');
		tokenizer.wordChars('~', '~');
		tokenizer.wordChars('%', '%');
		tokenizer.eolIsSignificant(false);
	}
	
	
	protected void parseTriple(StreamTokenizer tokenizer,  Dictionary dictionary, Prefixes prefixes, List<Resource> bindings, List<Atom> atoms) throws IOException  {
		Resource currentSubject = parseIRIorVariable(tokenizer, dictionary, prefixes);
		while (true) {
			parsePredicateObject(tokenizer, dictionary, prefixes, bindings, atoms, currentSubject);
			if (tokenizer.ttype == ';') {
				tokenizer.nextToken();
				if (tokenizer.ttype == StreamTokenizer.TT_WORD && ".".equals(tokenizer.sval))
					break;
			}
			else
				break;
		}
	}

	protected void parsePredicateObject(StreamTokenizer tokenizer,  Dictionary dictionary, Prefixes prefixes, List<Resource> bindings, List<Atom> atoms, Resource currentSubject) throws IOException  {
		Resource currentPredicate = parseIRIorVariable(tokenizer, dictionary, prefixes);
		if (currentPredicate.resourceType == ResourceType.VARIABLE)
			throw new IllegalArgumentException("Variables in the predicate position are not supported.");
		while (true) {
			Resource currentObject = parseResource(tokenizer, dictionary, prefixes);
			addAtom(bindings, atoms, currentSubject, currentPredicate, currentObject);
			if (tokenizer.ttype == ',')
				tokenizer.nextToken();
			else
				break;
		}
	}
	
	public boolean addAtom(SPARQLQuery query, Collection<Atom> atoms, Resource... arguments) {
		Resource subject = arguments[0]; 
		Resource predicate = arguments[1]; 
		Resource object = arguments[2];

		boolean boundSubject = isBound(subject);
		boolean boundObject = isBound(object);

		Integer subjectIndex = getOrCreatePosition(query, subject);		
		Integer predicateIndex = getOrCreatePosition(query, predicate);	
		Integer objectIndex = getOrCreatePosition(query, object);		

		return atoms.add(new Atom(subjectIndex, predicateIndex, objectIndex, boundSubject, boundObject));
	}


	public void addAtom(List<Resource> bindings, Collection<Atom> atoms, Resource... arguments) {
		Resource subject = arguments[0]; 
		Resource predicate = arguments[1]; 
		Resource object = arguments[2];
		boolean boundSubject = isBound(subject);
		boolean boundObject = isBound(object);
		Integer subjectIndex = getOrCreatePosition(bindings, subject);		
		Integer predicateIndex = getOrCreatePosition(bindings, predicate);	
		Integer objectIndex = getOrCreatePosition(bindings, object);		
		atoms.add(new Atom(subjectIndex, predicateIndex, objectIndex, boundSubject, boundObject));
	}
	
	protected boolean isBound(Resource resource) {
		return !ResourceType.VARIABLE.equals(resource.resourceType) || resourcesToPositions.containsKey(resource);
	}

	protected Integer getOrCreatePosition(SPARQLQuery query, Resource resource) {
		Integer position = resourcesToPositions.get(resource);
		if (position == null) {
			position = query.resourcesToPositions.get(resource);
			m_bindings[position] = resource;
			resourcesToPositions.put(resource, position);
		}
		return position;
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

	protected Resource parseResource(StreamTokenizer tokenizer, Dictionary dictionary, Prefixes prefixes) throws IOException {
		if (tokenizer.ttype == '"') {

			String lexicalForm = tokenizer.sval;
			tokenizer.nextToken();

			if (tokenizer.ttype == '@') {
				tokenizer.nextToken();
				if (tokenizer.ttype != StreamTokenizer.TT_WORD)
					throw new IOException("Invalid language tag.");
				String languageTag = tokenizer.sval;
				tokenizer.nextToken();
				return dictionary.getLiteral(lexicalForm + '@' + languageTag, RDF_PLAIN_LITERAL);
			}
			else if (tokenizer.ttype == '^') {

				tokenizer.nextToken();
				if (tokenizer.ttype != '^') {
					throw new IOException("Invalid datatype URI.");
				}
				tokenizer.nextToken();
				String datatype = prefixes.expandAbbreviatedIRI(tokenizer.sval);
				tokenizer.nextToken();
				return dictionary.getLiteral(lexicalForm, datatype);
			}
			else
				return dictionary.getLiteral(lexicalForm, XSD_STRING);
		} 
		else
			return parseIRIorVariable(tokenizer,  dictionary, prefixes);
	}

	protected Resource parseIRIorVariable(StreamTokenizer tokenizer,  Dictionary dictionary, Prefixes prefixes) throws IOException  {
		if (tokenizer.ttype != '[') 
			return parseIRI(tokenizer, dictionary, prefixes);
		throw new IllegalArgumentException("Blank nodes are not supported.");
	}

	protected Resource parseIRI(StreamTokenizer tokenizer, Dictionary dictionary, Prefixes prefixes) throws IOException  {
		if (tokenizer.ttype == StreamTokenizer.TT_WORD) {
			String label = tokenizer.sval;
			if (label.endsWith("."))
				throw new IllegalArgumentException("the string finishes with a dot");
			tokenizer.nextToken();
			if (label.startsWith("?"))
				return variableDictionary.getVariable(label);
			else if (label.startsWith("_:"))
				throw new IllegalArgumentException("blank nodes are not supported");
			else if ("a".equals(label))
				return dictionary.getIRIReference(RDF_TYPE);
			else 
				return dictionary.getIRIReference(prefixes.expandAbbreviatedIRI(label));
		}
		else
			throw new IllegalArgumentException("Invalid IRI.");
	}
	
	protected String getNextString(StreamTokenizer tokenizer) throws IOException  {
		if (tokenizer.ttype != StreamTokenizer.TT_WORD)
			throw new IllegalArgumentException("String expected.");
		else {
			String result = tokenizer.sval;
			tokenizer.nextToken();
			return result;
		}
	}
	
	protected String getNextQuotedString(StreamTokenizer tokenizer) throws IOException  {
		if (tokenizer.ttype != StreamTokenizer.TT_WORD)
			throw new IllegalArgumentException("Quoted IRI expected.");
		else {
			String result = tokenizer.sval;
			tokenizer.nextToken();
			if (!result.startsWith("<") || !result.endsWith(">"))
				throw new IllegalArgumentException("Invalid quoted IRI: " + result);
			return result.substring(1, result.length() - 1);
		}
	}
	
	protected Resource getNextQuotedIRI(StreamTokenizer tokenizer, Dictionary dictionary) throws IOException  {
		if (tokenizer.ttype != StreamTokenizer.TT_WORD)
			throw new IllegalArgumentException("Quoted IRI expected.");
		else {
			String result = tokenizer.sval;
			tokenizer.nextToken();
			if (!result.startsWith("<") || !result.endsWith(">"))
				throw new IllegalArgumentException("Invalid quoted IRI: " + result);
			return dictionary.getIRIReference(result.substring(1, result.length() - 1));
		}
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("SELECT * WHERE {");
		boolean first = true;
		for (Atom atom : m_atoms) {
			if (first)
				first = false;
			else
				buffer.append(" .");

			Resource subject = m_bindings[atom.subjectIndex];
			appendArgument(subject, buffer);
			if (atom.boundSubject)
				buffer.append('&');

			Resource predicate = m_bindings[atom.predicateIndex];
			appendArgument(predicate, buffer);
			buffer.append('&');

			Resource object = m_bindings[atom.objectIndex];
			appendArgument(object, buffer);
			if (atom.boundObject)
				buffer.append('&');
		}
		buffer.append(" }");
		return buffer.toString();
	}
	
	protected void appendArgument(Resource argument, StringBuffer buffer) {
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
			buffer.append("^^");
			buffer.append(argument.datatypeIRIIndex);
			buffer.append('"');
			break;
		}
	}	
	
	
} 
