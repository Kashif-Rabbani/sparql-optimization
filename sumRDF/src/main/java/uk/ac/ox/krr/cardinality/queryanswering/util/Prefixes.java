package uk.ac.ox.krr.cardinality.queryanswering.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Prefixes {

	protected static final String PN_CHARS_BASE = "[A-Za-z\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u02FF\\u0370-\\u037D\\u037F-\\u1FFF\\u200C-\\u200D\\u2070-\\u218F\\u2C00-\\u2FEF\\u3001-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFFD]";
	protected static final String PN_CHARS = "[A-Za-z0-9_\\u002D\\u00B7\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u02FF\\u0300-\\u036F\\u0370-\\u037D\\u037F-\\u1FFF\\u200C-\\u200D\\u203F-\\u2040\\u2070-\\u218F\\u2C00-\\u2FEF\\u3001-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFFD]";
	protected static final Pattern s_localNameChecker = Pattern.compile("(" + PN_CHARS_BASE + "|_|[0-9])((" + PN_CHARS + "|[.])*(" + PN_CHARS + "))?");
	public static final Map<String, String> s_semanticWebPrefixes;
	static {
		s_semanticWebPrefixes = new HashMap<String, String>();
		s_semanticWebPrefixes.put("rdf:", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		s_semanticWebPrefixes.put("rdfs:", "http://www.w3.org/2000/01/rdf-schema#");
		s_semanticWebPrefixes.put("owl:", "http://www.w3.org/2002/07/owl#");
		s_semanticWebPrefixes.put("xsd:", "http://www.w3.org/2001/XMLSchema#");
		s_semanticWebPrefixes.put("swrl:", "http://www.w3.org/2003/11/swrl#");
		s_semanticWebPrefixes.put("swrlb:", "http://www.w3.org/2003/11/swrlb#");
		s_semanticWebPrefixes.put("swrlx:", "http://www.w3.org/2003/11/swrlx#");
		s_semanticWebPrefixes.put("ruleml:", "http://www.w3.org/2003/11/ruleml#");
	}

	protected final Map<String, String> m_prefixIRIsByPrefixName;
	protected final Map<String, String> m_prefixNamesByPrefixIRI;
	protected Pattern m_prefixIRIMatchingPattern;

	public Prefixes() {
		m_prefixIRIsByPrefixName = new TreeMap<String, String>();
		m_prefixNamesByPrefixIRI = new TreeMap<String, String>();
		buildPrefixIRIMatchingPattern();
	}

	protected void buildPrefixIRIMatchingPattern() {
		List<String> list = new ArrayList<String>(m_prefixNamesByPrefixIRI.keySet());
		// Sort the prefix IRIs, longest first
		Collections.sort(list, new Comparator<String>() {
			public int compare(String lhs, String rhs) {
				return rhs.length() - lhs.length();
			}
		});
		StringBuilder pattern = new StringBuilder("^(");
		boolean didOne = false;
		for (String prefixIRI : list) {
			if (didOne)
				pattern.append("|(");
			else {
				pattern.append("(");
				didOne = true;
			}
			pattern.append(Pattern.quote(prefixIRI));
			pattern.append(")");
		}
		pattern.append(")");
		if (didOne)
			m_prefixIRIMatchingPattern = Pattern.compile(pattern.toString());
		else
			m_prefixIRIMatchingPattern = null;
	}

	public String abbreviateIRI(String iri) {
		if (m_prefixIRIMatchingPattern != null) {
			Matcher matcher = m_prefixIRIMatchingPattern.matcher(iri);
			if (matcher.find()) {
				String localName = iri.substring(matcher.end());
				if (isValidLocalName(localName)) {
					String prefix = m_prefixNamesByPrefixIRI.get(matcher.group(1));
					return prefix + localName;
				}
			}
		}
		return "<" + iri + ">";
	}

	public String expandAbbreviatedIRI(String abbreviation) {
		if (abbreviation.length() > 0 && abbreviation.charAt(0) == '<') {
			if (abbreviation.charAt(abbreviation.length() - 1) != '>')
				throw new IllegalArgumentException("The string '" + abbreviation + "' is not a valid abbreviation: IRIs must be enclosed in '<' and '>'.");
			return abbreviation.substring(1, abbreviation.length() - 1);
		}
		else {
			int pos = abbreviation.indexOf(':');
			if (pos != -1) {
				String prefix = abbreviation.substring(0, pos + 1);
				String prefixIRI = m_prefixIRIsByPrefixName.get(prefix);
				if (prefixIRI == null) {
					// Catch the common error of not quoting IRIs starting with
					// http:
					if (prefix == "http:")
						throw new IllegalArgumentException("The IRI '" + abbreviation + "' must be enclosed in '<' and '>' to be used as an abbreviation.");
					throw new IllegalArgumentException("The string '" + prefix + "' is not a registered prefix name.");
				}
				return prefixIRI + abbreviation.substring(pos + 1);
			}
			else
				throw new IllegalArgumentException("The abbreviation '" + abbreviation + "' is not valid (it does not start with a colon).");
		}
	}

	public boolean canBeExpanded(String iri) {
		if (iri.length() > 0 && iri.charAt(0) == '<')
			return false;
		else {
			int pos = iri.indexOf(':');
			if (pos != -1) {
				String prefix = iri.substring(0, pos + 1);
				return m_prefixIRIsByPrefixName.get(prefix) != null;
			}
			else
				return false;
		}
	}

	public boolean declarePrefix(String prefixName, String prefixIRI) {
		boolean containsPrefix = declarePrefixRaw(prefixName, prefixIRI);
		buildPrefixIRIMatchingPattern();
		return containsPrefix;
	}

	protected boolean declarePrefixRaw(String prefixName, String prefixIRI) {
		if (!prefixName.endsWith(":"))
			throw new IllegalArgumentException("Prefix name '" + prefixName + "' should end with a colon character.");
		String existingPrefixName = m_prefixNamesByPrefixIRI.get(prefixIRI);
		if (existingPrefixName != null && !prefixName.equals(existingPrefixName))
			throw new IllegalArgumentException("The prefix IRI '" + prefixIRI + "' has already been associated with the prefix name '" + existingPrefixName + "'.");
		m_prefixNamesByPrefixIRI.put(prefixIRI, prefixName);
		return m_prefixIRIsByPrefixName.put(prefixName, prefixIRI) == null;
	}

	public boolean declareDefaultPrefix(String defaultPrefixIRI) {
		return declarePrefix(":", defaultPrefixIRI);
	}

	public Map<String, String> getPrefixIRIsByPrefixName() {
		return Collections.unmodifiableMap(m_prefixIRIsByPrefixName);
	}

	public String getPrefixIRI(String prefixName) {
		return m_prefixIRIsByPrefixName.get(prefixName);
	}

	public String getPrefixName(String prefixIRI) {
		return m_prefixNamesByPrefixIRI.get(prefixIRI);
	}

	public boolean declareSemanticWebPrefixes() {
		boolean containsPrefix = false;
		for (Map.Entry<String, String> entry : s_semanticWebPrefixes.entrySet())
			if (declarePrefixRaw(entry.getKey(), entry.getValue()))
				containsPrefix = true;
		buildPrefixIRIMatchingPattern();
		return containsPrefix;
	}

	public boolean addPrefixes(Prefixes prefixes) {
		boolean containsPrefix = false;
		for (Map.Entry<String, String> entry : prefixes.m_prefixIRIsByPrefixName.entrySet())
			if (declarePrefixRaw(entry.getKey(), entry.getValue()))
				containsPrefix = true;
		buildPrefixIRIMatchingPattern();
		return containsPrefix;
	}

	public void clear() {
		m_prefixIRIsByPrefixName.clear();
		m_prefixNamesByPrefixIRI.clear();
		buildPrefixIRIMatchingPattern();
	}

	public String toString() {
		return m_prefixIRIsByPrefixName.toString();
	}

	public static boolean isValidLocalName(String localName) {
		return s_localNameChecker.matcher(localName).matches();
	}
}
