package uk.ac.ox.krr.cardinality.summarisation.model;

public class Edge {
		
	public Bucket subject;
	
	public Bucket predicate;
	
	public Bucket object;
	
	public int weight;

	public int hashCode;
	
	public Edge nextSP;
	public Edge nextOP;
	public Edge nextP;
	
	public Edge(Bucket s, Bucket p, Bucket o) {
		this.subject = s;
		this.predicate = p;
		this.object = o;
		this.hashCode = hashCode(s,p,o);
		this.weight = 1;
	}
	
	@Override
	public int hashCode() {
		return this.hashCode;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Edge) {
			Edge edge = (Edge) obj;
			return edge.subject == subject && edge.predicate == predicate && edge.object == object;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return "[" + subject + ", " + predicate.resources.getHead().lexicalValue + ", " + object + "]:" + weight;
	}
	
	public static int hashCode(Bucket s, Bucket p, Bucket o) {
		int result = 0;
		result += (s == null) ? 1 : s.hashCode();
	    result += (result << 10);
	    result ^= (result >> 6);

	    result += (p == null) ? 1 : p.hashCode();
	    result += (result << 10);
	    result ^= (result >> 6);
	    
	    result += (o == null) ? 1 : o.hashCode();
	    result += (result << 10);
	    result ^= (result >> 6);

	    result += (result << 3);
	    result ^= (result >> 11);
	    result += (result << 15);

		return result;
	}
	
	public static int hashCode(Bucket bucket1, Bucket bucket2) {
		int result = 0;
		result += bucket1.hashCode();
	    result += (result << 10);
	    result ^= (result >> 6);

	    result += bucket2.hashCode();
	    result += (result << 10);
	    result ^= (result >> 6);
	    
	    result += (result << 3);
	    result ^= (result >> 11);
	    result += (result << 15);

		return result;
	}
	
	

}
