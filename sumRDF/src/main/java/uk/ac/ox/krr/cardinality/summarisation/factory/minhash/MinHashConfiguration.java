package uk.ac.ox.krr.cardinality.summarisation.factory.minhash;

public  class MinHashConfiguration {
	
	protected int target;
	
	
	protected float threshold;
	
	// must be a power of 2
	protected int bins;
	
	protected short scheme_rows;
	protected short scheme_cols;
	protected short scheme_size;
	
	protected boolean classTyped;
	
	protected String[] disallowedOutgoing;
	protected String[] disallowedIngoing;
	
	public MinHashConfiguration(int tg) {
		target = tg;
		threshold = 0.15f;
		
		bins = 32768; 
		
		classTyped = true;
		
		scheme_rows = 40;
		scheme_cols = 2;	
		scheme_size = (short) (scheme_rows * scheme_cols);
		this.disallowedOutgoing = null;
		this.disallowedIngoing = null;	
	}
	
	public MinHashConfiguration() {
		this(20000);
		
	}
	
	public MinHashConfiguration(int tg, float th, int b, short rows, short cols, String[] out, String[] in) {
		target = tg;
		threshold = th;
		bins = b;
		scheme_rows = rows;
		scheme_cols = cols;
		disallowedOutgoing = out;
		disallowedIngoing = in;
	}

	int getTarget() {
		return target;
	}

	float getThreshold() {
		return threshold;
	}

	int getBins() {
		return bins;
	}

	short schemeRows() {
		return scheme_rows;
	}

	short schemeCols() {
		return scheme_cols;
	}

	String[] getDisallowedOutgoing() {
		return disallowedOutgoing;
	}

	String[] getDisallowedIngoing() {
		return disallowedIngoing;
	}

}
