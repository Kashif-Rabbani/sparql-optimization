package uk.ac.ox.krr.cardinality.queryanswering.reasoner;

import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;

public interface EdgeIterator {
		
	public void open(Bucket[] tau);
	
	public boolean hasNext();
	
	public Edge next();

}
