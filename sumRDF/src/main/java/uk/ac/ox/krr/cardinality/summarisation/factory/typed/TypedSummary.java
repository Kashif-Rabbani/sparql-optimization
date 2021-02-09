package uk.ac.ox.krr.cardinality.summarisation.factory.typed;

import java.util.Set;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.summarisation.factory.SummaryFactory;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;
import uk.ac.ox.krr.cardinality.summarisation.model.Type;
import uk.ac.ox.krr.cardinality.util.Dictionary;

public abstract class TypedSummary extends SummaryFactory {
	
	protected Set<Bucket> disallowedOutgoing;
	protected Set<Bucket> disallowedIngoing;
	
	protected Dictionary dictionary;

	public TypedSummary(Summary graph, Dictionary dict) {
		this(graph, null, null);
		this.dictionary = dict;
	}
	
	public TypedSummary(Summary graph, Set<Bucket> out, Set<Bucket> in) {
		super(graph);
		this.disallowedOutgoing = out;
		this.disallowedIngoing = in;
	}
	
	protected void typeBuckets() {
		Type type = inputGraph.getType();
		while (type != null) {
			Bucket bucket4Type = summary.createBucket();
			Bucket gBucket = type.bucketByType.getHead();
			while (gBucket != null) {
				assert gBucket.size() == 1;
				Resource r = gBucket.resources.getHead();
				while (r != null) {
					Resource next = r.nextInBucket;
					r.nextInBucket = null;
					summary.addToBucket(r, bucket4Type);	
					r = next;
				}
				assert bucket4Type.size() == bucket4Type.resources.size();
				gBucket = gBucket.nextByType;
			}
			type = type.nextType;
		}
	}

}
