package uk.ac.ox.krr.cardinality.queryanswering.reasoner;

import uk.ac.ox.krr.cardinality.model.ResourceType;
import uk.ac.ox.krr.cardinality.queryanswering.model.Query.Atom;
import uk.ac.ox.krr.cardinality.queryanswering.model.SPARQLQuery;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.SPARQLEvaluator.PartitionBase;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;

public class ApproximateEvaluator {
	final private Summary summary;

	double result;
	SPARQLQuery query;

	PartitionBase base;

	// tau maps variables, resources, and literals to buckets
	Bucket[] tau;

	// edges for the query
	Edge[] edges;
	
	EdgeIterator[] iterators;
	
	
	boolean containsSpuriousResources = false;

	public ApproximateEvaluator(Summary s, SPARQLQuery q) {
		this.summary = s;
		query = q;
		base = new PartitionBase(query);
		tau = new Bucket[query.m_bindings.length];
		edges = new Edge[query.m_atoms.length];
		iterators = new EdgeIterator[query.m_atoms.length];
		
		for (int atomIndex = 0; atomIndex < query.m_atoms.length; atomIndex++) {
			Atom atom = query.m_atoms[atomIndex];
			
			if (query.m_bindings[atom.subjectIndex].resourceType != ResourceType.VARIABLE && tau[atom.subjectIndex] == null) {
				tau[atom.subjectIndex] = summary.getBucket(query.m_bindings[atom.subjectIndex]);
				if (tau[atom.subjectIndex] == null)
					containsSpuriousResources = true;
			}
			
			if (query.m_bindings[atom.predicateIndex].resourceType != ResourceType.VARIABLE && tau[atom.predicateIndex] == null) {
				tau[atom.predicateIndex] = summary.getBucket(query.m_bindings[atom.predicateIndex]);
				if (tau[atom.predicateIndex] == null)
					containsSpuriousResources = true;
			}
			
			if (query.m_bindings[atom.objectIndex].resourceType != ResourceType.VARIABLE && tau[atom.objectIndex] == null) {
				tau[atom.objectIndex] = summary.getBucket(query.m_bindings[atom.objectIndex]);
				if (tau[atom.objectIndex] == null)
					containsSpuriousResources = true;
			}
			
			if (!atom.boundObject && !atom.boundSubject) {
				iterators[atomIndex] = summary.iteratorByPredicate(atom.predicateIndex);
			} else if (atom.boundSubject && atom.boundObject) {
				iterators[atomIndex] = summary.iteratorByEdge(atom.subjectIndex, atom.predicateIndex, atom.objectIndex);
			} else if (atom.boundSubject) {
				iterators[atomIndex] = summary.iteratorBySubjectPredicate(atom.subjectIndex, atom.predicateIndex);
			} else {
				iterators[atomIndex] = summary.iteratorByObjectPredicate(atom.objectIndex, atom.predicateIndex);
			}
		}
		
		result = 0.0;
	}

	public double evaluate() {
		assert query.m_atoms.length > 0;
		if (!containsSpuriousResources) {
			evaluate(0);
			return result;
		} else {
			return 0.0;
		}
	}

	private void evaluate(int atomIndex) {
		if (atomIndex == query.m_atoms.length) {
			process();
		} else {
			Atom atom = query.m_atoms[atomIndex];
			iterators[atomIndex].open(tau);
			while(iterators[atomIndex].hasNext()) {
				Edge current = iterators[atomIndex].next();
				if (!atom.equalityCheck || current.subject == current.object) {
					tau[atom.subjectIndex] = current.subject;
					tau[atom.objectIndex] = current.object;
					edges[atomIndex] = current;
					evaluate(atomIndex + 1);
				}
			}
		}
	}
	

	private void process() {
		double tau_contr = 1;
		for (int atomIndex = 0; atomIndex < query.m_atoms.length; atomIndex++) {
			Atom atom = query.m_atoms[atomIndex];
			double edge_contr = edges[atomIndex].weight;
			double divisor = 1.0;
			if (query.m_bindings[atom.subjectIndex].resourceType != ResourceType.VARIABLE) {
				// selection
				divisor *= tau[atom.subjectIndex].size(); 
			} else if (atom.boundSubject) {
				// join variable
				divisor *= tau[atom.subjectIndex].size(); 
			}
			if (query.m_bindings[atom.objectIndex].resourceType != ResourceType.VARIABLE) {
				divisor *= tau[atom.objectIndex].size(); 
			} else if (atom.boundObject) {
				divisor *= tau[atom.objectIndex].size(); 
			}
			tau_contr *= edge_contr/divisor;
		}
		result += tau_contr;
	}

}
