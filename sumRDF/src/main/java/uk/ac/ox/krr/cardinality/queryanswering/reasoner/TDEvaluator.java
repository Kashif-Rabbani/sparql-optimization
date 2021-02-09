package uk.ac.ox.krr.cardinality.queryanswering.reasoner;

import uk.ac.ox.krr.cardinality.model.ResourceType;
import uk.ac.ox.krr.cardinality.queryanswering.model.Query.Atom;
import uk.ac.ox.krr.cardinality.queryanswering.model.UnificationFreeTD;
import uk.ac.ox.krr.cardinality.queryanswering.model.UnificationFreeTD.DNode;
import uk.ac.ox.krr.cardinality.queryanswering.model.UnificationFreeTD.Substitution;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;

public class TDEvaluator {

	Summary summary;
	double result;
	UnificationFreeTD query;
	
	public TDEvaluator(Summary s, UnificationFreeTD tree) {
		this.summary = s;
		query = tree;
		result = 0.0;
		iterators(query.root);
	}
	
	protected void iterators(DNode node) {
		node.iterators = new EdgeIterator[node.children.length];
		for (int childIndex = 0; childIndex < node.children.length; childIndex++) {
			if (node.children[childIndex] instanceof Atom) {
				Atom atom = (Atom)node.children[childIndex];
				if (!atom.boundObject && !atom.boundSubject) {
					node.iterators[childIndex] = summary.iteratorByPredicate(atom.predicateIndex);
				} else if (atom.boundSubject && atom.boundObject) {
					node.iterators[childIndex] = summary.iteratorByEdge(atom.subjectIndex, atom.predicateIndex, atom.objectIndex);
				} else if (atom.boundSubject) {
					node.iterators[childIndex] = summary.iteratorBySubjectPredicate(atom.subjectIndex, atom.predicateIndex);
				} else  {
					node.iterators[childIndex] = summary.iteratorByObjectPredicate(atom.objectIndex,  atom.predicateIndex);
				}
			} else if (node.children[childIndex] instanceof DNode){
				iterators((DNode) node.children[childIndex]);
			}
		}
	}

	public double evaluate() {
		assert query.m_atoms.length > 0;
		Bucket[] tau =  new Bucket[query.m_bindings.length];
		for (int index = 0; index < tau.length; index++) {
			if (query.m_bindings[index].resourceType != ResourceType.VARIABLE) {
				tau[index] = summary.getBucket(query.m_bindings[index]);
				if (tau[index] == null)
					return 0.0;
			}
		}
		Substitution emptySub = new Substitution(query, tau);
		populate(query.root, emptySub);
		emptySub = emptySub.restrict(query.root.intersectionVariables);
		this.result = query.root.hashTable.get(emptySub);
		query.clear();
		return result;
	}

	private void populate(DNode node, Substitution sigma) {
		if (!node.hashTable.containsKey(sigma)) {
			constructTable(node, sigma);
		}
	}

	private void constructTable(DNode node, Substitution sigma) {
		construct(node, sigma, 1.0, 0);
	}

	private void construct(DNode node, Substitution sigma, double f, int childIndex) {
		if (childIndex == node.children.length) {
			Substitution rho = sigma.restrict(node.intersectionVariables);
			node.hashTable.add(rho, f);
		} else if (node.children[childIndex] instanceof Atom) {
			Atom atom = (Atom) node.children[childIndex];
			node.iterators[childIndex].open(sigma.buckets);
			while(node.iterators[childIndex].hasNext()) {
				Edge current = node.iterators[childIndex].next();
				if (!atom.equalityCheck || current.subject == current.object) {
					int subjectIndex = getSubjectDivisor(sigma, atom);
					int objectIndex = getObjectDivisor(sigma, atom);
					sigma.update(atom, current);
					double divisor = combineDivisors(sigma, subjectIndex, objectIndex);
					double factor = f * (current.weight / divisor);
					construct(node, sigma, factor, childIndex + 1);
				}
			}
		} else {
			DNode child = (DNode) node.children[childIndex];
			Substitution rho = sigma.restrict(child.intersectionVariables);
			populate(child, rho);
			rho = sigma.restrict(child.intersectionVariables);
			double factor = child.hashTable.get(rho);
			if (factor >0) {
				construct(node, sigma, f * factor, childIndex + 1);
			}
		}
	}


	protected long combineDivisors(Substitution sigma, int subjectIndex, int objectIndex) {
		long divisor = 1;
		if (subjectIndex >= 0) {
			divisor *= sigma.get(subjectIndex).size();
		}
		if (objectIndex >= 0) {
			divisor *= sigma.get(objectIndex).size();
		}
		return divisor;
	}

	protected int getObjectDivisor(Substitution sigma, Atom atom) {
		int objectIndex = -1;
		if (query.m_bindings[atom.objectIndex].resourceType == ResourceType.IRI_REFERENCE)
			objectIndex = atom.objectIndex;
		else if (query.m_bindings[atom.objectIndex].resourceType == ResourceType.LITERAL)
			objectIndex = atom.objectIndex;
		else if (query.m_bindings[atom.objectIndex].resourceType == ResourceType.BNODE)
			objectIndex = atom.objectIndex;
		else if (atom.boundObject && sigma.get(atom.objectIndex) != null)
			objectIndex = atom.objectIndex;
		else if (atom.equalityCheck)
			objectIndex = atom.objectIndex;
		return objectIndex;
	}

	protected int getSubjectDivisor(Substitution sigma, Atom atom) {
		int subjectIndex = -1;
		if (query.m_bindings[atom.subjectIndex].resourceType == ResourceType.IRI_REFERENCE)
			subjectIndex = atom.subjectIndex;
		else if (query.m_bindings[atom.subjectIndex].resourceType == ResourceType.LITERAL)
			subjectIndex = atom.subjectIndex;
		else if (query.m_bindings[atom.subjectIndex].resourceType == ResourceType.BNODE)
			subjectIndex = atom.subjectIndex;
		else if (atom.boundSubject && sigma.get(atom.subjectIndex) != null)
			subjectIndex = atom.subjectIndex;
		return subjectIndex;
	}
}
