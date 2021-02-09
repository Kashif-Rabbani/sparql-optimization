package uk.ac.ox.krr.cardinality.queryanswering.reasoner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import uk.ac.ox.krr.cardinality.model.Resource;
import uk.ac.ox.krr.cardinality.model.ResourceType;
import uk.ac.ox.krr.cardinality.model.SinglyLinkedList;
import uk.ac.ox.krr.cardinality.queryanswering.model.Query.Atom;
import uk.ac.ox.krr.cardinality.queryanswering.model.SPARQLQuery;
import uk.ac.ox.krr.cardinality.summarisation.model.Bucket;
import uk.ac.ox.krr.cardinality.summarisation.model.Edge;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;

public class SPARQLEvaluator {

	private Set<Edge> queryImage = new LinkedHashSet<Edge>();
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

	public SPARQLEvaluator(Summary s, SPARQLQuery q) {
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

	protected void process() {
		Partition partition = base.partitions.getHead();
		
		for (Edge edge : edges) {
			queryImage.add(edge);
		}

		while (partition != null) {
			if (partition.isTauUnifiable(tau)) {
				result += queryFactor(partition, tau) * minimalSolutions(partition, tau);
			}
			partition = partition.nextPartition;
		}
		queryImage.clear();
	}


	private double minimalSolutions(Partition partition, Bucket[] tau) {
		double sum = 0.0;
		for (Partition finer : partition.finerPartitions) {
			if (finer.isTauUnifiable(tau)) {
				sum += minimalSolutions(finer, tau);
			}
		}
		return solutions(partition, tau) - sum;
	}

	private double solutions(Partition partition, Bucket[] tau) {
		double result = 1.0;
		for (int varpos : partition.q_mgu.variablePositions) {
			result *= (double) tau[partition.query.resourcesToPositions.get(partition.q_mgu.m_bindings[varpos])].size();
		}
		return result;
	}

	private double queryFactor(Partition p, Bucket[] tau) {
		double result = 1.0;
		for (Edge edge : queryImage) {
			result = result * atomFactor(p, tau, edge);
		}
		return result;
	}

	private double atomFactor(Partition partition, Bucket[] tau, Edge edge) {
		final int preimageSize = preimage(partition, tau, edge);
		if (preimageSize > edge.weight)
			return 0.0;
		double size = (double) edge.subject.size() * (double) edge.object.size();
		double result = 1.0;
		for (int i = 0; i < preimageSize; i++) {
			result *= (double) (edge.weight - i) / (double) (size - i);
		}
		return result;
	}

	private int preimage(Partition partition, Bucket[] tau, Edge edge) {
		int size = 0;
		for (Atom atom : partition.q_mgu.m_atoms) {
			if (tau[atom.subjectIndex] == edge.subject && tau[atom.predicateIndex] == edge.predicate
					&& tau[atom.objectIndex] == edge.object)
				size++;
		}
		return size;
	}

	public static class PartitionBase {
		
		public final SinglyLinkedList<Partition> partitions;
		public int size;
		
		private int[] subjectForPartition;
		private int[] predicateForPartition;
		private int[] objectForPartition;
		private ArrayList<Atom>[] blocks;
		private int[] mgu;

		@SuppressWarnings("unchecked")
		public PartitionBase(SPARQLQuery query) {
			
			subjectForPartition = new int[query.m_atoms.length];
			predicateForPartition = new int[query.m_atoms.length];
			objectForPartition = new int[query.m_atoms.length];
			
			blocks = (ArrayList<Atom>[]) new ArrayList[query.m_atoms.length];
			for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
				blocks[blockIndex] = new ArrayList<>();
			}
			mgu = new int[query.m_bindings.length];
			
			partitions = new PartitionList();
			size = 0;
			computePartitions(query);
			Partition current = partitions.getHead();
			while (current != null) {
				Partition finer = partitions.getHead();
				while (finer != null) {
					if (current.isFiner(finer))
						current.finerPartitions.add(finer);
					finer = finer.nextPartition;
				}
				current = current.nextPartition;
			}
		}

		protected void computePartitions(SPARQLQuery query) {
			int[] p = new int[query.m_atoms.length];
			int[] m = new int[query.m_atoms.length];

			// computing the initial trivial partition
			trivialPartition(query, p);
			int pLengthMinusOne = p.length -1;
			Partition partition;
			while (true) {
				int index = findDecrementable(p);
				if (index > 0) {
					assert index > 0;
					p[index]--;
					m[index] = Math.max(p[index - 1], m[index - 1]);
					for (int i = index + 1; i < p.length; i++) {
						m[i] = Math.max(p[i - 1], m[i - 1]);
						p[i] = m[i] + 1;
					}
					int countPartitions = Math.max(p[pLengthMinusOne], m[pLengthMinusOne]) + 1;
					if (isUnifiable(query, p, countPartitions)) {
						partition = new Partition(query, p, countPartitions, mgu, blocks);
						partition.computeQuery();
						partitions.append(partition);
						size++;
					}
				} else {
					break;
				}
			}
		}
		
		protected static int findDecrementable(int[] partition) {
			for (int i = partition.length - 1; i >= 0; i--) {
				if (partition[i] > 0) {
					return i;
				}
			}
			return -1;
		}

		protected void trivialPartition(SPARQLQuery query, int[] p) {
			for (int index = 0; index < p.length; index++) {
				p[index] = index;
				blocks[index].add(query.m_atoms[index]);
			}
			for (int i = 0; i < mgu.length; i++) {
				mgu[i] = i;
			}
			Partition partition = new Partition(query, p, p.length, mgu, blocks);
			
			partition.atomMap = new int[query.m_atoms.length];
			for (int i = 0; i < partition.atomMap.length; i++) {
				partition.atomMap[i] = i;
			}
			partition.q_mgu = query;
			partitions.append(partition);
			size++;
		}
		
		
		protected  boolean isUnifiable(SPARQLQuery query, int p[], int countPartitions) {
			Arrays.fill(subjectForPartition, -1);
			Arrays.fill(predicateForPartition, -1);
			Arrays.fill(objectForPartition, -1);
			
			for (ArrayList<Atom> block : blocks)
				block.clear();
			
			for (int index = 0; index < mgu.length; index++) 
				mgu[index] = index;			
			
			 for (int atomIndex = 0; atomIndex < p.length; atomIndex++) {
	                int partitionIndex = p[atomIndex];
	                
	                Atom atom = query.m_atoms[atomIndex];

	                blocks[partitionIndex].add(atom);
	            
	                int subjectIndex = atom.subjectIndex;
	                if (subjectForPartition[partitionIndex] < 0)
	                	subjectForPartition[partitionIndex] = subjectIndex;
	                
	                int predicateIndex = atom.predicateIndex;
	                
	                if (predicateForPartition[partitionIndex] < 0)
	                	predicateForPartition[partitionIndex] = predicateIndex;
	                
	                int objectIndex = atom.objectIndex;
	                if (objectForPartition[partitionIndex] < 0)
	                	objectForPartition[partitionIndex] = objectIndex;
	                
	                // update mgu for subject
	                int subjectPartitionIndex = mgu[subjectForPartition[partitionIndex]];
	                Resource subjectPartitionResource = query.m_bindings[subjectPartitionIndex];
	                int subjectAtomIndex = mgu[subjectIndex];
	                Resource subjectAtomResource = query.m_bindings[subjectAtomIndex];
	                
	                if (!updateMgu(partitionIndex, subjectPartitionResource, subjectPartitionIndex, subjectAtomResource, subjectAtomIndex))
	                	return false;
	                
	                // update mgu for predicate
	                int predicatePartitionIndex = mgu[predicateForPartition[partitionIndex]];
	                Resource predicatePartitionResource = query.m_bindings[predicatePartitionIndex];
	                int predicateAtomIndex = mgu[predicateIndex];
	                Resource predicateAtomResource = query.m_bindings[predicateAtomIndex];
	                if (!updateMgu(partitionIndex, predicatePartitionResource, predicatePartitionIndex, predicateAtomResource, predicateAtomIndex))
	                	return false;
	                
	                // update mgu for object
	                int objectPartitionIndex = mgu[objectForPartition[partitionIndex]];
	                Resource objectPartitionResource = query.m_bindings[objectPartitionIndex];
	                int objectAtomIndex = mgu[objectIndex];
	                Resource objectAtomResource = query.m_bindings[objectAtomIndex];
	                if (!updateMgu(partitionIndex, objectPartitionResource, objectPartitionIndex, objectAtomResource, objectAtomIndex))
	                	return false;
	            }
			 
			return true;
		}
		
		private boolean updateMgu(int partitionIndex, Resource partitionRepr, int indexPartitionRepr, Resource atomRepr, int indexAtomRepr) {
            if (partitionRepr != atomRepr) {
                if (atomRepr.resourceType == ResourceType.VARIABLE && partitionRepr.resourceType == ResourceType.VARIABLE) {
                    //mgu[atomRepresentative -> partitionRepresentative];
                	replaceInMgu(indexAtomRepr, indexPartitionRepr);
                } else if (atomRepr.resourceType == ResourceType.VARIABLE){
                    // mgu[atomRepresentative -> partitionRepresentative];
                	replaceInMgu(indexAtomRepr, indexPartitionRepr);
                } else if (partitionRepr.resourceType == ResourceType.VARIABLE) {
                    // mgu[subjectReprForPartition -> subjectMguAtom];
                	replaceInMgu(indexPartitionRepr,indexAtomRepr);
                } else {
                    return false;
                }
            }
            return true;
		}

		private void replaceInMgu(int i, int j) {
			for (int index = 0; index < mgu.length; index++) 
				if (mgu[index] == i)
					mgu[index] = j;
		}
	}

	public static class Partition {

		private final SPARQLQuery query;

		// we represent a partition as an array of size |query.atoms|
		// two atoms are in the same block iff p[index_atom1] == p[index_atom2]
		public final int[] partition;


		private final Atom[][] blocks;

		// associates to each term in the query a unique representative based on
		// the partition
		private int[] gamma;

		// the query after applying the mgu
		private SPARQLQuery q_mgu;

		// maps atoms position in the mguQuery to atoms positions in the
		// original query
		private int[] atomMap;

		protected Partition nextPartition;

		public final List<Partition> finerPartitions;

		public String toString() {
			return Arrays.toString(partition);
		}
		
		public Partition(SPARQLQuery q, int[] p, int blockCount, int[] mgu, ArrayList<Atom>[] blockList) {
			query = q;
			partition = Arrays.copyOf(p, p.length);
			finerPartitions = new ArrayList<Partition>();
			blocks = new Atom[blockCount][];
			this.gamma = Arrays.copyOf(mgu, mgu.length);
			for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
				blocks[blockIndex] = blockList[blockIndex].toArray(new Atom[blockList[blockIndex].size()]);
			}
		}
		
		public boolean isTauUnifiable(Bucket[] tau) {
			for (int i = 0; i < gamma.length; i++) {
				if (tau[i] != tau[gamma[i]])
					return false;
			}
			return true;
		}
		
		public int numberOfBlocks() {
			return this.blocks.length;
		}
		
		protected void computeQuery() {
			this.atomMap = new int[query.m_atoms.length];
			q_mgu = new SPARQLQuery(query, gamma, atomMap);
			assert this.atomMap != null;
		}

		private boolean isFiner(Partition finer) {
			if (finer.numberOfBlocks() < numberOfBlocks()) {
				for (Atom[] block : blocks) {
					if (!Arrays.stream(finer.blocks).anyMatch(finerBlock -> isContained(block, finerBlock)))
						return false;
				}
				return true;
			}
			return false;
		}

		private boolean isContained(Atom[] block, Atom[] finerBlock) {
			for (Atom atom : block) {
				if (!Arrays.stream(finerBlock).anyMatch(b -> b == atom))
					return false;
			}
			return true;
		}
	}

	static class PartitionList extends SinglyLinkedList<Partition> {

		@Override
		public Partition getNext(Partition element) {
			return element.nextPartition;
		}

		@Override
		protected void setNext(Partition element, Partition next) {
			element.nextPartition = next;
		}

	}
	

}
