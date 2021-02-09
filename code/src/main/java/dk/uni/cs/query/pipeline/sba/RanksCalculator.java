package dk.uni.cs.query.pipeline.sba;

import dk.uni.cs.Main;
import dk.uni.cs.query.pipeline.SubQuery;
import dk.uni.cs.shapes.Statistics;
import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.RDF;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;


class RanksCalculator {
    private double countDistinctSubjects = 0d;
    private double countDistinctObjects = 0d;
    HashMap<Triple, List<Statistics>> tripleWithStats;
    HashMap<Triple, Boolean> isTriplePatternTypeDefined;
    
    RanksCalculator(StarsEvaluator starsEvaluator) {
        this.tripleWithStats = starsEvaluator.getTriplesWithStats();
        this.isTriplePatternTypeDefined = starsEvaluator.getIsTriplePatternTypeDefined();
    }
    
    double getRankWhenObjIsNotVar(SubQuery sq, Triple triple, double rank) {
        // ?s    ?p   objA          -->4. c_t / c_o  ...  TotalCount / distinct
        // subjA ?p   objA          -->6. c_t / c_s . c_o
        if (triple.getPredicate().isVariable()) {
            if (triple.getSubject().isVariable()) {
                rank = (double) Main.getAllTriplesCount() / (double) Main.getDistinctObjectsCount();
            } else {
                rank = (double) Main.getAllTriplesCount() / ((double) Main.getDistinctSubjectsCount() * (double) Main.getDistinctObjectsCount());
            }
            countDistinctSubjects = Main.getDistinctSubjectsCount();
            countDistinctObjects = Main.getDistinctObjectsCount();
        }
        
        if (!triple.getPredicate().isVariable()) {
            // ?s     rdf:type objA         -->11. getTotalSubjectCount
            // subjA  rdf:type objA         -->12. Assumption here 1
            
            if (triple.getPredicate().getURI().equals(RDF.type.toString())) {
                // subjA  rdf:type objA
                if (triple.getSubject().isVariable()) {
                    if (sq.getShapes().get(triple).size() > 0)
                        rank = sq.getShapes().get(triple).stream().mapToDouble(Statistics::getSubjectCount).max().getAsDouble();
                    //rank = (sq.getShapes().get(triple).stream().max(Comparator.comparing(Statistics::getSubjectCount))).get().getSubjectCount();
                    countDistinctObjects = rank;
                } else {
                    rank = 1;
                    countDistinctObjects = 1;
                }
                countDistinctSubjects = rank;
                
            } else {
                // ?s predA objA            -->7. getTotalSubjectCount/getDistinctCountPredicate
                // subjA predA objA         -->8. getPredicateCount/getSubjectCount
                if (triple.getSubject().isVariable()) {
                    double nom, den;
                    // If the triple belongs to a type defined star then it will correspond to i) Its type specific shape, ii) MetadataShape
                    // Otherwise the triple can correspond to many shapes including MetadataShape.
                    //FIXME: Should I select the stats of Metadata Shape when the triple does not belong to a type defined star ?
                    if (isTriplePatternTypeDefined.get(triple)) {
                        //Now find the predicateShapeProperty Node Shape
                        List<Statistics> typeSpecificStats = chooseTheAppropriateCandidateShape(triple, false);
                        nom = typeSpecificStats.get(0).getTotalCount();
                        den = typeSpecificStats.get(0).getDistinctObjectCount();
                        countDistinctSubjects = typeSpecificStats.get(0).getDistinctSubjectCount();
                        countDistinctObjects = den;
                    } else {
                        //Now find the Metadata Node Shape
                        List<Statistics> metadataShapeStats = chooseTheAppropriateCandidateShape(triple, true);
                        nom = metadataShapeStats.get(0).getTotalCount();
                        den = metadataShapeStats.get(0).getDistinctObjectCount();
                        countDistinctSubjects = metadataShapeStats.get(0).getDistinctSubjectCount();
                        countDistinctObjects = den;
                    }
                    /*
                    nom = sq.getShapes().get(triple).stream().mapToDouble(Statistics::getTotalCount).max().getAsDouble();
                    den = calculateDistinctObjectCount(sq, triple);
                    //nom = (sq.getShapes().get(triple).stream().max(Comparator.comparing(Statistics::getSubjectCount))).get().getSubjectCount();
                    //den = (sq.getShapes().get(triple).stream().max(Comparator.comparing(Statistics::getDistinctCount))).get().getDistinctCount();
                    
                    countDistinctSubjects = calculateDistinctSubjectCount(sq, triple);
                    countDistinctObjects = den;*/
                    rank = nom / den;
                } else {
                    double nom = 0, den = 0;
                    if (isTriplePatternTypeDefined.get(triple)) {
                        //Now find the predicateShapeProperty Node Shape
                        List<Statistics> typeSpecificStats = chooseTheAppropriateCandidateShape(triple, false);
                        nom = typeSpecificStats.get(0).getTotalCount();
                        den = typeSpecificStats.get(0).getSubjectCount();
                        countDistinctSubjects = typeSpecificStats.get(0).getDistinctSubjectCount();
                        countDistinctObjects = typeSpecificStats.get(0).getDistinctObjectCount();
                    } else {
                        //Now find the Metadata Node Shape
                        List<Statistics> metadataShapeStats = chooseTheAppropriateCandidateShape(triple, true);
                        nom = metadataShapeStats.get(0).getTotalCount();
                        den = metadataShapeStats.get(0).getSubjectCount();
                        countDistinctSubjects = metadataShapeStats.get(0).getDistinctSubjectCount();
                        countDistinctObjects = metadataShapeStats.get(0).getDistinctObjectCount();
                    }
                    /*
                    nom = sq.getShapes().get(triple).stream().mapToDouble(Statistics::getTotalCount).max().getAsDouble();
                    den = sq.getShapes().get(triple).stream().mapToDouble(Statistics::getSubjectCount).max().getAsDouble();
                    countDistinctSubjects = calculateDistinctSubjectCount(sq, triple);
                    countDistinctObjects = calculateDistinctObjectCount(sq, triple);*/
                    rank = nom / den;
                }
            }
        }
        return rank;
    }
    
    
    double getRankWhenObjIsVar(SubQuery sq, Triple triple, double rank) {
        // ?s    ?p   ?o          -->1. c_t
        // subjA ?p   ?o          -->2. c_t / c_s
        if (triple.getPredicate().isVariable()) {
            if (triple.getSubject().isVariable()) {
                rank = Main.getAllTriplesCount();
            } else {
                rank = (double) Main.getAllTriplesCount() / (double) Main.getDistinctSubjectsCount();
            }
            countDistinctSubjects = Main.getDistinctSubjectsCount();
            countDistinctObjects = Main.getDistinctObjectsCount();
        }
        
        if (!triple.getPredicate().isVariable()) {
            // ?s     rdf:type ?o         -->9. Count of rdf:type triples
            // subjA  rdf:type ?o         -->10. countRdfTypeTriples/ No. of subjects in the dataset with predicate rdf:type
            if (triple.getPredicate().getURI().equals(RDF.type.toString())) {
                if (triple.getSubject().isVariable()) {
                    rank = Main.getDistinctRdfTypeCount();
                } else {
                    rank = (double) Main.getDistinctRdfTypeCount() / Main.getDistinctRdfTypeSubjCount();
                    
                }
                countDistinctSubjects = Main.getDistinctRdfTypeSubjCount();
                countDistinctObjects = Main.getDistinctRdfTypeObjectsCount();
            } else {
                // ?s predA ?o            -->3. No. of triples in the dataset with predA
                // subjA predA ?o         -->5. No. of subjects in the dataset with predA
                if (triple.getSubject().isVariable()) {
                    
                    if (isTriplePatternTypeDefined.get(triple)) {
                        //Now find the predicateShapeProperty Node Shape
                        List<Statistics> typeSpecificStats = chooseTheAppropriateCandidateShape(triple, false);
                        
                        if (typeSpecificStats.size() > 0) {
                            rank = typeSpecificStats.get(0).getTotalCount();
                            countDistinctSubjects = typeSpecificStats.get(0).getDistinctSubjectCount();
                            countDistinctObjects = typeSpecificStats.get(0).getDistinctObjectCount();
                        }
                        else {
                            rank = 0;
                            countDistinctObjects = 0;
                            countDistinctSubjects = 0;
                        }
                        
                    } else {
                        //Now find the Metadata Node Shape
                        List<Statistics> metadataShapeStats = chooseTheAppropriateCandidateShape(triple, true);
                        rank = metadataShapeStats.get(0).getTotalCount();
                        countDistinctSubjects = metadataShapeStats.get(0).getDistinctSubjectCount();
                        countDistinctObjects = metadataShapeStats.get(0).getDistinctObjectCount();
                    }
                    //rank = sq.getShapes().get(triple).stream().mapToDouble(Statistics::getTotalCount).max().getAsDouble(); // old
                    
                } else {
                    double nom = 0, den = 0;
                    
                    if (isTriplePatternTypeDefined.get(triple)) {
                        //Now find the predicateShapeProperty Node Shape
                        List<Statistics> typeSpecificStats = chooseTheAppropriateCandidateShape(triple, false);
                        nom = typeSpecificStats.get(0).getSubjectCount();
                        den = typeSpecificStats.get(0).getDistinctObjectCount();
                        countDistinctSubjects = typeSpecificStats.get(0).getDistinctSubjectCount();
                        countDistinctObjects = typeSpecificStats.get(0).getDistinctObjectCount();
                    } else {
                        //Now find the Metadata Node Shape
                        List<Statistics> metadataShapeStats = chooseTheAppropriateCandidateShape(triple, true);
                        nom = metadataShapeStats.get(0).getSubjectCount();
                        den = metadataShapeStats.get(0).getDistinctObjectCount();
                        countDistinctSubjects = metadataShapeStats.get(0).getDistinctSubjectCount();
                        countDistinctObjects = metadataShapeStats.get(0).getDistinctObjectCount();
                    }
                    //nom = sq.getShapes().get(triple).stream().mapToDouble(Statistics::getSubjectCount).max().getAsDouble();
                    //den = calculateDistinctObjectCount(sq, triple);
                    rank = nom / den;
                }
                //countDistinctSubjects = calculateDistinctSubjectCount(sq, triple);
                //countDistinctObjects = calculateDistinctObjectCount(sq, triple);
            }
        }
        return rank;
    }
    
    @NotNull
    private List<Statistics> chooseTheAppropriateCandidateShape(Triple triple, boolean seekMetadataShape) {
        List<Statistics> stats = tripleWithStats.get(triple).stream().filter(statistics -> {
            boolean flag = false;
            if (seekMetadataShape) {
                if (statistics.getShapeOrClassLocalName().equals("RDFGraph")) {
                    flag = true;
                }
            } else {
                if (!statistics.getShapeOrClassLocalName().equals("RDFGraph")) {
                    flag = true;
                }
            }
            return flag;
        }).collect(Collectors.toList());
        if (stats.size() > 1) {
            System.out.println("WARNING: RanksCalculator has a problem! " + seekMetadataShape);
        }
        return stats;
    }
    
    private double calculateDistinctSubjectCount(SubQuery sq, Triple triple) {
        return sq.getShapes().get(triple).stream().mapToDouble(Statistics::getDistinctSubjectCount).max().getAsDouble();
    }
    
    private double calculateDistinctObjectCount(SubQuery sq, Triple triple) {
        return sq.getShapes().get(triple).stream().mapToDouble(Statistics::getDistinctObjectCount).max().getAsDouble();
    }
    
    
    double getCountDistinctSubjects() {
        return countDistinctSubjects;
    }
    
    double getCountDistinctObjects() {
        return countDistinctObjects;
    }
    
}