package dk.uni.cs.query.pipeline;

import dk.uni.cs.Main;
import dk.uni.cs.extras.BGPVisitor;
import dk.uni.cs.extras.ElemVisitor;
import dk.uni.cs.extras.TrueCardinalityComputer;
import dk.uni.cs.shapes.Statistics;
import dk.uni.cs.utils.Tuple3;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.apache.jena.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.util.*;
import java.util.stream.Collectors;

public class QueryUtils {
    private static Expr filterExpression;
    
    public QueryUtils() {}
    
    public static Expr getFilterExpression() {
        return filterExpression;
    }
    
    public static ArrayList<HashSet<Triple>> getBGPs(Query query) {
        ArrayList<HashSet<Triple>> bgps = null;
        try {
            ElemVisitor elemVisitor = new ElemVisitor();
            ElementWalker.walk(query.getQueryPattern(), elemVisitor);
            filterExpression = elemVisitor.getFilterExpression();
            
            Op op = (new AlgebraGenerator()).compile(query);
            BGPVisitor bgpv = new BGPVisitor();
            OpWalker.walk(op, bgpv);
            bgps = bgpv.getBGPs();
        } catch (Exception e) {
            e.printStackTrace();
            //System.exit(1);
        }
        return bgps;
    }
    
    public void getStatisticsFromShapes(HashMap<Triple, List<Tuple3<String, String, String>>> triplesToShapesMapping, HashMap<Triple, List<Statistics>> triplesWithStats) {
        statsExtractor(triplesToShapesMapping, triplesWithStats);
    }
    
    HashMap<Triple, List<Statistics>> getStatisticsContainedInTheShapes(HashMap<Triple, List<Tuple3<String, String, String>>> triplesToShapesMapping) {
        HashMap<Triple, List<Statistics>> triplesWithStats = new HashMap<>();
        statsExtractor(triplesToShapesMapping, triplesWithStats);
        return triplesWithStats;
    }
    
    private void statsExtractor(HashMap<Triple, List<Tuple3<String, String, String>>> triplesToShapesMapping, HashMap<Triple, List<Statistics>> triplesWithStats) {
        triplesToShapesMapping.forEach((triple, list) -> {
            List<Statistics> tempStatList = new ArrayList<Statistics>();
            List<Statistics> stats = new ArrayList<Statistics>();
            //System.out.println("Triple: " + triple + " : " + list.size());
            
            List<String> listShapesIRIs = new ArrayList<>();
            boolean flag = false;
            //FIXME: The size limit should be removed and it should actually be handled properly e.g., if the triple belongs to a type defined star, then we should set the flag to true..
            if (list.size() < 5) {
                flag = true;
                list.forEach(tuple3 -> {
                    listShapesIRIs.add(tuple3._3);
                });
            }
            //Marker :- In Memory Mappings Resolver
            if (triple.getPredicate().toString().equals(RDF.type.toString()) && !triple.getObject().isVariable()) {
                if (Main.rdfTypePredicateTargetedNodeShapesMap.containsKey(triple.getObject().toString())) {
                    //System.out.println("CommonComponents - Contains RDF.type predicate");
                    //Main.rdfTypePredicateTargetedNodeShapesMap.get(triple.getObject().toString());
                    tempStatList.add(Main.rdfTypePredicateTargetedNodeShapesStatsMap.get(triple.getObject().toString()));
                }
            }
            if (Main.predicatesToShapesStatsMapping.containsKey(triple.getPredicate().toString())) {
                //System.out.println("CommonComponents - Contains predicate");
                stats = Main.predicatesToShapesStatsMapping.get(triple.getPredicate().toString());
                
                if (flag) {
                    stats = stats.stream().filter(statistics -> {
                        boolean exists = false;
                        if (listShapesIRIs.contains(statistics.getShapeIRI()))
                            exists = true;
                        return exists;
                    }).collect(Collectors.toList());
                }
                
                
                tempStatList.addAll(stats);
            }
    
            /*list.forEach(tuple -> {
                if (triple.getPredicate().toString().equals(RDF.type.toString())) {
                    
                    //In this case we treat <?x a :Composition> where :Composition is a class and there is no min & max count associated with the Node Shape.
                    // There is no shape property for RDF.type predicate
                    //long start = System.currentTimeMillis();
                    int subjectCount = (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "count", Main.getShapesModelIRI())).getInt();
                    Statistics statistics = new Statistics(
                            triple,
                            tuple._3,
                            1,
                            1, // default values for min and max count is 1 (Assumption)
                            subjectCount,
                            (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "distinctCount", Main.getShapesModelIRI())).getInt(),
                            tuple._2);
                    
                    statistics.setSubject(tuple._1);
                    statistics.setSubjectCount(subjectCount);
                    tempStatList.add(statistics);
                    //long end = System.currentTimeMillis() - start;
                    
                    //System.out.println("RDF.Type : " + end  + " MS") ;
                }
                else {
                    Statistics statistics;
                    if (Main.shapePropStatsMap.containsKey(tuple._3)) {
                        
                        statistics = new Statistics(
                                triple,
                                tuple._3,
                                Main.shapePropStatsMap.get(tuple._3).get(0),
                                Main.shapePropStatsMap.get(tuple._3).get(1),
                                Main.shapePropStatsMap.get(tuple._3).get(2),
                                Main.shapePropStatsMap.get(tuple._3).get(3),
                                tuple._2);
                        
                    } else {
                        statistics = new Statistics(
                                triple,
                                tuple._3,
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "minCount", Main.getShapesModelIRI())).getInt(),
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "maxCount", Main.getShapesModelIRI())).getInt(), // need a check on maxCount
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "count", Main.getShapesModelIRI())).getInt(),
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "distinctCount", Main.getShapesModelIRI())).getInt(),
                                tuple._2);
                        System.out.println("NOT FOUND IN MAP ->  " + tuple._3);
                    }
                    
                    statistics.setSubject(tuple._1);
                    
                    //if (Main.shapePropStatsMap.containsKey(tuple._2 + "Shape")) {
                    //    statistics.setSubjectCount(Main.shapePropStatsMap.get(tuple._2 + "Shape").get(2));
                    //} else {
                    statistics.setSubjectCount((RdfUtils.getFromInMemoryModel(
                            Main.getShapesPrefixURL() + tuple._2 + "Shape",
                            Main.getShapesStatsPrefixURL() + "count",
                            Main.getShapesModelIRI())).getInt());
                    //System.out.println("NOT FOUND IN the MAP ->  " + tuple.toString());
                    
                    //}
                    
                    
                    // It means that the current property only belongs to the candidate shape RDFGraph
                    if (tuple._2.equals("RDFGraph")) {
                        if (Main.shapePropStatsMap.containsKey(tuple._3)) {
                            //distinct subject count
                            statistics.setDistinctSubjectCount(Main.shapePropStatsMap.get(tuple._3).get(4));
                            
                            //distinct object count
                            statistics.setDistinctObjectCount(Main.shapePropStatsMap.get(tuple._3).get(5));
                        } else {
                            System.out.println("NOT FOUND IN RDFGraph MAP ->  " + tuple._3);
                            //distinct subject count
                            statistics.setDistinctSubjectCount((RdfUtils.getFromInMemoryModel(
                                    tuple._3,
                                    Main.getShapesStatsPrefixURL() + "distinctSubjectCount",
                                    Main.getShapesModelIRI())).getInt());
                            
                            //distinct object count
                            statistics.setDistinctObjectCount((RdfUtils.getFromInMemoryModel(
                                    tuple._3,
                                    Main.getShapesStatsPrefixURL() + "distinctObjectCount",
                                    Main.getShapesModelIRI())).getInt());
                        }
                        
                    }
                    
                    tempStatList.add(statistics);
                }
            });*/
            
            triplesWithStats.put(triple, tempStatList);
        });
    }
    
    public String prettyPrint(Triple triple) {
        String statement = "";
        if (triple.getSubject().isURI()) {
            statement = triple.getSubject().getLocalName();
            
        } else {
            statement = triple.getSubject().toString();
        }
        if (triple.getPredicate().isURI()) {
            if (triple.getPredicate().toString().equals(RDF.type.toString())) {
                statement += " a ";
            } else {
                statement += " " + triple.getPredicate().getLocalName() + " ";
            }
        } else {
            statement += triple.getPredicate().toString();
        }
        if (triple.getObject().isURI()) {
            statement += triple.getObject().getLocalName();
        } else {
            statement += triple.getObject().toString();
        }
        return statement;
    }
    
    public String formatTriple(Triple triple) {
        String statement = "";
        if (triple.getSubject().isURI()) {
            statement = triple.getSubject().toString();
            
        } else {
            statement = triple.getSubject().toString();
        }
        if (triple.getPredicate().isURI()) {
            if (triple.getPredicate().toString().equals(RDF.type.toString())) {
                statement += " a ";
            } else {
                statement += " <" + triple.getPredicate().toString() + "> ";
            }
        } else {
            statement += " " + triple.getPredicate().toString() + " ";
        }
        if (triple.getObject().isURI()) {
            statement += "<" + triple.getObject().toString() + ">";
        } else {
            statement += triple.getObject().toString();
        }
        return statement;
    }
    
    public Op makeQueryAlgebra(Query query, BasicPattern bp, List<Var> projectedVariables) {
        Op algebraOp;
        algebraOp = new OpBGP(bp);
        
        if (filterExpression != null) {
            algebraOp = OpFilter.filter(filterExpression, algebraOp);
            //algebraOp = OpAssign.assign(algebraOp, (VarExprList) filterExpression);
        }
        if (query.getOrderBy() != null) algebraOp = new OpOrder(algebraOp, query.getOrderBy());
        
        algebraOp = new OpProject(algebraOp, projectedVariables);
        algebraOp = new OpSlice(algebraOp, query.getOffset(), query.getLimit());
        
        return algebraOp;
    }
    
    public static Op constructQueryAndPreserveTheGivenOrder(Query q, String queryId) {
        List<Triple> triplePatterns = getTriplePatterns(q);
        //computeTrueCardForCurrentQuery(queryId, triplePatterns);
        BasicPattern bpp = new BasicPattern();
        triplePatterns.forEach(bpp::add);
        return new QueryUtils().makeQueryAlgebra(q, bpp, q.getProjectVars());
    }
    
    private static void computeTrueCardForCurrentQuery(String queryId, List<Triple> triplePatterns) {
        TrueCardinalityComputer tCc = new TrueCardinalityComputer(triplePatterns);
        System.out.println("******* TrueCardinalityComputer **********");
        tCc.triplesWithCard.forEach(System.out::println);
        System.out.println("CARD,queryId,finalCard,cost");
        System.out.println("CARD," + queryId + "," + tCc.finalCard + "," + tCc.sumCard);
    }
    
    public static List<Triple> getTriplePatterns(Query query) {
        //final Set<Triple> triplePatterns = Sets.newHashSet();
        final List<Triple> triplesList = new ArrayList<>();
        ElementWalker.walk(query.getQueryPattern(), new ElementVisitorBase() {
            @Override
            public void visit(ElementTriplesBlock el) {
                Iterator<Triple> triples = el.patternElts();
                while (triples.hasNext()) {
                    Triple triple = triples.next();
                    //triplePatterns.add(triple);
                    triplesList.add(triple);
                }
            }
            
            @Override
            public void visit(ElementPathBlock el) {
                Iterator<TriplePath> triplePaths = el.patternElts();
                while (triplePaths.hasNext()) {
                    TriplePath tp = triplePaths.next();
                    if (tp.isTriple()) {
                        Triple triple = tp.asTriple();
                        //triplePatterns.add(triple);
                        triplesList.add(triple);
                    }
                }
            }
        });
        return triplesList;
    }
    
    // In case need to execute a query on an endpoint
    Long executeAndEvaluateQueryOverEndpoint(String q) {
        SPARQLRepository repo = new SPARQLRepository(Main.getEndPointAddress());
        RepositoryConnection conn = repo.getConnection();
        long durationIs = 0l;
        try {
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, q);
            long start = System.currentTimeMillis();
            TupleQueryResult res = query.evaluate();
            //return Long.parseLong(rs.next().getValue("objs").stringValue());
            int n = 0;
            while (res.hasNext()) {
                res.next();
                //System.out.println(res.next());
                n++;
            }
            long duration = System.currentTimeMillis() - start;
            durationIs = duration;
            //System.out.println("Done query \n" + q + ": \n duration=" + duration + "ms, results=" + n);
        } finally {
            conn.close();
            repo.shutDown();
        }
        //System.out.println(durationIs + " ms");
        return durationIs;
    }
}