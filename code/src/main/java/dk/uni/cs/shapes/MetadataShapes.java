package dk.uni.cs.shapes;

import dk.uni.cs.Main;
import dk.uni.cs.utils.ConfigManager;
import dk.uni.cs.utils.RdfUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.vocabulary.RDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataShapes {
    private final List<Statement> metadataGraphShapeStatement = new ArrayList<>();
    Map<String, String> prefixMappingsKV;
    Map<String, String> prefixMappingsVK = new HashMap<>();
    
    public void annotate() {
        System.out.println("Annotating with Metadata RDFGraphShape ...");
        RdfUtils.insertWatDivPrefixes(Main.getShapesModelIRI());
        this.prefixMappingsKV = RdfUtils.getPrefixes(Main.getShapesModelIRI());
        prefixMappingsKV.entrySet().iterator().forEachRemaining((k) -> {
            prefixMappingsVK.put(k.getValue(), k.getKey());
        });
        buildMetadataGraphShapeStatements();
        System.out.println(metadataGraphShapeStatement);
        addMetadataStatementsToShapesModel();
    }
    
    private void buildMetadataGraphShapeStatements() {
        
        String shape = "RDFGraphShape";
        metadataGraphShapeStatement.add(ResourceFactory.createStatement(new ResourceImpl(Main.getShapesPrefixURL() + shape),
                RDF.type, new ResourceImpl("http://www.w3.org/ns/shacl#NodeShape")));
        
        String query = "SELECT DISTINCT ?p (COUNT(?p) as ?propCount) (COUNT( DISTINCT ?p) as ?propDistinctCount) (COUNT(DISTINCT ?s) as ?distinctSubject) (COUNT(DISTINCT ?o) as ?distinctObject) WHERE { ?s ?p ?o.}  GROUP BY  ?p";
        
        ResultSet result = RdfUtils.runAQuery(query, Main.getRdfModelIRI());
        while (result.hasNext()) {
            String nameSpacePrefix = "";
            QuerySolution row = result.next();
            String propLocalName = row.get("p").asResource().getLocalName();
            if (prefixMappingsVK.containsKey(row.get("p").asResource().getNameSpace())) {
                nameSpacePrefix = prefixMappingsVK.get(row.get("p").asResource().getNameSpace());
            } else {
                String ns = row.get("p").asResource().getNameSpace();
                nameSpacePrefix = ns.split("/")[ns.split("/").length - 1];
            }
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + shape),
                    new PropertyImpl("http://www.w3.org/ns/shacl#property"),
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName)));
            
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName),
                    RDF.type,
                    new ResourceImpl("http://www.w3.org/ns/shacl#PropertyShape")));
            
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName),
                    new PropertyImpl("http://www.w3.org/ns/shacl#nodeKind"),
                    new ResourceImpl("http://www.w3.org/ns/shacl#IRI")));
            
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName),
                    new PropertyImpl("http://www.w3.org/ns/shacl#path"),
                    row.get("p").asResource()
            ));
            
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName),
                    new PropertyImpl(Main.getShapesStatsPrefixURL() + "distinctSubjectCount"),
                    row.get("distinctSubject").asLiteral()
            ));
            
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName),
                    new PropertyImpl(Main.getShapesStatsPrefixURL() + "distinctObjectCount"),
                    row.get("distinctObject").asLiteral()
            ));
            
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName),
                    new PropertyImpl(Main.getShapesStatsPrefixURL() + "count"),
                    row.get("propCount").asLiteral()
            ));
            
            metadataGraphShapeStatement.add(ResourceFactory.createStatement(
                    new ResourceImpl(Main.getShapesPrefixURL() + nameSpacePrefix + "_" + propLocalName),
                    new PropertyImpl(Main.getShapesStatsPrefixURL() + "distinctCount"),
                    row.get("propDistinctCount").asLiteral()
            ));
        }
    }
    
    private void addMetadataStatementsToShapesModel() {
        System.out.println("Writing Metadata Shape into Model: " + Main.getShapesModelIRI());
        Dataset dataset = RdfUtils.getTDBDataset();
        dataset.begin(ReadWrite.WRITE);
        Model graphModel = dataset.getNamedModel(Main.getShapesModelIRI());
        graphModel.add(metadataGraphShapeStatement);
        graphModel.close();
        dataset.commit();
        dataset.end();
        dataset.close();
    }
}
