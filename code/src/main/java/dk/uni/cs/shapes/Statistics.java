package dk.uni.cs.shapes;

import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.RDF;

public class Statistics {
    private Triple triple;
    private Integer minCount;
    private Integer maxCount;
    private Integer totalCount;
    private Integer distinctCount;
    private Float average = 0f;
    private String shapeIRI; // This IRI can be of a Subject, Predicate, or an object
    private String subject;
    private Integer subjectCount;
    private String shapeOrClassLocalName;
    private Integer distinctSubjectCount = -1;
    private Integer distinctObjectCount = -1;


    public Statistics(Triple triple, String shapeIRI, Integer minCount, Integer maxCount, Integer totalCount, Integer distinctCount, String shapeOrClassLocalName) {
        this.triple = triple;
        this.shapeIRI = shapeIRI;
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.totalCount = totalCount;
        this.distinctCount = distinctCount;
        this.distinctObjectCount = distinctCount;
        this.shapeOrClassLocalName = shapeOrClassLocalName;
    }
    // Constructor without triple
    public Statistics(String shapeIRI, Integer minCount, Integer maxCount, Integer totalCount, Integer distinctCount, String shapeOrClassLocalName) {
        this.shapeIRI = shapeIRI;
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.totalCount = totalCount;
        this.distinctCount = distinctCount;
        this.distinctObjectCount = distinctCount;
        this.shapeOrClassLocalName = shapeOrClassLocalName;
    }
    
    
    @Override
    public String toString() {
        return "Statistics{" +
                "triple=" + triple +
                ", minCount=" + minCount +
                ", maxCount=" + maxCount +
                ", totalCount=" + totalCount +
                ", distinctCount=" + distinctCount +
                ", average=" + average +
                ", shapeIRI='" + shapeIRI + '\'' +
                ", subject='" + subject + '\'' +
                ", subjectCount=" + subjectCount +
                ", shapeOrClassLocalName='" + shapeOrClassLocalName + '\'' +
                ", distinctSubjectCount=" + distinctSubjectCount +
                ", distinctObjectCount=" + distinctObjectCount +
                '}';
    }

    public void setSubject(String subject) { this.subject = subject; }

    public void setSubjectCount(Integer subjectCount) { this.subjectCount = subjectCount; }

    public Triple getTriple() { return triple; }

    public Integer getMinCount() { return minCount; }

    public Integer getMaxCount() { return maxCount; }

    public Integer getTotalCount() { return totalCount; }

    public Integer getDistinctCount() { return distinctCount; }

    public Float getAverage() { return average; }

    public String getShapeIRI() { return shapeIRI; }

    public String getSubject() { return subject; }

    public String getShapeOrClassLocalName() { return shapeOrClassLocalName; }

    public Integer getSubjectCount() { return subjectCount; }

    public Integer getDistinctSubjectCount() {
        return distinctSubjectCount;
    }

    public void setDistinctSubjectCount(Integer distinctSubjectCount) {
        this.distinctSubjectCount = distinctSubjectCount;
    }

    public Integer getDistinctObjectCount() {
        return distinctObjectCount;
    }

    public void setDistinctObjectCount(Integer distinctObjectCount) {
        this.distinctObjectCount = distinctObjectCount;
    }

}
