package dk.uni.cs.extras;

import dk.uni.cs.query.pipeline.TriplesEvaluatorWithPlan;
import org.apache.jena.graph.Triple;

import java.util.ArrayList;
import java.util.List;

public class TrueCardinalityComputer {
    List<Triple> triples;
    public int finalCard;
    public int sumCard;
    public List<String> triplesWithCard;
    
    public TrueCardinalityComputer(List<Triple> triples) {
        this.triples = triples;
        compute();
    }
    
    private void compute() {
        triplesWithCard = new ArrayList<>();
        sumCard = 0;
        for (int i = 1; i <= triples.size(); i++) {
            int card = new TriplesEvaluatorWithPlan().executeCountQueryOnGraphDB(new TriplesEvaluatorWithPlan().buildCountQuery(triples.subList(0, i)));
            triplesWithCard.add(triples.get(i - 1) + " True Card: " + card);
            if (i != 1)
                sumCard += card;
            if (i == triples.size())
                finalCard = card;
        }
        //triplesWithCard.forEach(System.out::println);
    }
}
