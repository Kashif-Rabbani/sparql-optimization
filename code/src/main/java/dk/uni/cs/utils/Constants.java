package dk.uni.cs.utils;

public class Constants {

    private static String PREFIXES = "PREFIX sh: <http://www.w3.org/ns/shacl#>\n" +
            "PREFIX sim-methods: <http://dbtune.org/sim-methods/resource/>\n" +
            "PREFIX dbpedia: <http://dbpedia.org/resource/>\n" +
            "PREFIX mbz: <http://dbtune.org/musicbrainz/resource/vocab/>\n" +
            "PREFIX event: <http://dbtune.org/classical/resource/event/>\n" +
            "PREFIX property: <http://dbtune.org/classical/resource/vocab/>\n" +
            "PREFIX cmno: <http://purl.org/ontology/classicalmusicnav#>\n" +
            "PREFIX composer: <http://dbtune.org/classical/resource/composer/>\n" +
            "PREFIX style: <http://dbtune.org/classical/resource/style/>\n" +
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX sim: <http://purl.org/ontology/similarity/>\n" +
            "PREFIX cmn: <http://dbtune.org/cmn/resource/>\n" +
            "PREFIX geo: <http://sws.geonames.org/>\n" +
            "PREFIX mo: <http://purl.org/ontology/mo/>\n" +
            "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
            "PREFIX type: <http://dbtune.org/classical/resource/type/>\n" +
            "PREFIX bio: <http://purl.org/vocab/bio/0.1/>\n" +
            "PREFIX wikipedia: <http://en.wikipedia.org/wiki/>\n" +
            "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
            "PREFIX dct: <http://purl.org/dc/terms/>\n" +
            "PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>\n" +
            "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX schema: <http://schema.org/>\n" +
            "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
            "PREFIX mo: <http://purl.org/ontology/mo/>\n " +
            "PREFIX stat: <http://dbTunes.shacl.stat.io/>\n " +
            "PREFIX shape: <http://dbTunes.shacl.io/>\n";

    public static String getPREFIXES() {
        //TODO Solve the issue of manually adding stat: prefix here
        return PREFIXES;
    }
}
