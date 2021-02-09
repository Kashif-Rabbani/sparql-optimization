package uk.ac.ox.krr.cardinality.eval;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.rdf4j.RDF4JException;

import uk.ac.ox.krr.cardinality.queryanswering.model.SPARQLQuery;
import uk.ac.ox.krr.cardinality.queryanswering.model.UnificationFreeTD;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.Reasoner;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.TimedSPARQLEvaluator;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.TimedSPARQLEvaluator.TimeOutException;
import uk.ac.ox.krr.cardinality.queryanswering.util.Prefixes;
import uk.ac.ox.krr.cardinality.summarisation.factory.minhash.MinHash;
import uk.ac.ox.krr.cardinality.summarisation.factory.minhash.MinHashConfiguration;
import uk.ac.ox.krr.cardinality.summarisation.factory.typed.ComplexTypedSummary;
import uk.ac.ox.krr.cardinality.summarisation.model.Summary;
import uk.ac.ox.krr.cardinality.util.Dictionary;
import uk.ac.ox.krr.cardinality.util.Importer;

public class Evaluator {
    
    public static final int N = 20;
    public static final long TIMEOUT = 1000; // 5 MINUTES
    
    public final File inputFolder;
    public final File outputFile;
    public final String graphName;
    public final File importSummary;
    public final int target;
    
    public Summary inputGraph;
    public Summary summary;
    
    public Dictionary dictionary;
    public final Prefixes prefixes;
    
    private List<TestResult> linear;
    private List<TestResult> star;
    private List<TestResult> flake;
    private List<TestResult> complex;
    
    private SummaryResult summaryResult;
    
    public Evaluator(String input, String output, String graph, String summary, int t) {
        inputFolder = new File(input);
        if (!inputFolder.exists())
            throw new RuntimeException("input folder does not exist");
        graphName = graph;
        target = t;
        if (summary != null) {
            importSummary = new File(summary);
        } else {
            importSummary = null;
        }
        outputFile = new File(output);
        linear = new ArrayList<>();
        star = new ArrayList<>();
        flake = new ArrayList<>();
        complex = new ArrayList<>();
        summaryResult = new SummaryResult();
        prefixes = new Prefixes();
    }
    
    public void run() throws Exception {
        readGraph();
        File[] linearFiles = linearQueries();
        File[] starFiles = starQueries();
        File[] snowflakeFiles = flakeQueries();
        File[] complexFiles = complexQueries();
        //init(linearFiles, starFiles, snowflakeFiles, complexFiles);
        //queryGraph(linearFiles, starFiles, snowflakeFiles, complexFiles);
        if (importSummary == null) {
            long start = System.currentTimeMillis();
            constructSummary();
            long end = System.currentTimeMillis() - start;
            System.out.println("Time taken to construct summary: " + end + " MS");
        } else {
            readSummary();
        }
//        querySummary(linearFiles, starFiles, snowflakeFiles, complexFiles);
//        PrintStream out = new PrintStream(new FileOutputStream(outputFile, false));
//        print(out);
//        out.close();
    }
    
    private void readSummary() throws Exception {
        System.out.println("reading summary from file");
        summary = new Summary();
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(importSummary));
        summary.readExternal(ois);
        ois.close();
        
    }
    
    private void constructSummary() throws IOException {
        long start = System.currentTimeMillis();
        summary = new ComplexTypedSummary(inputGraph, dictionary).getSummary();
        summaryResult.typed_time = System.currentTimeMillis() - start;
        summaryResult.typedTriples = summary.multiplicity();
        summaryResult.typedResources = summary.numberOfBuckets();
        if (summary.multiplicity() > target) {
            summaryResult.refinement = true;
            start = System.currentTimeMillis();
            MinHash mHash = new MinHash(dictionary, inputGraph, new MinHashConfiguration(target));
            mHash.update(summary);
            summaryResult.minhashTime = System.currentTimeMillis() - start;
        }
        summaryResult.resources = summary.numberOfBuckets();
        summaryResult.resourcesReduction = (double) inputGraph.numberOfBuckets() / summaryResult.resources;
        summaryResult.triples = summary.multiplicity();
        summaryResult.triplesReduction = (double) inputGraph.multiplicity() / summaryResult.triples;
        System.out.println("printing summary to file");
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputFile.getAbsolutePath() + ".obj"));
        summary.writeExternal(oos);
        oos.close();
    }
    
    protected void queryGraph(File[] linearFiles, File[] starFiles, File[] snowflakeFiles, File[] complexFiles)
            throws Exception {
        Reasoner graphReasoner = new Reasoner(inputGraph);
        System.out.println("Running queries over input graph");
        System.out.println("linear queries");
        for (int i = 0; i < linearFiles.length; i++) {
            TestResult test = linear.get(i);
            System.out.print(linearFiles[i].getName());
            test.actual = runEstimate(graphReasoner, linearFiles[i], false);
            System.out.println("...done");
        }
        
        System.out.println("star queries");
        for (int i = 0; i < starFiles.length; i++) {
            TestResult test = star.get(i);
            System.out.print(starFiles[i].getName());
            test.actual = runEstimate(graphReasoner, starFiles[i], false);
            System.out.println("...done");
            
        }
        
        System.out.println("snowflake queries");
        for (int i = 0; i < snowflakeFiles.length; i++) {
            TestResult test = flake.get(i);
            System.out.print(snowflakeFiles[i].getName());
            test.actual = runEstimate(graphReasoner, snowflakeFiles[i], false);
            System.out.println("...done");
            
        }
        
        System.out.println("complex queries");
        for (int i = 0; i < complexFiles.length; i++) {
            TestResult test = complex.get(i);
            System.out.print(complexFiles[i].getName());
            test.actual = runEstimate(graphReasoner, complexFiles[i], false);
            System.out.println("...done");
        }
    }
    
    protected void querySummary(File[] linearFiles, File[] starFiles, File[] snowflakeFiles, File[] complexFiles)
            throws Exception {
        Reasoner summaryReasoner = new Reasoner(summary);
        System.out.println("Running queries over summary");
        System.out.println("linear queries");
        for (int i = 0; i < linearFiles.length; i++) {
            TestResult test = linear.get(i);
            System.out.print(linearFiles[i].getName());
            test.estimate = runEstimate(summaryReasoner, linearFiles[i], true);
            test.approx = runApproximate(summaryReasoner, linearFiles[i]);
            test.std_dev = standardDeviation(linearFiles[i], test.estimate.result);
            test.computeErrors();
            System.out.println("...done");
        }
        
        System.out.println("star queries");
        for (int i = 0; i < starFiles.length; i++) {
            TestResult test = star.get(i);
            System.out.print(starFiles[i].getName());
            test.estimate = runEstimate(summaryReasoner, starFiles[i], true);
            System.out.print(" estimate");
            
            test.approx = runApproximate(summaryReasoner, starFiles[i]);
            System.out.print(" approx");
            
            
            test.std_dev = standardDeviation(starFiles[i], test.estimate.result);
            System.out.print(" deviation");
            
            test.computeErrors();
            System.out.println("...done");
            
        }
        
        System.out.println("snowflake queries");
        for (int i = 0; i < snowflakeFiles.length; i++) {
            TestResult test = flake.get(i);
            System.out.print(snowflakeFiles[i].getName());
            test.estimate = runEstimate(summaryReasoner, snowflakeFiles[i], true);
            System.out.print(" estimate");
            
            test.approx = runApproximate(summaryReasoner, snowflakeFiles[i]);
            System.out.print(" approx");
            
            long start = System.currentTimeMillis();
            test.std_dev = standardDeviation(snowflakeFiles[i], test.estimate.result);
            long end = System.currentTimeMillis() - start;
            System.out.print(" deviation in " + (end / 1000.0) + " secs");
            test.computeErrors();
            System.out.println("...done");
            
        }
        
        System.out.println("complex queries");
        for (int i = 0; i < complexFiles.length; i++) {
            TestResult test = complex.get(i);
            System.out.print(complexFiles[i].getName());
            
            test.estimate = runEstimate(summaryReasoner, complexFiles[i], true);
            System.out.print(" estimate");
            
            test.approx = runApproximate(summaryReasoner, complexFiles[i]);
            System.out.print(" approx");
            
            test.std_dev = standardDeviation(complexFiles[i], test.estimate.result);
            System.out.print(" deviation");
            
            test.computeErrors();
            System.out.println("...done");
            
        }
    }
    
    private void init(File[] linearQueries, File[] starQueries, File[] snowflakeQueries, File[] complexQueries) {
        for (int i = 0; i < linearQueries.length; i++) {
            linear.add(new TestResult(linearQueries[i].getName()));
        }
        for (int i = 0; i < starQueries.length; i++) {
            star.add(new TestResult(starQueries[i].getName()));
        }
        
        for (int i = 0; i < snowflakeQueries.length; i++) {
            flake.add(new TestResult(snowflakeQueries[i].getName()));
        }
        
        for (int i = 0; i < complexQueries.length; i++) {
            complex.add(new TestResult(complexQueries[i].getName()));
        }
        
    }
    
    private QueryResult standardDeviation(final File queryFile, final double expectation) throws IOException {
        SPARQLQuery query = new SPARQLQuery(queryFile, dictionary, prefixes);
        SPARQLQuery doubleQuery = SPARQLQuery.standardDeviationQuery(query);
        if (doubleQuery.countAtoms() != 2 * query.countAtoms())
            throw new RuntimeException("aaaa");
        QueryResult result = new QueryResult();
        long totalTime = System.currentTimeMillis() + TIMEOUT;
        long start = System.nanoTime();
        try {
            double doubleexp = new TimedSPARQLEvaluator(summary, doubleQuery, totalTime).evaluate();
            System.out.println("exp = " + expectation + " double " + doubleexp);
            result.ntime = toMicroSeconds(System.nanoTime() - start);
            result.result = Math.sqrt(doubleexp - (expectation * expectation));
        } catch (TimeOutException e) {
            result.result = -1;
            result.ntime = System.currentTimeMillis() - start;
        }
        return result;
    }
    
    private QueryResult runEstimate(Reasoner reasoner, File queryFile, boolean extendedTest) throws Exception {
        QueryResult result = new QueryResult();
        SPARQLQuery query = new SPARQLQuery(queryFile, dictionary, prefixes);
        result.result = reasoner.answer(query);
        if (extendedTest) {
            UnificationFreeTD tdQuery = getDecomposition(queryFile);
            result.ntime = normalTime(reasoner, query);
            if (tdQuery != null)
                result.dtime = decompositionTime(reasoner, tdQuery);
        }
        return result;
    }
    
    private QueryResult runApproximate(Reasoner reasoner, File queryFile) throws Exception {
        QueryResult result = new QueryResult();
        SPARQLQuery query = new SPARQLQuery(queryFile, dictionary, prefixes);
        result.result = reasoner.approximate(query);
        result.ntime = approximateTime(reasoner, query);
        return result;
    }
    
    private UnificationFreeTD getDecomposition(File queryFile) throws Exception {
        File td = new File(FilenameUtils.removeExtension(queryFile.getAbsolutePath()) + ".xml");
        if (td.exists()) {
            UnificationFreeTD tdQuery = new UnificationFreeTD(td.getAbsolutePath(), dictionary, prefixes);
            return tdQuery;
        }
        return null;
    }
    
    private double toMicroSeconds(double nanoseconds) {
        return nanoseconds / 1000;
    }
    
    private double normalTime(Reasoner reasoner, SPARQLQuery query) throws Exception {
        for (int i = 0; i < N; i++) {
            reasoner.answer(query);
        }
        long start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            reasoner.answer(query);
        }
        double totalTime = System.nanoTime() - start;
        return toMicroSeconds(totalTime / N);
    }
    
    private double approximateTime(Reasoner reasoner, SPARQLQuery query) throws Exception {
        for (int i = 0; i < N; i++) {
            reasoner.approximate(query);
        }
        long start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            reasoner.approximate(query);
        }
        double totalTime = System.nanoTime() - start;
        return toMicroSeconds(totalTime / N);
    }
    
    private double decompositionTime(Reasoner reasoner, UnificationFreeTD query) throws Exception {
        for (int i = 0; i < N; i++) {
            reasoner.answer(query);
        }
        long start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            reasoner.answer(query);
        }
        double totalTime = System.nanoTime() - start;
        return toMicroSeconds(totalTime / N);
    }
    
    private File[] linearQueries() {
        File queryFolder = new File(inputFolder.getAbsolutePath() + "/queries/linear/");
        File[] linearQueries = sortedQueries(queryFolder);
        return linearQueries;
    }
    
    private File[] starQueries() {
        File queryFolder = new File(inputFolder.getAbsolutePath() + "/queries/star/");
        File[] linearQueries = sortedQueries(queryFolder);
        return linearQueries;
    }
    
    private File[] flakeQueries() {
        File queryFolder = new File(inputFolder.getAbsolutePath() + "/queries/snowflake/");
        File[] linearQueries = sortedQueries(queryFolder);
        return linearQueries;
    }
    
    private File[] complexQueries() {
        File queryFolder = new File(inputFolder.getAbsolutePath() + "/queries/complex/");
        File[] linearQueries = sortedQueries(queryFolder);
        return linearQueries;
    }
    
    protected File[] sortedQueries(File queryFolder) {
        File[] queries = queryFolder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".sparql");
            }
        });
        Arrays.sort(queries);
        return queries;
    }
    
    private void readGraph() throws RDF4JException, FileNotFoundException, IOException {
        Object[] result = Importer.readGraph(inputFolder.getAbsolutePath() + "/facts/" + graphName, "krr-onto");
        dictionary = (Dictionary) result[0];
        inputGraph = (Summary) result[1];
    }
    
    private void print(PrintStream out) {
        out.println("GRAPH STATS");
        out.println("\t name: " + graphName);
        out.println("\t resources: " + inputGraph.numberOfBuckets());
        out.println("\t triples: " + inputGraph.multiplicity());
        
        summaryResult.print(out);
        
        out.println("\nQUERY RESULTS (all times in microseconds)");
        
        out.println("\n LINEAR QUERIES\n");
        print(out, linear);
        out.flush();
        
        out.println("\n STAR QUERIES\n");
        print(out, star);
        out.flush();
        
        out.println("\n SNOWFLAKE QUERIES\n");
        print(out, flake);
        out.flush();
        
        out.println("\n COMPLEX QUERIES\n");
        print(out, complex);
        out.flush();
    }
    
    private static void print(PrintStream out, List<TestResult> results) {
        printHeader(out);
        for (TestResult result : results) {
            out.printf("%20s, ", result.id);
            out.printf("%20.1f, ", result.actual.result);
            out.printf("%20.1f, ", result.estimate.result);
            out.printf("%20.3f, ", result.estimate.ntime);
            out.printf("%20.3f, ", result.estimate.dtime);
            out.printf("%20.1f, ", result.est_qerror);
            out.printf("%20.1f, ", result.approx.result);
            out.printf("%20.3f, ", result.approx.ntime);
            out.printf("%20.1f, ", result.apx_qerror);
            out.printf("%20.1f, ", result.std_dev.result);
            out.printf("%20.1f, ", result.std_dev.ntime);
            
            out.println();
        }
    }
    
    private static void printHeader(PrintStream out) {
        out.printf("%20s, %20s, %20s, %20s, %20s, %20s, %20s, %20s, %20s, %20s, %20s\n", "query", "act_res", "est_res", "est_ntime", "est_dtime", "est_qerror", "apx_res", "apx_t", "apx_qerror",
                "std_dev", "std_dev_time");
    }
    
    static class SummaryResult {
        
        long typed_time;
        boolean refinement;
        long minhashTime;
        
        int triples;
        double triplesReduction;
        
        int resources;
        double resourcesReduction;
        
        int typedTriples;
        int typedResources;
        
        public void print(PrintStream out) {
            out.println("\nSUMMARY STATS");
            out.println("\t resources: " + resources + ". Reduction factor: " + resourcesReduction);
            out.println("\t triples: " + triples + ". Reduction factor: " + triplesReduction);
            out.println("\t typed resources: " + typedResources);
            out.println("\t typed triples: " + typedTriples);
            out.println("\t typed time (ms): " + typed_time);
            out.println("\t minhash time (ms): " + minhashTime);
            out.println("\t total time (ms): " + (typed_time + minhashTime));
            
        }
    }
    
    static class TestResult {
        
        String id;
        QueryResult actual;
        QueryResult estimate;
        QueryResult approx;
        
        double est_qerror;
        double apx_qerror;
        
        QueryResult std_dev;
        
        public TestResult(String qName) {
            id = qName;
        }
        
        void computeErrors() {
            double act = Math.max(1, actual.result);
            double est = Math.max(1, estimate.result);
            double apx = Math.max(1, approx.result);
            est_qerror = Math.max(act / est, est / act);
            apx_qerror = Math.max(act / apx, apx / act);
        }
    }
    
    static class QueryResult {
        double result;
        double ntime;
        double dtime;
        
        public QueryResult() {
            ntime = -1;
            dtime = -1;
            result = -1;
        }
    }
    
    public static void main(String[] args) throws Exception {
        Options options = constructOptions();
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String benchmark = cmd.getOptionValue("benchmark");
            String output = cmd.getOptionValue("output");
            String data = cmd.getOptionValue("data");
            String importSummary = cmd.getOptionValue("import");
            int target = Integer.parseInt(cmd.getOptionValue("target"));
            Evaluator eval = new Evaluator(benchmark, output, data, importSummary, target);
            eval.run();
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Run experiments\n\n";
            formatter.printHelp("Experiment", header, options, null, true);
        }
        
    }
    
    protected static Options constructOptions() {
        Options options = new Options();
        
        Option benchmark = new Option("b", "benchmark", true, "benchmark directory");
        benchmark.setRequired(true);
        options.addOption(benchmark);
        
        Option input = new Option("d", "data", true, "data file");
        input.setRequired(true);
        options.addOption(input);
        
        Option output = new Option("o", "output", true, "output file");
        output.setRequired(true);
        options.addOption(output);
        
        Option target = new Option("t", "target", true, "target summary size");
        target.setRequired(true);
        options.addOption(target);
        
        Option importSummary = new Option("i", "import", true, "import summary");
        importSummary.setRequired(false);
        options.addOption(importSummary);
        return options;
    }
}
