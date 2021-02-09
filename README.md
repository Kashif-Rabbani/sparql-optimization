## Optimizing SPARQL Queries using Shape Statistics

Cardinality estimates are essential for finding a good join order to improve query performance. In order to access the impact of having shapes statistics of RDF graphs on cardinality estimation, we have performed these experiments. We have generated global and shapes statistics and proposed a join ordering technique to make use of these statistics and estimate cardinalities to propose efficient query plans. We used synthetic (LUBM, WATDIV) and a real dataset (i.e., YAGO-4). We compared against the query plans proposed by Jena ARQ query engine, GraphDB, Characteristics Sets, and SumRDF approach. At this page we present technical details of our experiments such as how to generate these statistics, how to run the experiments, the links to the datasets, and finally the results.


### Persistent URI & Licence:
All of the data and results presented in our experimental study are available at
[https://github.com/kworkr/repo/](https://github.com/kworkr/repo/) under [Apache License 2.0](https://github.com/kworkr/repo/blob/master/LICENSE) .


### Datasets, Queries and the Statistics used:
We used the following datasets, queries, and the statistics: 

Dataset | RDF Dump | Queries | Stats
------------ | ------------- | -------------| -------------
[LUBM](http://swat.cse.lehigh.edu/projects/lubm/)|[Download](http://130.226.98.152/datasets/lubm.n3)| [See LUBM Queries](https://github.com/kworkr/repo/tree/master/queries/lubmQueries) | [Global and Shapes Statistics](https://github.com/kworkr/repo/tree/master/globalAndShapesStats/lubmStats)
[YAGO-4](http://swat.cse.lehigh.edu/projects/lubm/)|[Download](http://130.226.98.152/datasets/lubm.n3)| [See YAGO-4 Queries](https://github.com/kworkr/repo/tree/master/queries/yago-4Queries) | [Global and Shapes Statistics](https://github.com/kworkr/repo/tree/master/globalAndShapesStats/yagoStats)
[WATDIV-100M](https://link.springer.com/chapter/10.1007/978-3-319-11964-9_13)|[Download](http://dsg.uwaterloo.ca/watdiv/watdiv.100M.tar.bz2) | [See WATDIV Queries](https://github.com/kworkr/repo/tree/master/queries/watdivQueries)| [Global and Shapes Statistics](https://github.com/kworkr/repo/tree/master/globalAndShapesStats/watdivStats)
[WATDIV-1Billion](https://link.springer.com/chapter/10.1007/978-3-319-11964-9_13)|[Download](https://hobbitdata.informatik.uni-leipzig.de/intelligent-SPARQL-interface/) | [See WATDIV Queries](https://github.com/kworkr/repo/tree/master/queries/watdivQueries)| [Global and Shapes Statistics](https://github.com/kworkr/repo/tree/master/globalAndShapesStats/watdivStats)


### How does it work?

#### 1. Generating SHACL Shapes Graph:
      Given an RDF graph, we used shaclgen https://pypi.org/project/shaclgen/ library to generate its SHACL shapes graph.

#### 2. Generating Shapes Statistics:
      We use Shapes Annotator component to extend SHACL shapes graph with the statistics of the RDF graph. E.g., for YAGO-4 dataset, we use the https://github.com/kworkr/repo/blob/master/code/yagoConfig.properties file by setting the generateStatistics=true.
  
#### 3. Running Experiments:
   We loaded all datasets in Jena TDB, bundled the code in a Jar and created a config file to run each type of experiment. For example we used the following pattern fo run experiments using:
  
  * ###### 1. Shapes Statistics
        > Set the appropriate paths for the Jena TDB and the directory containing queries in the config files, e.g., for YAGO-4 dataset https://github.com/kworkr/repo/blob/master/code/yagoConfig.properties
        > Set the value fo shapeExec=true , set the number of times the query should run.
        > Use java -jar code.jar yagoConfig.properties YAGO  &> output.log
        > Logs will be saved in OUTPUT_QUERY directory as benchmarks.csv and also in output.log file. 
        > Use these logs to plot the results.

  * ###### 2. Global Statistics
        > Follow the same steps as mentioned above for Shapes Statistics, except set the value shapeExec=false and globalStatsExec=true.
        
  * ###### 3. Jena
        > Follow the same steps as mentioned above except set the value shapeExec and globalStatsExec as false and jenaExec=true.
    
  * ###### 4. GraphDB
        > We loaded each dataset in GraphDB and used 'onto:explain' feature explained https://graphdb.ontotext.com/documentation/standard/explain-plan.html to see the plans and their cardinalities. 
        
  * ###### 5. Characteristics Sets
        > We used the extended characteristics sets implementation from https://github.com/gmontoya/federatedOptimizer to generate characteristics Sets for each dataset and then gnerated their query plans.
  
  * ###### 6. SumRDF Cardinality Estimator (official [link](https://www.cs.ox.ac.uk/isg/tools/SumRDF/))
        > We implemented our join ordering algorithm using SumRDF cardinality estimator. The code is available in the folder https://github.com/kworkr/repo/tree/master/sumRDF 

     

### Evaluation Results:

Discussed in the paper and available in folder [results_data](https://github.com/kworkr/repo/tree/master/results_data)
