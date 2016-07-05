# RDFind

RDFind is a Flink-based distributed algorithm to discover conditional inclusion dependencies (CINDs). Such CINDs are a useful asset for a various RDF data management tasks, ranging from ontology reverse engineering over SPARQL query optimization to knowledge discovery. More details can be found in our [SIGMOD 2016 paper](https://www.researchgate.net/publication/304021433_RDFind_Scalable_Conditional_Inclusion_Dependency_Discovery_in_RDF_Datasets). If you are using RDFind in your research, we would be pleased if you cite us:
```
@inproceedings{kruse2016rdfind,
  author = { Sebastian Kruse, Anja Jentzsch, Thorsten Papenbrock, Zoi Kaoudi, Jorge-Arnulfo Quiane-Ruiz, Felix Naumann },
  title = { RDFind: Scalable Conditional Inclusion Dependency Discovery in RDF Datasets },
  year = { 2016 }
}
```

## Instructions

**Requirements.** RDFind is written in Scala and Java and requires Apache Maven for the build and Apache Flink for the execution.

**Building RDFind.** RDFind can build with Maven but requires some external dependencies to be installed beforehand.

1. Install the RDF converter.
  * `$ git clone https://github.com/sekruse/rdf-converter.git`
  * `$ cd rdf-converter`
  * `$ mvn install`
2. Install the modified Guava library.
  * `$ git clone https://github.com/sekruse/guava.git`
  * `$ cd guava`
  * `$ git checkout -b extended-bloom-filters`
  * `$ mvn install`
3. Install RDFind.
  * `$ git clone https://github.com/stratosphere/rdfind.git`
  * `$ cd rdfind`
  * `$ mvn package`
  
**Running RDFind.** RDFind's main class is `de.hpi.isg.sodap.rdfind.programs.RDFind`. Run it without parameters, to get a list of supported parameters. Your final execution command might look something like this:
```
java -Djava.rmi.server.hostname=<my-ip> \
        -Dlog4j.configuration="file:conf/log4j.properties" \
        -cp "lib/*" \
        de.hpi.isg.sodap.rdfind.programs.RDFind \
        $* \
        -rex <Flink job manager host>:<Flink job manager port> \
        -jar lib/rdfind-algorithm-0.2-SNAPSHOT.jar lib/rdfind-flink-0.2-SNAPSHOT.jar lib/rdfind-util-0.2-SNAPSHOT.jar  lib/fastutil-6.5.15.jar lib/rdf-converter-0.0.2-SNAPSHOT.jar lib/guava-19.0-sekruse-SNAPSHOT.jar
```
  
In case you are facing problems with the installation or execution, feel free to contact sebastian.kruse:e-mail:hpi.de
