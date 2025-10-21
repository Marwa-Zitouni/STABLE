package csparql.ind;

import java.io.File;
import org.apache.log4j.PropertyConfigurator;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.larkc.csparql.common.utils.CsparqlUtils;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import csparql.ind.streamer.SensorsStreamer;

public class App {

	private static Logger logger = LoggerFactory.getLogger(App.class);


	public static void main(String[] args) {

		try{
			
			//Configure log4j logger for the csparql engine
			PropertyConfigurator.configure("log4j_configuration/csparql_readyToGoPack_log4j.properties");


			//Create csparql engine instance
			CsparqlEngineImpl engine = new CsparqlEngineImpl();
			//Initialize the engine instance
			//The initialization creates the static engine (SPARQL) and the stream engine (CEP)
			engine.initialize(true);

			String fileOntology = "thermal_final.owl";
			String excelFilePath = "all_hotspots_summary.xlsx"; 

			// put static model
			engine.putStaticNamedModel("http://example.org/thermal.owl",CsparqlUtils.serializeRDFFile(fileOntology));
			

			String queryS6 = "REGISTER QUERY S6detection AS "
			+ "PREFIX : <http://example.org/thermal.owl#> "
			+ "PREFIX sosa: <http://www.w3.org/ns/sosa/> "
			+ "SELECT ?seg ?v1 "
			+ "FROM STREAM <Stream_ThermalSensor_1> [RANGE 10s STEP 10s] "
			+ "FROM <http://example.org/thermal.owl> "
			+ "WHERE { "
			+ "  { ?seg :hasSize1 ?v1 . "
			+ "    ?seg :hasTemperature1 ?v2 . "
			+ "    FILTER ( ?v1 > 200.0 && ?v2 > 33.0 ) "
			+ "  } "
			+ "} ";

			String queryS7 = "REGISTER QUERY S7detection AS "
			+ "PREFIX : <http://example.org/thermal.owl#> "
			+ "PREFIX sosa: <http://www.w3.org/ns/sosa/> "
			+ "SELECT ?seg ?v1 "
			+ "FROM STREAM <Stream_ThermalSensor_1> [RANGE 10s STEP 10s] "
			+ "FROM <http://example.org/thermal.owl> "
			+ "WHERE { "
			+ "  ?seg :isLocatedIn :battery_right ; "
			+ "        :hasTemperature1 ?v1 . "
			+ "  FILTER (?v1 > 34) "
			+ "}";
		
			String queryS8 = "REGISTER QUERY S8detection AS "
			+ "PREFIX : <http://example.org/thermal.owl#> "
			+ "PREFIX sosa: <http://www.w3.org/ns/sosa/> "
			+ "SELECT ?img ?seg ?temp "
			+ "FROM STREAM <Stream_ThermalSensor_1> [RANGE 10s STEP 10s] "
			+ "FROM <http://example.org/thermal.owl> "
			+ "WHERE { "
			+ "  ?img :hasSegment ?seg . "
			+ "  ?seg :isLocatedIn :battery_center ; "
			+ "        :hasTemperature1 ?temp . "
			+ "  FILTER (?temp > 34.0) "
			+ "}";
		
			String queryS9 = "REGISTER QUERY S9detection AS "
			+ "PREFIX : <http://example.org/thermal.owl#> "
			+ "PREFIX sosa: <http://www.w3.org/ns/sosa/> "
			+ "SELECT ?img (AVG(?tempC) AS ?avgC) (AVG(?tempR) AS ?avgR) "
			+ "FROM STREAM <Stream_ThermalSensor_1> [RANGE 2s STEP 2s] "
			+ "FROM <http://example.org/thermal.owl> "
			+ "WHERE { "
			+ "  ?img :hasSegment ?segC , ?segR . "
			+ "  ?segC :isLocatedIn :battery_center ; "
			+ "        :hasTemperature1 ?tempC . "
			+ "  ?segR :isLocatedIn :battery_right ; "
			+ "        :hasTemperature1 ?tempR . "
			+ "} "
			+ "GROUP BY ?img "
			+ "HAVING ((AVG(?tempC) - AVG(?tempR)) > 0.8)";
				
			
			String queryS10 = "REGISTER QUERY S10detection AS "
			+ "PREFIX : <http://example.org/thermal.owl#> "
			+ "PREFIX sosa: <http://www.w3.org/ns/sosa/> "
			+ "PREFIX f: <http://larkc.eu/csparql/sparql/jena/ext#>"
			+ "SELECT ?img1 ?img2 ?size1 ?seg1 ?seg2 ?size2 ?part "
			+ "FROM STREAM <Stream_ThermalSensor_1> [RANGE 5s STEP 5s] "
			+ "FROM <http://example.org/thermal.owl> "
			+ "WHERE { "
			+ "  ?img1 :hasSegment ?seg1 . "
			+ "  ?seg1 :hasSize1 ?size1 ; "
			+ "         :isLocatedIn ?part ; "
			+ "         :hasXCoordinate ?x1 ; "
			+ "         :hasYCoordinate ?y1 . "
			+ "  ?img2 :hasSegment ?seg2 . "
			+ "  ?seg2 :hasSize1 ?size2 ; "
			+ "         :isLocatedIn ?part ; "
			+ "         :hasXCoordinate ?x2 ; "
			+ "         :hasYCoordinate ?y2 . "
			+ "  FILTER(f:timestamp(?img1, :hasSegment , ?seg1 ) < f:timestamp(?img2, :hasSegment, ?seg2) "
			+ "&& ?img1 != ?img2 && ?size2 > ?size1 + 20 "
			+ "         && (?x1 - ?x2 <= 5 && ?x2 - ?x1 <= 5) "
			+ "         && (?y1 - ?y2 <= 5 && ?y2 - ?y1 <= 5)) "
			+ "}";
			
			String queryS12 = "REGISTER QUERY S12detection AS "
				+ "PREFIX : <http://example.org/thermal.owl#> "
				+ "PREFIX f: <http://larkc.eu/csparql/sparql/jena/ext#> "
				+ "SELECT ?img1 ?img2  "
				+ "       (AVG(?tempC1) AS ?avgC1) "
				+ "       (AVG(?tempR1) AS ?avgR1) "
				+ "       (AVG(?tempR2) AS ?avgR2) "
				+ "       (AVG(?tempL1) AS ?avgL1) "
				+ "       (AVG(?tempL2) AS ?avgL2) "
				+ "FROM STREAM <Stream_ThermalSensor_1> [RANGE 6s STEP 6s] "
				+ "FROM <http://example.org/thermal.owl> "
				+ "WHERE { "
				+ "  ?img1 :hasSegment ?segC1 , ?segR1 , ?segL1 . "
				+ "         ?segC1 :isLocatedIn :battery_center ; "
				+ "                :hasTemperature1 ?tempC1 . "
				+ "         ?segR1 :isLocatedIn :battery_right ; "
				+ "                :hasTemperature1 ?tempR1 . "
				+ "         ?segL1 :isLocatedIn :battery_left ; "
				+ "                :hasTemperature1 ?tempL1 . "

				+ "  ?img2 :hasSegment ?segR2 , ?segL2 . "
				+ "         ?segR2 :isLocatedIn :battery_right ; "
				+ "                :hasTemperature1 ?tempR2 . "
				+ "         ?segL2 :isLocatedIn :battery_left ; "
				+ "                :hasTemperature1 ?tempL2 . "
			
				+ "  FILTER ( "
				+ "    ?img1 != ?img2 && "
				+ "    f:timestamp(?img1, :hasSegment, ?segR1) < f:timestamp(?img2, :hasSegment, ?segR2) "
				+ "  ) "
				+ "} "
				+ "GROUP BY ?img1 ?img2 "
				+ "HAVING ( "
				+ "  (?avgC1 > ?avgR1 && (?avgR2 - ?avgR1) > 0.05) || "
				+ "  (?avgC1 > ?avgL1 && (?avgL2 - ?avgL1) > 0.05) "
				+ ")";


			OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
			OWLDataFactory factory = manager.getOWLDataFactory();
			String ontologyURI = "http://example.org/thermal.owl";
			String ns = ontologyURI + "#";
			final OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new File(fileOntology));
			
			SensorsStreamer Stream_ThermalSensor_1 = new SensorsStreamer("Stream_ThermalSensor_1",ns,"ThermalSensor",2,excelFilePath,ontology,factory);


			//Register new streams in the engine
			engine.registerStream(Stream_ThermalSensor_1);



			Thread Stream_ThermalSensor_Thread_1 = new Thread(Stream_ThermalSensor_1);


			//Register new query in the engine
			// CsparqlQueryResultProxy c_S6 = engine.registerQuery(queryS6, false);
			// CsparqlQueryResultProxy c_S7 = engine.registerQuery(queryS7, false);
			// CsparqlQueryResultProxy c_S8 = engine.registerQuery(queryS8, false);
			// CsparqlQueryResultProxy c_S9 = engine.registerQuery(queryS9, false);
			// CsparqlQueryResultProxy c_S10 = engine.registerQuery(queryS10, false);
			// CsparqlQueryResultProxy c_S11 = engine.registerQuery(queryS11, false);
			CsparqlQueryResultProxy c_S12 = engine.registerQuery(queryS12, false);

			//Attach a result consumer to the query result proxy to print the results on the console
			// c_S6.addObserver(new ConsoleFormatter("S6",ns,ontology,factory));	
			// c_S7.addObserver(new ConsoleFormatter("S7",ns,ontology,factory));	
			// c_S8.addObserver(new ConsoleFormatter("S8",ns,ontology,factory));	
			// c_S9.addObserver(new ConsoleFormatter("S9",ns,ontology,factory));	
			// c_S10.addObserver(new ConsoleFormatter("S10",ns,ontology,factory));	
			// c_S11.addObserver(new ConsoleFormatter("S11",ns,ontology,factory));	
			c_S12.addObserver(new ConsoleFormatter("S12",ns,ontology,factory));	


			//Start streaming data
			Stream_ThermalSensor_Thread_1.start();



		}catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

	}

}