# A stream reasoning framework for thermal image-based anomaly detection in lithium-ion batteries
## Overview
This GitHub project presents an approach for anomaly detection in lithium-ion batteries using thermal images and stream reasoning. The framework continuously processes temperature data extracted from thermal image streams, aligns it with an ontology that defines battery modules, image segments, and potential thermal anomalies, and identifies abnormal heat propagation patterns such as localized hotspots or significant temperature imbalances between regions.
## Project Structure
-   **`csparql.ind` package:**
    -   **`App.java`:** The main application class responsible for initializing the C-SPARQL engine, registering streams and queries, and starting the data streaming process.
    -   **`streamer.SensorsStreamer.java`:** A class that simulates sensor data streams from an Excel file (`all_hotspots_summary.xlsx`) and pushes them into the C-SPARQL engine.
-   **`log4j_configuration` directory:**
    -   **`csparql_readyToGoPack_log4j.properties`:** Configuration file for the Log4j logging framework used by the C-SPARQL engine.
-   **`thermal_final.owl`:** The OWL ontology defining concepts related to thermal anomalies in lithium ion batteries.
-   **`all_hotspots_summary.xlsx`:** An Excel file containing temperature data extracted from the images.## Requirements

To run this project, you will need:

-   **Java Development Kit (JDK):** Ensure you have a compatible JDK installed on your system.
-   **Apache Maven:** The project is likely built using Maven. Install Maven if you don't have it.
-   **OWL API:** The project uses the OWL API for handling the ontology. Maven should automatically download this dependency.
-   **C-SPARQL Engine:** The core stream processing engine. Maven should handle this dependency.
-   **Apache Log4j:** For logging. Maven should handle this dependency.
-   **SLF4j:** A simple logging facade for Java. Maven should handle this dependency.
-   **eu.larkc.csparql:** Larkc CSPARQL utilities. Maven should handle this dependency.
-   **Apache POI:** For reading data from the Excel file. Maven should handle this dependency.

