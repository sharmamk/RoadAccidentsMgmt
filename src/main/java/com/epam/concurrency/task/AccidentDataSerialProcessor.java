package com.epam.concurrency.task;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.data.RoadAccident;

public class AccidentDataSerialProcessor{
	
	private Logger log = LoggerFactory.getLogger(AccidentDataSerialProcessor.class);
	
	public void processFile(AccidentDataReader accidentDataReader, AccidentDataEnricher accidentDataEnricher,
			AccidentDataWriter accidentDataWriter){
		
		log.info("Processing files serially");
		
        int batchCount = 1;
        while (!accidentDataReader.hasFinished()){
            List<RoadAccident> roadAccidents = accidentDataReader.getNextBatch();
            log.info("Read [{}] records in batch [{}]", roadAccidents.size(), batchCount++);
            List<RoadAccidentDetails> roadAccidentDetailsList = accidentDataEnricher.enrichRoadAccidentData(roadAccidents);
            log.info("Enriched records");
            accidentDataWriter.writeAccidentData(roadAccidentDetailsList);
            log.info("Written records");
        }
    }
}