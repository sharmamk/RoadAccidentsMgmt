package com.epam.concurrency.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.data.RoadAccident;

/**
 * Created by Tanmoy on 6/17/2016.
 */
public class AccidentDataProcessor {

    private static final String FILE_PATH_1 = "src/main/resources/DfTRoadSafety_Accidents_2010.csv";
    private static final String FILE_PATH_2 = "src/main/resources/DfTRoadSafety_Accidents_2011.csv";
    private static final String FILE_PATH_3 = "src/main/resources/DfTRoadSafety_Accidents_2012.csv";
    private static final String FILE_PATH_4 = "src/main/resources/DfTRoadSafety_Accidents_2013.csv";

    private static final String OUTPUT_FILE_PATH = "target/DfTRoadSafety_Accidents_consolidated.csv";

    private static final int DATA_PROCESSING_BATCH_SIZE = 10000;

    private AccidentDataReader accidentDataReader = new AccidentDataReader();
    private AccidentDataEnricher accidentDataEnricher = new AccidentDataEnricher();
    private AccidentDataWriter accidentDataWriter = new AccidentDataWriter();

    private List<String> fileQueue = new ArrayList<String>();

    private Logger log = LoggerFactory.getLogger(AccidentDataProcessor.class);


    public void init(){
        fileQueue.add(FILE_PATH_1);
        //fileQueue.add(FILE_PATH_2);
        //fileQueue.add(FILE_PATH_3);
        //fileQueue.add(FILE_PATH_4);

        accidentDataWriter.init(OUTPUT_FILE_PATH);
    }

    public void process() throws InterruptedException, ExecutionException{
        for (String accidentDataFile : fileQueue){
            log.info("Starting to process {} file ", accidentDataFile);
            accidentDataReader.init(DATA_PROCESSING_BATCH_SIZE, accidentDataFile);
            processFile();
        }
    }

    private void processFile() throws InterruptedException, ExecutionException{
        int batchCount = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        while (!accidentDataReader.hasFinished()){
        	Future<List<RoadAccident>> roadAccidentsFuture = executorService.submit(new Runnable() {				
				@Override
				public void run() {
					// TODO Auto-generated method stub
					
				}
			}, accidentDataReader.getNextBatch());
        	List<RoadAccident> roadAccidents = roadAccidentsFuture.get();
        	log.info("Read [{}] records in batch [{}]", roadAccidents.size(), batchCount++);
        	
        	Future<List<RoadAccidentDetails>> enricherFuture = executorService.submit(new Callable<List<RoadAccidentDetails>>() {
				@Override
				public List<RoadAccidentDetails> call() throws Exception {
					return accidentDataEnricher.enrichRoadAccidentData(roadAccidents);
				}
        		
			});
        	log.info("Enriched records");
        	List<RoadAccidentDetails> roadAccidentDetailsList = enricherFuture.get();
        	
        	executorService.submit(new Runnable() {
				@Override
				public void run() {
					 accidentDataWriter.writeAccidentData(roadAccidentDetailsList);
				}
			});
        	log.info("Written records");
        	
        }
        
        executorService.shutdown();
    }


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        AccidentDataProcessor dataProcessor = new AccidentDataProcessor();
        
        long start = System.currentTimeMillis();
        
        dataProcessor.init();
        dataProcessor.process();
        long end = System.currentTimeMillis();
        System.out.println("Process finished in s : " + (end-start)/1000);
    }

}
