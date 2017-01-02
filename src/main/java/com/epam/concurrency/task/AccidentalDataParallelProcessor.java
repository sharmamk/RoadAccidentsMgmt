package com.epam.concurrency.task;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.data.RoadAccident;

public class AccidentalDataParallelProcessor {

	private Logger log = LoggerFactory.getLogger(AccidentalDataParallelProcessor.class);

	private volatile boolean running = true;

	public void processFile(AccidentDataReader accidentDataReader, AccidentDataEnricher accidentDataEnricher,
			AccidentDataWriter accidentDataWriter) throws InterruptedException {

		BlockingQueue<List<RoadAccident>> roadAccidentQueue = new ArrayBlockingQueue<>(20);
		BlockingQueue<List<RoadAccidentDetails>> roadAccidentDetailsQueue = new ArrayBlockingQueue<>(20);
		ExecutorService executor = Executors.newFixedThreadPool(20);

		Callable<List<RoadAccident>> reader = () -> {
			int batchCount = 1;
			List<RoadAccident> roadAccidents = null;
			while (!accidentDataReader.hasFinished()) {
				try {
					roadAccidents = accidentDataReader.getNextBatch();
					roadAccidentQueue.put(roadAccidents);
					log.info("Read [{}] records in batch [{}]", roadAccidents.size(), batchCount++);
				} catch (InterruptedException e) {
					running = false;
					throw e;
				}
			}
			return roadAccidents;
		};

		FutureTask<List<RoadAccident>> readerTaskFuture = new FutureTask<>(reader);
		executor.submit(readerTaskFuture);

		Callable<List<RoadAccidentDetails>> enrichCallable = () -> {
			List<RoadAccidentDetails> roadAccidentDetailsList = null;
			try {
				while (running) {
					roadAccidentDetailsList = accidentDataEnricher.enrichRoadAccidentData(roadAccidentQueue.take());
					roadAccidentDetailsQueue.put(roadAccidentDetailsList);
					log.info("Enriched records");
				}
			} catch (InterruptedException e) {
				running = false;
				throw e;
			}
			return roadAccidentDetailsList;

		};

		FutureTask<List<RoadAccidentDetails>> enrichTaskFuture = new FutureTask<>(enrichCallable);
		executor.submit(enrichTaskFuture);

		Callable<RoadAccidentDetails> writerCallable = () -> {
			try {
				while (running) {
					accidentDataWriter.writeAccidentData(roadAccidentDetailsQueue.take());
					log.info("Written records");
				}
			} catch (InterruptedException e) {
				running = false;
				throw e;
			}
			return null;
		};

		FutureTask<RoadAccidentDetails> writerTaskFuture = new FutureTask<>(writerCallable);
		executor.submit(writerTaskFuture);

		try {
			if ((readerTaskFuture.get() != null && enrichTaskFuture != null && writerTaskFuture != null) || !running) {
				readerTaskFuture.cancel(true);
				enrichTaskFuture.cancel(true);
				writerTaskFuture.cancel(true);
				executor.shutdownNow();
			}
		} catch (ExecutionException e) {
			e.printStackTrace();

		}
	}

}