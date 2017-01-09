package com.epam.processor;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.epam.concurrency.task.AccidentDataEnricher;
import com.epam.concurrency.task.AccidentDataProcessor;
import com.epam.concurrency.task.AccidentDataReader;
import com.epam.concurrency.task.AccidentDataSerialProcessor;
import com.epam.concurrency.task.AccidentDataWriter;
import com.epam.data.RoadAccident;

@RunWith(MockitoJUnitRunner.class)
public class AccidentDataProcessorTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final int DATA_PROCESSING_BATCH_SIZE = 10000;

	private static AccidentDataWriter accidentDataWriterMock = mock(AccidentDataWriter.class);

	private static AccidentDataProcessor accidentDataProcessor;

	private static AccidentDataSerialProcessor accidentDataSerialProcessor;

	private static List<String> fileQueue;

	private static final String FILE_PATH_2 = "src/main/resources/DfTRoadSafety_Accidents_2011.csv";
	private static final String FILE_WITH_LESS_RECORDS = "src/main/resources/DfTRoadSafety_Accidents_Integration_Tests.csv";
	private static final String OUTPUT_FILE = "target/DfTRoadSafety_Accidents_consolidated.csv";

	@BeforeClass
	public static void setUp() {
		fileQueue = new ArrayList<>();
		accidentDataProcessor = new AccidentDataProcessor(new AccidentDataReader(), new AccidentDataEnricher(),
				accidentDataWriterMock);
		accidentDataSerialProcessor = new AccidentDataSerialProcessor();
	}

	@AfterClass
	public static void tearDown() {
		fileQueue = null;
		accidentDataProcessor = null;
		accidentDataWriterMock = null;
	}

	@Test
	public void shouldProcessFileParallelyMockingAccidentDataWriter() throws InterruptedException, ExecutionException {
		doNothing().when(accidentDataWriterMock).writeAccidentData(anyObject());
		doNothing().when(accidentDataWriterMock).init(anyObject());
		fileQueue.add(FILE_PATH_2);
		accidentDataProcessor.process();
//		accidentDataProcessor.process(fileQueue);
		verify(accidentDataWriterMock, Mockito.times(1)).init(anyObject());
	}

	@Test
	public void shouldProcessFileSeriallyMockingAccidentDataWriter() throws InterruptedException {
		doNothing().when(accidentDataWriterMock).writeAccidentData(anyObject());
		doNothing().when(accidentDataWriterMock).init(anyObject());
		fileQueue.add(FILE_PATH_2);
		accidentDataSerialProcessor.processFile(new AccidentDataReader(), new AccidentDataEnricher(),
				accidentDataWriterMock);
		verify(accidentDataWriterMock, Mockito.times(1)).init(anyObject());
	}

	@Test
	public void shouldThrowARuntimeExceptionIfFileNotFound() {
		AccidentDataReader accidentDataReader = new AccidentDataReader();
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Failed to prepare file iterator for  file : file-name");
		accidentDataReader.init(DATA_PROCESSING_BATCH_SIZE, "file-name");
	}

	@Test
	public void shouldReadRowsEqualToBactchSize() {
		AccidentDataReader accidentDataReader = new AccidentDataReader();
		accidentDataReader.init(DATA_PROCESSING_BATCH_SIZE, "src/main/resources/DfTRoadSafety_Accidents_2011.csv");
		List<RoadAccident> roadAccidentList = accidentDataReader.getNextBatch();
		assertThat(roadAccidentList, hasSize(DATA_PROCESSING_BATCH_SIZE));
		assertThat(accidentDataReader.hasFinished(), equalTo(false));
	}

}