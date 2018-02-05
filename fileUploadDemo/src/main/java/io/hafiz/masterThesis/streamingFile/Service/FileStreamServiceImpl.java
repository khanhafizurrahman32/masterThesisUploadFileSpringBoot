package io.hafiz.masterThesis.streamingFile.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Service;

import io.hafiz.masterThesis.fileManipulation.OrderedPair;
import io.hafiz.masterThesis.fileManipulation.sampleFileToHandle;

@Service
public class FileStreamServiceImpl implements FileStreamingServiceInterface {

	@Override
	public void startProcessingFileToSubmitIntoTopic(String filePath) {
		//List<sampleFileToHandle> inputList = new ArrayList<sampleFileToHandle>();
		List<String[]> inputList = new ArrayList<String[]>();
		List<String> headerNames = new ArrayList<String>();
		String [] headerArray;
		
		try {
			File inputF = new File(filePath);
			InputStream inputFS = new FileInputStream(inputF);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
			
			//headerNames = Stream.of(br.readLine()).map(line -> line.split(","))
			//		.flatMap(Arrays:: stream).collect(Collectors.toList());
			
			headerArray = br.readLine().split(",");
			inputList = br.lines().skip(0).map(line -> line.split(",")).collect(Collectors.toList());
			
			br.close();
			HashMap<String,String> headerNSampleTuple = mappingHeaderValuePair(headerArray,inputList);
			submitToKafkaTopic(headerNSampleTuple);
			//receivingContentsFromTopic();
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	

	private HashMap<String,String> mappingHeaderValuePair(String[] headerArray, List<String[]> inputList) {
	
		HashMap<String,String> headerRowValueTuple = new HashMap<String,String>();
		for (String[] currentRow : inputList) {
			for(int headerCount = 0; headerCount < headerArray.length; headerCount++) {
				System.out.println(currentRow[headerCount]);
				headerRowValueTuple.put(headerArray[headerCount],currentRow[headerCount]);
			}	
		}
		return headerRowValueTuple;
	}

	

	private void receivingContentsFromTopic() {
		Properties props = new Properties();
	    	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
	    	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	    	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	    	
	    	final StreamsBuilder builder = new StreamsBuilder();
	    	final KStream<Object, Object> checkStream = builder.stream("sampleFileTopic");
	    	builder.stream("sampleFileTopic").to("sampleFileTopicOutput");
	    	
	    	final Topology topology = builder.build();
	    	final KafkaStreams streams = new KafkaStreams(topology, props);
	    	final CountDownLatch latch = new CountDownLatch(1);
        	
	    	Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
           @Override
           public void run() {
               streams.close();
               latch.countDown();
           }
	    	});
    	   
	   try {
           streams.start();
           latch.await();
       } catch (Throwable e) {
           System.exit(1);
       }
    	   
	   System.exit(0);
        	
	}

	private void submitToKafkaTopic(HashMap<String,String> headerNSampleTuple) {
		String topicName = "sampleFileTopic";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
//		Producer<String,sampleFileToHandle> producer = new KafkaProducer<String,sampleFileToHandle>(props);
		Producer<String, String> producer = new KafkaProducer<String,String>(props);
		for(Map.Entry<String, String> m: headerNSampleTuple.entrySet()) {
			producer.send(new ProducerRecord<String, String>(topicName,(m.getKey() +":::"+m.getValue())));
		}
		System.out.println("Message sent successfully");
		producer.close();
		
	}
	
	private Function<String, sampleFileToHandle> mapToItem = (line)->{
		String [] afterSplitting = line.split(",");
		sampleFileToHandle s = new sampleFileToHandle(Integer.parseInt(afterSplitting[0]),afterSplitting[1],afterSplitting[2],afterSplitting[3]);
		return s;
	};

}
