package io.hafiz.masterThesis.streamingFile.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
		System.out.println("line 51");
		List<String[]> inputList = new ArrayList<String[]>();
		List<String> headerNames = new ArrayList<String>();
		String [] headerArray;
		
		try {
			File inputF = new File(filePath);
			InputStream inputFS = new FileInputStream(inputF);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
			
			headerArray = br.readLine().split(",");
			inputList = br.lines().skip(0).map(line -> line.split(",")).collect(Collectors.toList());
			
			br.close();
			List<String> headerNSampleTuple = mappingHeaderValuePair(headerArray,inputList);
			submitToKafkaTopic(headerNSampleTuple);
			receivingContentsFromTopic();
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	private List<String> mappingHeaderValuePair(String[] headerArray, List<String[]> inputList) {
		
		List<String> mappedValuePair = new ArrayList<String>();
		for (String[] currentRow : inputList) {
			for(int headerCount = 0; headerCount < headerArray.length; headerCount++) {
				mappedValuePair.add(headerArray[headerCount] +" , " + currentRow[headerCount]);
			}	
		}
		return mappedValuePair;
	}

	

	private void receivingContentsFromTopic() {
		final Consumer<String, String> consumer = createConsumer();
		while(true) {
			ConsumerRecords<String,String> records = consumer.poll(100);
			System.out.println(records.count());
//			Iterator<ConsumerRecord<String,String>> iterable = records.iterator();
//			System.out.println(iterable.hasNext());
//			while(iterable.hasNext()) {
//				ConsumerRecord<String,String> currentRecord = iterable.next();
//				System.out.println("Key is "+ currentRecord.key()+ " value is "+ currentRecord.value() + " offset is "+ currentRecord.offset()+ "partition is " + currentRecord.partition());
//			}
		}
	}

	private static Consumer<String, String> createConsumer() {
		// TODO Auto-generated method stub
		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("zookeeper.connect", "localhost:2181");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "masterThesisGroup");
		
		final Consumer<String, String> consumer = new KafkaConsumer<> (props); 
		consumer.subscribe(Collections.singletonList("sampleFileTopic"));
		return consumer;	
		
	}

	private void submitToKafkaTopic(List<String> headerNSampleTuple) {
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
		
		Producer<String, String> producer = new KafkaProducer<String,String>(props);
		for(String s: headerNSampleTuple) {
			producer.send(new ProducerRecord<String, String>(topicName,s));
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
