package io.hafiz.masterThesis.streamingFile.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import io.hafiz.masterThesis.fileManipulation.sampleFileToHandle;

@Service
public class FileStreamServiceImpl implements FileStreamingServiceInterface {

	@Override
	public void startProcessingFileToSubmitIntoTopic(String filePath) {
		List<sampleFileToHandle> inputList = new ArrayList<sampleFileToHandle>();
		List<String> headerNames = new ArrayList<String>();
		try {
			File inputF = new File(filePath);
			InputStream inputFS = new FileInputStream(inputF);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
			
			headerNames = Stream.of(br.readLine()).map(line -> line.split(","))
					.flatMap(Arrays:: stream).collect(Collectors.toList());
			
			inputList = br.lines().skip(0).map(mapToItem).collect(Collectors.toList());
			submitToKafkaTopic(inputList);
			br.close();
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	private void submitToKafkaTopic(List<sampleFileToHandle> inputList) {
		String topicName = "sampleFileTopic";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "io.hafiz.masterThesis.fileManipulation.SampleFileToHandleSerializer");
		
		Producer<String,sampleFileToHandle> producer = new KafkaProducer<String,sampleFileToHandle>(props);
		for(sampleFileToHandle sampleFile: inputList) {
			producer.send(new ProducerRecord<String, sampleFileToHandle>(topicName, sampleFile.getFirstName(), sampleFile));
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
