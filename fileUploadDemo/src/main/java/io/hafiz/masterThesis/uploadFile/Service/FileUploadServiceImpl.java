package io.hafiz.masterThesis.uploadFile.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.JsonArray;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Service
public class FileUploadServiceImpl implements FileUploadServiceInterface {

	private static String UPLOADED_FOLDER = "/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/UploadFiles/";
 	private static String kafka_broker_end_point = null;
    private static String csv_input_File = null;
    private static String csv_injest_topic = null;
    
	@Override
	public byte[] getDocumentFile(Long id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResponseMetaData save(MultipartFile multipartFile) throws IOException {
		FileDescription individualFile = new FileDescription();
		individualFile.setFileName(multipartFile.getOriginalFilename());
		individualFile.setFile(multipartFile.getBytes());
		saveUploadedFiles(multipartFile);
		ResponseMetaData metaData = new ResponseMetaData();
		metaData.setMessage("success");
		metaData.setStatus(200);
		return metaData;
	}

	private void saveUploadedFiles(MultipartFile multipartFile) throws IOException{
		byte[] bytes = multipartFile.getBytes();
		Path path = Paths.get(UPLOADED_FOLDER + multipartFile.getOriginalFilename());
		Files.write(path, bytes);
	}

	@Override
	public ArrayList<FileDescription> findAll() {
		File folder = new File ("/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/UploadFiles");
		File[] listOfFiles = folder.listFiles();
		ArrayList<FileDescription>  ListOfFilesInDirectory = new ArrayList <FileDescription> ();
		for (int i= 0; i < listOfFiles.length; i++) {
			System.out.println(listOfFiles[i].getName());
			FileDescription detailsOfFile = new FileDescription((long) i,listOfFiles[i].getName());
			ListOfFilesInDirectory.add(detailsOfFile);
		}
		return ListOfFilesInDirectory;
	}

	@Override
	public List<String> contentsInJson(String inputFilePath) {
		String absolutePath = UPLOADED_FOLDER + inputFilePath;
		List<String> fileContents = new ArrayList<String>(); 
		try {
			Stream<String> csv_data_File_Stream = Files.lines(Paths.get(absolutePath)).skip(1);
			fileContents = csv_data_File_Stream.collect(Collectors.toList());
			fileContents.forEach(System.out::println);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileContents;
	}

	public printOutput getStreamWrapper(InputStream is, String type){
        return  new printOutput(is, type);
    }
	
	@Override
	public void startKafkaTerminalCommandsFromJava(String topicName) {
		// TODO Auto-generated method stub
		System.out.println("start kafka terminal commands from java");
		System.out.println(topicName);
		String command_to_run = "sh /Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/terminal_commands/kafka_start.sh " + topicName;
		Runtime rt = Runtime.getRuntime();
		printOutput outputMessage, errorReported;
		
		try {
			Process proc = rt.exec(command_to_run);
			errorReported = getStreamWrapper(proc.getErrorStream(), "ERROR");
            	outputMessage = getStreamWrapper(proc.getInputStream(), "OUTPUT");
            errorReported.start();
            outputMessage.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
   private Producer<String,String> createKafkaProducer() {
        Properties prop = new Properties();

        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_broker_end_point);
        prop.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "csvDataKafkaProducer");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(prop);
    }
   
   private void publishCSVFileData() {
		 final Producer<String, String> csv_data_producer = createKafkaProducer();
		 final CountDownLatch countDownLatch = new CountDownLatch(1);
         ObjectNode lineNode = JsonNodeFactory.instance.objectNode();
         
         try {
             Stream<String> csv_data_File_Stream = Files.lines(Paths.get(csv_input_File)).skip(1);
             long start = System.currentTimeMillis();
             csv_data_File_Stream.forEach(line -> {
                 String [] parts_by_parts = line.split(",");
                 
                 lineNode.put("sepal_length_in_cm", Float.parseFloat(parts_by_parts [0]));
                 lineNode.put("sepal_width_in_cm", Float.parseFloat(parts_by_parts[1]));
                 lineNode.put("petal_length_in_cm", Float.parseFloat(parts_by_parts[2]));
                 lineNode.put("petal_width_in_cm", Float.parseFloat(parts_by_parts[3]));
                 lineNode.put("class", parts_by_parts[4]);
                 lineNode.put("emni", "dummyClass");
                 final ProducerRecord<String, String> csv_record =
                         new ProducerRecord<String, String>(csv_injest_topic, UUID.randomUUID().toString(), lineNode.toString());

                 try {
                     Thread.currentThread().sleep(0, 1);
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
                 csv_data_producer.send(csv_record, ((metadata, exception) -> {
                     if (metadata != null){
                         System.out.println("Data Sent --> " + csv_record.key() + " | " + csv_record.value() + " | " + metadata.partition());
                     } else {
                         System.out.println("Error Sending Data Event --> " + csv_record.value());
                     }
                 }));
             });

             long end = System. currentTimeMillis();

         } catch (IOException e) {
             e.printStackTrace();
         }
         
	}
	@Override
	public void sendDataToKafkaTopic(Map<String, String> parameters) {
		kafka_broker_end_point = parameters.get("kafka_broker_end_point");
		csv_input_File = parameters.get("csv_input_file");
		csv_input_File = UPLOADED_FOLDER + csv_input_File;
		System.out.println(csv_input_File);		
		csv_injest_topic = parameters.get("topic_name");
		publishCSVFileData();
	}
	
	

	@Override
	public void submitPysparkProjectTerminalCommand(Map<String, String> parameters) {
		String app_name = parameters.get("app_name");
		String master_server = parameters.get("master_server");
		String kafka_bootstrap_server = parameters.get("kafka_bootstrap_server");
		String subscribe_topic = parameters.get("subscribe_topic");
		String command_to_run = "sh /Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/kafkaStreamAnalysis/spark_start.sh "
				                 + app_name + " "
				                 + master_server + " "
				                 + kafka_bootstrap_server + " "
				                 + subscribe_topic ;
		//String command_to_run = "sh /Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/check_pandas3/runfile.sh";
		
		Runtime rt = Runtime.getRuntime();
		printOutput outputMessage, errorReported;
		
		try {
			Process proc = rt.exec(command_to_run);
			errorReported = getStreamWrapper(proc.getErrorStream(), "ERROR");
            	outputMessage = getStreamWrapper(proc.getInputStream(), "OUTPUT");
            errorReported.start();
            outputMessage.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public String getHeadersName(String inputFilePath) {
		String absolutePath = UPLOADED_FOLDER + inputFilePath;	
		List<String> headerNames = new ArrayList<String>();
		String headerNamesString = "";
		try {
			File inputF = new File(absolutePath);
			InputStream inputFS = new FileInputStream(inputF);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
			
			headerNames = Stream.of(br.readLine()).map(line -> line.split(","))
								.flatMap(Arrays:: stream).collect(Collectors.toList());
	
		} catch (Exception e) {
			// TODO: handle exception
		}
		for (String s : headerNames)
		{
			headerNamesString += s + ",";
		}
		return headerNamesString;
	}
	
}
