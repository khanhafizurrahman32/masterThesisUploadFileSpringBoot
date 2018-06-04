package io.hafiz.masterThesis.uploadFileController;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import io.hafiz.masterThesis.fileManipulation.FileWatcher;
import io.hafiz.masterThesis.streamingFile.Service.FileStreamingServiceInterface;
import io.hafiz.masterThesis.uploadFile.Service.FileDescription;
import io.hafiz.masterThesis.uploadFile.Service.FileUploadServiceInterface;
import io.hafiz.masterThesis.uploadFile.Service.ResponseMetaData;


@RestController
@RequestMapping(value = "/upload")
public class FileUploadController {
	//private static final Logger LOG = Logger.getLogger(FileUploadController.class);
	
	private FileUploadServiceInterface fileUploadService;
	private FileStreamingServiceInterface fileStreamService;
	
	@Autowired
	public FileUploadController(FileUploadServiceInterface fileUploadService, FileStreamingServiceInterface fileStreamService) {
		this.fileUploadService = fileUploadService;
		this.fileStreamService = fileStreamService;
	}

	@CrossOrigin
	@RequestMapping("/readAllFiles")
	public ArrayList<FileDescription> getAllFiles() {
		return fileUploadService.findAll();
	}
	@CrossOrigin
	@RequestMapping(value = "/toaFixedPlace", method = RequestMethod.POST )
	public @ResponseBody ResponseMetaData handleFileUpload(@RequestParam(value="file") MultipartFile multipartFile) throws IOException{
		return fileUploadService.save(multipartFile);
	}
	
	@CrossOrigin
	@RequestMapping("/startProcessingFile")
	public List<String> processFile(@RequestParam(value="inputFilePath") String inputFilePath) {
		return fileUploadService.contentsInJson(inputFilePath);
	}
	
	@RequestMapping("/processSampleFile")
	public void processSampleFile(@RequestParam(value="sampleFilePath", defaultValue="location is not specified") String sampleFilePath) {
		System.out.println(sampleFilePath);
		fileStreamService.startProcessingFileToSubmitIntoTopic(sampleFilePath);
	}
	
	@CrossOrigin
	@RequestMapping("/startKafkaCommandShell")
	public void commandKafkaShellStart(@RequestParam(value="topicName", defaultValue="Please specify a topicName") String topicName) {
		System.out.println(topicName);
		fileUploadService.startKafkaTerminalCommandsFromJava(topicName);
	}
	
	@CrossOrigin
	@RequestMapping("/startPythonCommandShell")
	public void commandSparkCreate(@RequestParam Map<String, String> parameters) {
		fileUploadService.submitPysparkProjectTerminalCommand(parameters);
	}
	
	
	@CrossOrigin
	@RequestMapping("/sendDatatoKafka")
	public void sendData(@RequestParam Map<String, String> parameters) {
		
		System.out.println(parameters.get("kafka_broker_end_point"));
		System.out.println(parameters.get("csv_input_file"));
		System.out.println(parameters.get("topic_name"));
		fileUploadService.sendDataToKafkaTopic(parameters);
	}
	
	@CrossOrigin
	@RequestMapping("/getHeadersOfaFile")
	public String getHeadersList(@RequestParam(value="inputFilePath") String inputFilePath) {
		return fileUploadService.getHeadersName(inputFilePath);
	}
	
	@RequestMapping("/fileWatch")
	public void checkFileMoldifiedOrnot(@RequestParam(value="checkFilePath") String filePath) {
		TimerTask task = new FileWatcher(new File(filePath)) {
			
			@Override
			protected void onChange(File file) {
				System.out.println("File " + file.getName() + "have changes !");
			}
		};
		
		Timer timer = new Timer ();
		timer.schedule(task, new Date(), 1000);
	}
}
