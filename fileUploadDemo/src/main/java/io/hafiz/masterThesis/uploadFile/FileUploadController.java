package io.hafiz.masterThesis.uploadFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


import io.hafiz.masterThesis.fileManipulation.sampleFileToHandle;
import io.hafiz.masterThesis.streamingFile.Service.FileStreamingServiceInterface;
import io.hafiz.masterThesis.uploadFile.Service.FileDescription;
import io.hafiz.masterThesis.uploadFile.Service.FileUploadServiceInterface;
import io.hafiz.masterThesis.uploadFile.Service.ResponseMetaData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;


@RestController
@RequestMapping(value = "/upload")
public class FileUploadController {
	private static final Logger LOG = Logger.getLogger(FileUploadController.class);
	
	private FileUploadServiceInterface fileUploadService;
	private FileStreamingServiceInterface fileStreamService;
	
	@Autowired
	public FileUploadController(FileUploadServiceInterface fileUploadService, FileStreamingServiceInterface fileStreamService) {
		this.fileUploadService = fileUploadService;
		this.fileStreamService = fileStreamService;
	}

	@CrossOrigin(origins = "http://localhost:3000")
	@RequestMapping("/readAllFiles")
	public ArrayList<FileDescription> getAllFiles() {
		return fileUploadService.findAll();
	}
	@CrossOrigin
	@RequestMapping(value = "/toaFixedPlace", method = RequestMethod.POST )
	public @ResponseBody ResponseMetaData handleFileUpload(@RequestParam(value="file") MultipartFile multipartFile) throws IOException{
		return fileUploadService.save(multipartFile);
	}
	
	@RequestMapping("/startProcessingFile")
	public void processFile(@RequestParam(value="inputFilePath", defaultValue="Please specify a fixed location")String inputFilePath) {
		System.out.println(inputFilePath);
		try {
			File inputF = new File(inputFilePath);
			InputStream inputFS = new FileInputStream(inputF);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
			
			System.out.println(br.readLine());
			br.close();
		}catch(Exception ex) {
			System.out.println(ex.toString());
		}
	}
	
	@RequestMapping("/processSampleFile")
	public void processSampleFile(@RequestParam(value="sampleFilePath", defaultValue="location is not specified") String sampleFilePath) {
		System.out.println(sampleFilePath);
		fileStreamService.startProcessingFileToSubmitIntoTopic(sampleFilePath);
	}
	
	
	@RequestMapping("/getHeadersOfaFile")
	public List<String> getHeadersList(@RequestParam(value="inputFilePath") String inputFilePath) {
		List<String> headerNames = new ArrayList<String>();
		try {
			File inputF = new File(inputFilePath);
			InputStream inputFS = new FileInputStream(inputF);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
			
			headerNames = Stream.of(br.readLine()).map(line -> line.split(","))
								.flatMap(Arrays:: stream).collect(Collectors.toList());
			
			//headerNames.forEach(System.out::println);
	
		} catch (Exception e) {
			// TODO: handle exception
		}
		return headerNames;
	}
	
	
	

	
	
}
