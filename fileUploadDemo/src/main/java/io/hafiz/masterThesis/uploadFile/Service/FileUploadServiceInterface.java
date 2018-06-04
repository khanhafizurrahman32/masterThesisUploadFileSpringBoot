package io.hafiz.masterThesis.uploadFile.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.json.JsonArray;

import org.springframework.web.multipart.MultipartFile;

public interface FileUploadServiceInterface {
	
	byte[] getDocumentFile(Long id);
	ResponseMetaData save (MultipartFile multipartFile) throws IOException;
	ArrayList<FileDescription> findAll();
	List<String> contentsInJson(String inputFilePath);
	void startKafkaTerminalCommandsFromJava(String topicName);
	void sendDataToKafkaTopic(Map<String, String> parameters);
	void submitPysparkProjectTerminalCommand(Map<String, String> parameters);
	String getHeadersName(String inputFilePath);
}
