package io.hafiz.masterThesis.uploadFile.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class FileUploadServiceImpl implements FileUploadServiceInterface {

	private static String UPLOADED_FOLDER = "/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/UploadFiles/";
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
	
}
