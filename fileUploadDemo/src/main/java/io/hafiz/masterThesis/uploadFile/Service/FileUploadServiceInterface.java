package io.hafiz.masterThesis.uploadFile.Service;

import java.io.IOException;
import java.util.List;

import org.springframework.web.multipart.MultipartFile;

public interface FileUploadServiceInterface {
	
	byte[] getDocumentFile(Long id);
	ResponseMetaData save (MultipartFile multipartFile) throws IOException;
	List<FileDescription> findAll();
	
	
}
