package io.hafiz.masterThesis.uploadFile;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import io.hafiz.masterThesis.uploadFile.Service.FileUploadServiceInterface;
import io.hafiz.masterThesis.uploadFile.Service.ResponseMetaData;


@RestController
@RequestMapping(value = "/upload")
public class FileUploadController {
	private static final Logger LOG = Logger.getLogger(FileUploadController.class);
	
	private FileUploadServiceInterface fileUploadService;
	
	@Autowired
	public FileUploadController(FileUploadServiceInterface fileUploadService) {
		this.fileUploadService = fileUploadService;
	}

	
	@RequestMapping(value = "/toaFixedPlace", method = RequestMethod.POST )
	public @ResponseBody ResponseMetaData handleFileUpload(@RequestParam(value="file") MultipartFile multipartFile) throws IOException{
		System.out.println("hhh");
		return fileUploadService.save(multipartFile);
	}
	
	
}
