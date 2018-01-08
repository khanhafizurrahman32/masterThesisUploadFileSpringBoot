package io.hafiz.masterThesis.uploadFile.Service;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;

@Entity
public class FileDescription {
	
	public FileDescription() {
		
	}
// need not to be here	
	public FileDescription(Long id, String fileName) {
		super();
		this.id = id;
		this.fileName = fileName;
	}
	
	public FileDescription(Long id, String fileName, byte[] file) {
		super();
		this.id = id;
		this.fileName = fileName;
		this.file = file;
	}
	
	@Id
	@GeneratedValue(strategy= GenerationType.IDENTITY)
	private Long id;
	
	@Column
	private String fileName;
	@Column
	@Lob
	private byte[] file;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public byte[] getFile() {
		return file;
	}
	public void setFile(byte[] file) {
		this.file = file;
	}
	
}
