package io.hafiz.masterThesis.fileManipulation;

public class sampleFileToHandle {
	private int id;
	private String firstName;
	private String lastName;
	private String sex;
	
	public sampleFileToHandle() {
		super();
	}

	public sampleFileToHandle(int id, String firstName, String lastName, String sex) {
		super();
		this.id = id;
		this.firstName = firstName;
		this.lastName = lastName;
		this.sex = sex;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}

	@Override
	public String toString() {
		return "sampleFileToHandle [id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", sex=" + sex
				+ "]";
	}	
}
