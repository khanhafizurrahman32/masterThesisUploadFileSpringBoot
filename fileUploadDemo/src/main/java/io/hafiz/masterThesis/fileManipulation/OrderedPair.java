package io.hafiz.masterThesis.fileManipulation;

public class OrderedPair {
	private String header;
	private String valueString;
	
	public OrderedPair(String header, String valueString) {
		super();
		this.header = header;
		this.valueString = valueString;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getValueString() {
		return valueString;
	}

	public void setValueString(String valueString) {
		this.valueString = valueString;
	}	
}
