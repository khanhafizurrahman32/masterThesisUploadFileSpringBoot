package io.hafiz.masterThesis.fileManipulation;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SampleFileToHandleSerializer implements Serializer<sampleFileToHandle>  {

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String arg0, sampleFileToHandle arg1) {
		// TODO Auto-generated method stub
		byte[] retval = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retval = objectMapper.writeValueAsString(arg1).getBytes();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return retval;
	}
	
}
