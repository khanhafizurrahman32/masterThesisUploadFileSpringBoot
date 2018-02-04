package io.hafiz.masterThesis.Files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class fileManipulation {
	@SuppressWarnings("unchecked")
	public List<String> readFileFromCsvFile(String inputFilePath) {
		
		List<String> inputList = new ArrayList<String>();
		
		try {
			
			File inputF = new File(inputFilePath);
			InputStream inputFS = new FileInputStream(inputF);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
			
			inputList = (List<String>) br.lines();
			br.close();
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		return inputList;
	}
}
